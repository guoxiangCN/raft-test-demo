use std::{
    collections::HashMap,
    fmt::format,
    sync::mpsc::{Receiver, Sender},
    time::{Duration, Instant},
};

use protobuf::Message as PbMessage;
use raft::{
    prelude::{ConfChange, EntryType, HardState, Message, MessageType},
    storage::MemStorage,
    Config, Peer, Raft, RaftState, RawNode, StateRole,
};

use crate::errors::Error;

trait ToString1 {
    fn to_string1(&self) -> String;
}

impl ToString1 for HardState {
    fn to_string1(&self) -> String {
        format!(
            "HardState, term={}, vote={}, commit={}",
            self.term, self.vote, self.commit
        )
    }
}

impl ToString1 for ConfChange {
    fn to_string1(&self) -> String {
        format!(
            "ConfChange, type={:?}, node_id:{}",
            self.change_type, self.node_id
        )
    }
}

pub struct ServerConfig {
    pub run_id: u64,
    pub peers: Vec<u64>,
}

impl ServerConfig {
    pub fn to_peers(&self) -> Vec<Peer> {
        self.peers
            .iter()
            .map(|id| Peer {
                id: *id,
                context: None,
            })
            .collect()
    }
}

pub enum Command {
    Put(Vec<u8>, Vec<u8>),
    Del(Vec<u8>),
}

pub struct CommandWithCb {
    pub cmd: Command,
    pub propose_callback: Box<dyn FnOnce()>, // trigger after the msg propose
    pub commit_callback: Box<dyn FnOnce()>,  // trigger after the msg accept by qurom nodes
    pub apply_callback: Box<dyn FnOnce()>,   // trigger after the msg apply by the fsm
}

impl Command {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = vec![];
        match self {
            Command::Put(k, v) => {
                bytes.push(1);
                bytes.extend_from_slice(&k.len().to_be_bytes());
                bytes.extend_from_slice(k);
                bytes.extend_from_slice(v);
            }
            Command::Del(k) => {
                bytes.push(0);
                bytes.extend_from_slice(k);
            }
        }
        bytes
    }

    pub fn from_bytes(data: &[u8]) -> crate::errors::Result<Command> {
        match data[0] {
            0 => Ok(Command::Del(data[1..].to_vec())),
            1 => {
                // use bytes::
                let d = &data[1..];
                let keylen = u64::from_be_bytes(d[..8].try_into().unwrap()) as usize;
                let key = d[..keylen].to_vec();
                let val = d[keylen..].to_vec();
                Ok(Command::Put(key, val))
            }
            _ => panic!(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RaftRouter {
    pub proposer_tx: Sender<Command>,
    pub inbound_raft_message_tx: Sender<Message>,
}

impl RaftRouter {
    pub fn propose_raft_command(&self, cmd: Command) -> crate::errors::Result<()> {
        self.proposer_tx
            .send(cmd)
            .map_err(|_e| Error::SendRaftCmdError)
    }

    pub fn accept_raft_msg(&self, msg: Message) -> crate::errors::Result<()> {
        self.inbound_raft_message_tx
            .send(msg)
            .map_err(|_e| Error::Other("Inbound raft message error".to_string()))
    }
}

pub struct KvServer {
    server_config: ServerConfig,
    raft_node: RawNode<MemStorage>,
    proposer_rx: Receiver<Command>,             // !Sync
    inbound_raft_message_rx: Receiver<Message>, // !Sync

    pub route_table: Option<HashMap<u64, Sender<Message>>>,

    // kv store
    pub kvstore: crossbeam_skiplist::SkipMap<Vec<u8>, Vec<u8>>,
}

impl KvServer {
    pub fn new(server_config: ServerConfig) -> (Self, RaftRouter) {
        let mut raft_config = Config::default();
        raft_config.id = server_config.run_id;
        raft_config.heartbeat_tick = 10; // aka 1s
        raft_config.election_tick = 30; // aka 3s

        let raft_store = MemStorage::new();
        let peers = server_config.to_peers();

        // peers仅当当前是一个全新的raft时才使用到, 否则会被忽略，真正的peer从storage的softstate里获取.
        let raft_node = RawNode::new(&raft_config, raft_store, peers).unwrap();

        let (tx, rx) = std::sync::mpsc::channel();
        let (raft_msg_tx, raft_msg_rx) = std::sync::mpsc::channel();
        (
            Self {
                server_config: server_config,
                raft_node: raft_node,
                proposer_rx: rx,
                inbound_raft_message_rx: raft_msg_rx,
                route_table: None,
                kvstore: crossbeam_skiplist::SkipMap::new(),
            },
            RaftRouter {
                proposer_tx: tx,
                inbound_raft_message_tx: raft_msg_tx,
            },
        )
    }

    pub fn run(&mut self) {
        let mut t = Instant::now();
        let mut timeout = Duration::from_millis(100);
        loop {
            // 将本节点的propose提交给raft状态机.
            match self.proposer_rx.recv_timeout(timeout.clone()) {
                Ok(cmd) => {
                    self.raft_node.propose(vec![], cmd.to_bytes()).unwrap();
                }
                Err(e) => match e {
                    std::sync::mpsc::RecvTimeoutError::Timeout => {}
                    std::sync::mpsc::RecvTimeoutError::Disconnected => break,
                },
            }

            let d = t.elapsed();
            if d >= timeout {
                t = Instant::now();
                timeout = Duration::from_millis(100);
                // We drive Raft every 100ms.
                self.raft_node.tick();
            } else {
                timeout -= d;
            }

            // 从其他节点发来的raft msg.
            loop {
                match self.inbound_raft_message_rx.try_recv() {
                    Ok(msg) => {
                        self.raft_node.step(msg).unwrap();
                    }
                    Err(e) => match e {
                        std::sync::mpsc::TryRecvError::Empty => break,
                        std::sync::mpsc::TryRecvError::Disconnected => {
                            println!("proposer disconnected...");
                            return;
                        }
                    },
                }
            }

            let d = t.elapsed();
            if d >= timeout {
                t = Instant::now();
                timeout = Duration::from_millis(100);
                // We drive Raft every 100ms.
                self.raft_node.tick();
            } else {
                timeout -= d;
            }

            if self.raft_node.has_ready() {
                let ready = self.raft_node.ready();

                if let Some(ref hs) = ready.hs {
                    // 持久化hard state
                    let mut store = self.raft_node.get_store().wl();
                    store.set_hardstate(hs.clone());
                    // TODO: sync.
                }

                if let Some(ref ss) = ready.ss {
                    if ss.raft_state == StateRole::Leader {
                        assert_eq!(ss.leader_id, self.server_config.run_id);
                        println!("node_id: {} became leader", self.server_config.run_id);
                    }
                }

                // 需要被保存到本地的entry, 目前只保存在raft的unstable中
                if !ready.entries.is_empty() {
                    // 持久化到本地raftlog storage.
                    let mut store = self.raft_node.get_store().wl();
                    store.append(&ready.entries).unwrap();
                }

                // 需要被持久化的快照.
                if !ready.snapshot.data.is_empty() {
                    let mut store = self.raft_node.get_store().wl();
                    store.apply_snapshot(ready.snapshot.clone()).unwrap();
                }

                // 存在需要发给其他节点的message
                if !ready.messages.is_empty() {
                    if let Some(ref route_table) = self.route_table {
                        for msg in &ready.messages {
                            println!(
                                "node_id: {}, msg.from: {}, msg.to:{}, msg.type:{:?}, entries:{:?}",
                                self.server_config.run_id, msg.from, msg.to, msg.msg_type, msg.entries
                            );
                            let sender = route_table.get(&msg.to).expect("unable find route");
                            sender.send(msg.clone()).unwrap();
                        }
                    }
                }

                if let Some(ref entries) = ready.committed_entries {
                    // TODO: Apply be async process. But should be sequcential.
                    for ent in entries {
                        // println!("Apply: entry : {:?}", ent);
                        if ent.entry_type == EntryType::EntryNormal {
                            if ent.get_data().is_empty() {
                                println!("Empty entry after election ok, skip apply");
                                continue;
                            }
                            let cmd = Command::from_bytes(ent.get_data())
                                .expect("failed to convert entry.data to Command");
                            match cmd {
                                Command::Put(k, v) => {
                                    self.kvstore.insert(k, v);
                                    println!("node_id: {} apply a PUT", self.server_config.run_id);
                                }
                                Command::Del(k) => {
                                    self.kvstore.remove(&k);
                                    println!("node_id: {} apply a DEL", self.server_config.run_id);
                                }
                            }
                        } else if ent.entry_type == EntryType::EntryConfChange {
                            let ref data = ent.data;
                            let cc: ConfChange = ConfChange::parse_from_bytes(data).unwrap();
                            println!(
                                "node_id: {}.,  {}",
                                self.server_config.run_id,
                                cc.to_string1()
                            );
                            // 如果是AddNode: 持久化,需要使得ConfState下次可以查到自己.
                        }
                    }
                }

                // 推进raft Ready.
                self.raft_node.advance(ready);

                // 50ms后再tick
                // std::thread::sleep(Duration::from_millis(10));

                println!("_______________________________________________________________");
            }
        }
    }
}

mod tests {
    use std::{collections::HashMap, time::Duration, vec};

    use crate::utils;

    use super::{KvServer, ServerConfig};

    #[test]
    fn test_send_sync() {
        utils::assert_send::<KvServer>();
        // utils::assert_sync::<KvServer>();  # !Sync
    }

    #[test]
    fn test() {
        let server_config = ServerConfig {
            run_id: 1,
            peers: vec![1],
        };

        let (mut kvserver, router) = KvServer::new(server_config);

        let jh = std::thread::spawn(move || {
            kvserver.run();
        });

        std::thread::sleep(Duration::from_millis(500));
        router
            .propose_raft_command(super::Command::Del("key1".as_bytes().to_vec()))
            .unwrap();

        std::thread::sleep(Duration::from_millis(500));
        router
            .propose_raft_command(super::Command::Put(b"key1".to_vec(), b"val1".to_vec()))
            .unwrap();

        jh.join().unwrap();
    }
}
