use std::{collections::HashMap, time::Duration};

use raft::prelude::Message;
use server::{Command, KvServer, ServerConfig};

pub mod errors;
pub mod server;
pub mod store;
pub mod utils;

fn main() {
    let sc1 = create_server_config(1, vec![1, 2, 3]);
    let sc2 = create_server_config(2, vec![1, 2, 3]);
    let sc3 = create_server_config(3, vec![1, 2, 3]);

    let (mut kvserver1, router1) = KvServer::new(sc1);
    let (mut kvserver2, router2) = KvServer::new(sc2);
    let (mut kvserver3, router3) = KvServer::new(sc3);

    let mut router_table = HashMap::new();
    router_table.insert(1, router1.inbound_raft_message_tx.clone());
    router_table.insert(2, router2.inbound_raft_message_tx.clone());
    router_table.insert(3, router3.inbound_raft_message_tx.clone());

    kvserver1.route_table = Some(router_table.clone());
    kvserver2.route_table = Some(router_table.clone());
    kvserver3.route_table = Some(router_table.clone());

    let jh1 = std::thread::spawn(move || {
        kvserver1.run();
        assert!(kvserver1.kvstore.is_empty());
    });
    let jh2 = std::thread::spawn(move || {
        kvserver2.run();
        assert!(kvserver2.kvstore.is_empty());
    });
    let jh3 = std::thread::spawn(move || {
        kvserver3.run();
        assert!(kvserver3.kvstore.is_empty());
    });

    // propose to leader, but we dont known the leader
    std::thread::sleep(Duration::from_millis(10000));
     router1
        .propose_raft_command(Command::Put(b"key1".to_vec(), b"val1".to_vec()))
        .unwrap();
     router1
        .propose_raft_command(Command::Put(b"key2".to_vec(), b"val2".to_vec()))
        .unwrap();
     router1
        .propose_raft_command(Command::Put(b"key3".to_vec(), b"val3".to_vec()))
        .unwrap();

    router1
        .propose_raft_command(Command::Del(b"key3".to_vec()))
        .unwrap();
    router1
        .propose_raft_command(Command::Del(b"key1".to_vec()))
        .unwrap();
    router1
        .propose_raft_command(Command::Del(b"key2".to_vec()))
        .unwrap();

    // TODO: 跑起来会多出一轮Append，不是成为leader的时刻, 需要研究清楚什么为什么产生的.
    // 成为leader的场景是发送一条空Entry, data为空:
    // node_id: 3, msg.from: 3, msg.to:2, msg.type:MsgAppend, entries:[term: 2 index: 4]
    // 但是异常的Append是entry完全没有:
    // node_id: 3, msg.from: 3, msg.to:1, msg.type:MsgAppend, entries:[]
    // node_id: 3, msg.from: 3, msg.to:2, msg.type:MsgAppend, entries:[]
    // NOTE: 已确认 raft_config.skip_bcast_commit参数影响的，设置成true则消失

    //
    // test a transfer leader
    std::thread::sleep(Duration::from_secs(100));
    // for x in 0..2 {
    //     let to = x % 3 + 1;
    //     std::thread::sleep(Duration::from_secs(8));
    //     println!("start to try transfer leader to {}...................", to);
    //     let mut msg = Message::new();
    //     msg.set_msg_type(raft::prelude::MessageType::MsgTransferLeader);
    //     match to {
    //         1 => &router1,
    //         2 => &router2,
    //         3 => &router3,
    //         _ => panic!(),
    //     }
    //     .accept_raft_msg(msg)
    //     .unwrap();
    // }

    std::mem::drop(router1);
    std::mem::drop(router2);
    std::mem::drop(router3);

    jh1.join().unwrap();
    jh2.join().unwrap();
    jh3.join().unwrap();
}

fn create_server_config(self_id: u64, peers: Vec<u64>) -> ServerConfig {
    ServerConfig {
        run_id: self_id,
        peers: peers,
    }
}
