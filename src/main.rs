use std::{collections::HashMap, time::Duration};

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
    });
    let jh2 = std::thread::spawn(move || {
        kvserver2.run();
    });
    let jh3 = std::thread::spawn(move || {
        kvserver3.run();
    });

    // propose to leader, but we dont known the leader
    std::thread::sleep(Duration::from_millis(10000));
    router1
        .propose_raft_command(Command::Del(b"key1".to_vec()))
        .unwrap();
    router2
        .propose_raft_command(Command::Del(b"key2".to_vec()))
        .unwrap();
    router3
        .propose_raft_command(Command::Del(b"key3".to_vec()))
        .unwrap();

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
