pub struct RaftStorage {}

impl RaftStorage {
    pub fn new() -> Self {
        RaftStorage {}
    }
}

impl raft::Storage for RaftStorage {
    fn initial_state(&self) -> raft::Result<raft::RaftState> {
        todo!()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: u64,
    ) -> raft::Result<Vec<raft::prelude::Entry>> {
        todo!()
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        todo!()
    }

    fn first_index(&self) -> raft::Result<u64> {
        todo!()
    }

    fn last_index(&self) -> raft::Result<u64> {
        todo!()
    }

    fn snapshot(&self) -> raft::Result<raft::prelude::Snapshot> {
        todo!()
    }
}
