use raft::Error as RaftError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("RaftError: {0}")]
    RaftError(#[from] RaftError),

    #[error("SendRaftCmdError")]
    SendRaftCmdError,

    #[error("Other: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;
