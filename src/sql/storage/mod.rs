
mod kv;
pub mod engine;
pub use engine::{Catalog, Transaction, IndexScan, Row, Tables};
pub mod schema;

pub mod types;
pub use types::{Value, Datatype, Expression};
pub use kv::{Mode, Kv, Mvcc};
pub use schema::{Column, Table};
mod raftlog;
pub use raftlog::{Store, Range};
mod raft;
use crate::raft::Client;
use crate::error::{Error, Result};

pub trait State: Send {
    fn applied_index(&self) -> u64;

    fn mutate(&mut self, index: u64, command: Vec<u8>) -> Result<Vec<u8>>;

    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>>;
}