use regex::bytes;
use serde_derive::{Deserialize, Serialize};
use serde::{Serialize, Deserialize};
use crate::sql::LogStore;
use crate::error::{Error, Result};

#[derive(Deserialize, Serialize)]
pub struct Entry {
    pub index: u64,
    pub term: u64,
    pub command: Option<Vec<u8>>,
}

pub enum Key {
    TernVote,
}

pub struct Log {
    store: LogStore,
    commit_index: u64,
    commit_term: u64,
    last_index: u64,
    last_term: u64,
}

impl Log {

}

fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)

}

fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(bytes)?)
}
