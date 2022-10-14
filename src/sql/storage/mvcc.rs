use super::Memory;
use std::{borrow::Cow, collections::HashSet};
use super::coding::*;

pub struct Mvcc {
    storage: Memory,
}

impl Mvcc {
    fn new() -> Self {
        Mvcc { storage: Memory::new() }
    }
}


struct Transaction {
    storage: Mvcc,
    txn_id: u64,
    mode: Mode,
    snapshot: Snapshot,
}

impl Transaction {

}

struct Snapshot {
    version: u64,
    invisible: HashSet<u64>,
}

enum Key<'a> {
    TxnNext,
    TxnActive(u64),
    TxnSnapshot(u64),
    TxnUpdate(u64, Cow<'a, [u8]>),
    Record(Cow<'a, [u8]>, u64),
    Metadata(Cow<'a, [u8]>),
}

enum Mode {
    ReadWrite,
    ReadOnly,
    Snapshot { version: u64 }
}

impl<'a> Key {
    fn encoding(&self) -> Vec<u8> {
        match self {
            Key::TxnNext => vec![0x01],
            Key::TxnActive(id) => [&[0x02][..], ]
        }
    }
}