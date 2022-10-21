use super::{log::Entry, message::Address, Status};



pub enum Instruction {
    Abort,
    Apply { entry: Entry },
    Notify { id: Vec<u8>, address: Address, index: u64 },
    Query { id: Vec<u8>, address: Address, command: Vec<u8>, term: u64, index: u64, quorum: u64},
    Status { id: Vec<u8>, address: Address, status: Box<Status> },
    Vote { term: u64, index: u64, address: Address},
}

