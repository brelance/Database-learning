use std::collections::{HashMap, BTreeMap, HashSet};

use super::{log::{Entry, Scan}, message::{Address, Message}, Status,};
use log::debug;
use tokio::sync::mpsc;
use crate::{State, error::{Error, Result}};



pub enum Instruction {
    Abort,
    Apply { entry: Entry },
    Notify { id: Vec<u8>, address: Address, index: u64 },
    Query { id: Vec<u8>, address: Address, command: Vec<u8>, term: u64, index: u64, quorum: u64},
    Status { id: Vec<u8>, address: Address, status: Box<Status> },
    Vote { term: u64, index: u64, address: Address},
}

pub struct Driver {
    state_rx: mpsc::UnboundedReceiver<Instruction>,
    node_tx: mpsc::UnboundedSender<Message>,
    applied_index: u64,
    notify: HashMap<u64, (Address, Vec<u8>)>,
    queries: BTreeMap<u64, BTreeMap<Vec<u8>, Query>>,
}

impl Driver {
    pub fn new(
        state_rx: mpsc::UnboundedReceiver<Instruction>,
        node_tx: mpsc::UnboundedSender<Message>,
    ) -> Self {
        Self {
            state_rx,
            node_tx,
            applied_index: 0,
            notify: HashMap::new(),
            queries: BTreeMap::new(),
        }
    }

    pub fn replay<'a>(&mut self, state: &mut dyn State, mut scan: Scan<'a>) -> Result<()> {
        while let Some(entry) = scan.next().transpose()? {
            debug!("Replaying {:?}", entry);
            if let Some(command) = entry.command {
                match state.mutate(entry.index, command) {
                    Err(error @ Error::Internal(_)) => return Err(error),
                    _ => self.applied_index = entry.index,
                }
            }
        }
        Ok(())
    }

    pub async fn driver(&self, state: Box<dyn State>) -> Result<()> {
        Ok(())
    }
}

struct Query {
    id: Vec<u8>,
    term: u64,
    address: Address,
    command: Vec<u8>,
    quorum: u64,
    votes: HashSet<Address>,
}