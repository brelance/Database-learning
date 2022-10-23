use std::collections::{HashMap, BTreeMap, HashSet};

use super::{log::{Entry, Scan}, message::{Address, Message, Event, Response}, Status,};
use log::debug;
use regex::internal::Inst;
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
        debug!("Executing {:?}", i);
        match i {

            Instruction::Apply { entry: Entry { index, command, ..} } => {
                if let Some(command) = command {
                    match tokio::task::block_in_place(|| state.mutate(index, command)) {
                        Err(err @ Error::Internal(_)) => return Err(error),
                        result => self.notify_applied(index, result),
                    }
                }
                self.applied_index = index;
                self.query_execute(&mut state)?;
            },
            

            Instruction::Notify { id, address, index } => {
                if index > state.applied_index() {
                    self.notify.insert(index, (address, id));
                } else {
                    //why
                    self.send(address, Event::ClientResponse { id, response: Err(Error::Abort) })?;
                }
            },

            Instruction::Query { id, address, command, term, index, quorum } => {
                self.queries.entry(index).or_default().insert(
                    id.clone(),
                    Query { id, term, address, command, quorum, votes, }
                );
            }

            Instruction::Vote { term, index, address } => {
                self.query_vote(term, index, address);
                self.query_execute(&mut state)?;
            },
        }
    }

    fn notify_applied(&mut self, index: u64, result: Result<Vec<u8>>) -> Result<()> {
        if let Some(to, id) = self.notify.remove(&index) {
            self.send(to, Event::ClientResponse { id, response: result.map(Response::State) })?;
        }
        Ok(())
    }

    fn send(&self, to: Address, event: Event) -> Result<()> {
        let message = Message {
            from: Address::Local,
            to,
            term: 0,
            event,
        };
        debug!("Sending {:?}", msg);
        Ok(self.node_tx.send(msg)?)
    }

    fn query_vote(&self, term: u64, commit_index: u64, address: Address) {
        for (_, queries) in self.queries.range_mut(..=commit_index) {
            for (_, query) in queries.iter_mut() {
                if term > query.term {
                    query.votes.insert(address.clone());
                }
            }
        }
    }

    fn query_execute(&mut self, state: &mut dyn State) -> Result<()> {
        for query in self.query_ready(self.applied_index) {
            debug!("Executing query {:?}", query.command);
            let result = state.query(query.command);
            match result {
                Err(error @ Error::Internal(_)) => return Err(error),
                _ => self.send(
                    query.address,
                    Event::ClientResponse { id: query.id, response: result.map(Response::State) },
                )?,
            }
        }
        Ok(())    
    }

    fn query_ready(&mut self, applied_index: u64) -> Vec<Query> {
        let mut ready = Vec::new();
        let mut empty = Vec::new();
        for (index, queries) in self.queries.range_mut(..=applied_index) {
            let mut ready_ids = Vec::new();
            for (id, query) in queries.iter_mut() {
                if query.votes.len() >= query.quorum {
                    ready_ids.push(id.clone());
                }
            }
            for id in ready_ids {
                if let Some(query) = queries.remove(&id) {
                    ready.push(query)
                }
            }
            if queries.is_empty() {
                empty.push(*index)
            }
        }
        for index in empty {
            self.queries.remove(&index);
        }
        ready
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