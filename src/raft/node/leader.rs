use std::collections::HashMap;
use log::{info, warn};

use crate::{
    error::{Error, Result},
    raft::{status::Instruction, message::{Message, Event, Address, Request, Response}}
};

use super::{HEARTBEAT_INTERVAL, RoleNode, follower::Follower, Status, Node};

pub struct Leader {
    heart_ticks: u64,
    peer_next_index: HashMap<String, u64>,
    peer_last_index: HashMap<String, u64>,
}

impl Leader {
    pub fn new(peers: Vec<String>, last_index: u64) -> Self {
        let mut leader = Leader {
            heart_ticks: 0,
            peer_last_index: HashMap::new(),
            peer_next_index: HashMap::new(),
        };
        for peer in peers {
            leader.peer_last_index.insert(peer.clone(),  0);
            leader.peer_next_index.insert(peer.clone(), last_index + 1);
        }
        leader
    }
}

impl RoleNode<Leader> {
    fn become_follower(mut self, term: u64, leader: &str) -> Result<RoleNode<Follower>> {
        info!("Discovered new leader {} for term {}, following", leader, term);
        self.term = term;
        self.log.save_term(term, None)?;
        self.state_tx.send(Instruction::Abort)?;
        self.become_role(Follower::new(Some(leader), None))
    }

    pub fn append(&mut self, command: Option<Vec<u8>>) -> Result<u64> {
        let entry = self.log.append(self.term, command)?;
        for peer in &self.peers {
            self.replicate(peer)?;
        }
        Ok(entry.index)
    }

    fn replicate(&self, peer: &str) -> Result<()> {
        let index = self
            .role
            .peer_next_index
            .get(peer)
            .cloned()
            .ok_or_else(||Error::Internal(format!("Unknown peer {}", peer)))?;
        let base_index = if index > 0 { index - 1} else { 0 };
        let base_term = match self.log.get(base_index)? {
            Some(base) => base.term,
            None if base_index == 0 => 0,
            None => return Err(Error::Internal(format!("Missing base entry {}", base_index))),
        };
        let entries = self.log.scan(index..).collect::<Result<Vec<_>>>()?;
        self.send(
            Address::Peer(peer.to_string()),
            Event::ReplicateEntries { base_index, base_term, entries, },
        )?;
        Ok(())
    }

    fn commit(&mut self) -> Result<u64> {
        let mut last_indexes = vec![self.log.last_index];
        last_indexes.extend(self.role.peer_last_index.values());
        last_indexes.sort_unstable();
        last_indexes.reverse();
        let quorum_index = last_indexes[self.quorum() as usize];
        if quorum_index > self.log.commited_index {
            if let Some(entry) = self.log.get(quorum_index)? {
                if entry.term == self.term {
                    let old_commit_index = self.log.commited_index;
                    self.log.commit(quorum_index)?;
                    let mut scan = self.log.scan(old_commit_index + 1..=quorum_index);
                    while let Some(entry) = scan.next().transpose()? {
                        self.state_tx.send(Instruction::Apply { entry })?;
                    }
                }
            }
        }
        Ok(self.log.commited_index)
    }

    pub fn step(mut self, msg: Message) -> Result<Node> {
        if let Err(err) = self.validate(&msg) {
            warn!("Ignoring invalid message: {}", err);
            return Ok(self.into());
        }
        if msg.term > self.term {
            if let Address::Peer(from) = &msg.from {
                return self.become_follower(msg.term, from)?.step(msg);
            }
        }

        match msg.event {

            Event::ConfirmLeader { commit_index, has_commited } => {
                if let Address::Peer(from) = msg.from.clone() {
                    self.state_tx.send(Instruction::Vote {
                         term: msg.term, index: commit_index, address: msg.from,
                        })?;
                    if !has_commited {
                            self.replicate(&from)?;
                    }
                }
            },

            Event::AcceptEntries { last_index } => {
                if let Address::Peer(from) = msg.from {
                    self.role.peer_last_index.insert(from.clone(), last_index);
                    self.role.peer_next_index.insert(from.clone(), last_index + 1);
                }
                self.commit()?;
            },

            Event::RejectEntries => {
                if let Address::Peer(from) = msg.from {
                    self.role.peer_next_index.entry(from.clone()).and_modify(
                        |i| {
                            if *i > 1 {
                                *i -= 1
                            }
                        }
                    );
                    self.replicate(&from)?;
                }
            },

            Event::ClientRequest { id, request: Request:: Query(command) } => {
                self.state_tx.send(Instruction::Query {
                    id,
                    address: msg.from,
                    command,
                    term: self.term,
                    index: self.log.commited_index,
                    quorum: self.quorum()
                })?;
                self.state_tx.send(
                    Instruction::Vote {
                        term: self.term, 
                        index: self.log.commited_index, 
                        address: Address::Local 
                })?;
                if !self.peers.is_empty() {
                    self.send(
                        Address::Peers, 
                        Event::Heartbeat {
                            commit_index: self.log.commited_index, 
                            commit_term: self.log.commited_term
                        }
                    )?;
                }
            },

            Event::ClientRequest { id, request: Request::Mutate(command) } => {
                let index = self.append(Some(command))?;
                self.state_tx.send(Instruction::Notify { id, address: msg.from, index, })?;
                if self.peers.is_empty() {
                    self.commit()?;
                }
            },

            Event::ClientRequest { id, request: Request::Status } => {
                let mut status = Box::new(Status {
                    server: self.id.clone(),
                    leader: self.id.clone(),
                    term: self.term,
                    node_last_index: self.role.peer_last_index.clone(),
                    commit_index: self.log.commited_index,
                    apply_index: 0,
                    storage: self.log.store.to_string(),
                    storage_size: self.log.store.size(),
                });
                status.node_last_index.insert(self.id.clone(), self.log.last_index);
                self.state_tx.send(Instruction::Status { id, address: Address::Peers, status, })?
            }

            Event::ClientResponse { id, mut response } => {
                if let Ok(Response::Status(ref mut status)) = response {
                    status.server = self.id.clone();
                }
                self.send(Address::Client, Event::ClientResponse { id, response, })?;
            },

            Event::SolicitVote { .. } | Event::GrantVote => {},
            Event::Heartbeat { .. } | Event::ReplicateEntries { .. } => {
                warn!("Received unexpected message {:?}", msg)
            }
        }
        Ok(self.into())

    }

    pub fn tick(mut self) -> Result<Node> {
        if !self.peers.is_empty() {
            self.role.heart_ticks += 1;
            if self.role.heart_ticks >= HEARTBEAT_INTERVAL {
                self.role.heart_ticks = 0;
                self.send(
                    Address::Peers, 
                    Event::Heartbeat { 
                        commit_index: self.log.commited_index,
                        commit_term: self.log.commited_term,
                    })?;
            }
        }
        Ok(self.into())
    }
}

