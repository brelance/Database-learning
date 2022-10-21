use std::collections::HashMap;
use log::info;

use crate::{
    error::{Error, Result},
    raft::{status::Instruction, message::{Message, Event, Address}}
};

use super::{HEARTBEAT_INTERVAL, RoleNode, follower::Follower};

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
        let entries = self.log.scan(peer_next..).collect::<Result<Vec<_>>>()?;
        self.send(
            Address::Peer(peer.to_string()),
            Event::ReplicateEntries { base_index, bas_term, entries, },
        )?;
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        let mut last_indexes = vec![self.log.last_index];
        last_indexes.extend(self.role.peer_last_index.values());
        last_indexes.sort_unstable();
        last_indexes.reverse();
        let quorum_index = last_indexes[self.quorum()];
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


    }

    fn quorum(&self) -> usize {
        (self.peers.len() + 1) / 2 + 1
    }
}

