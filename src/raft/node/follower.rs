use log::{warn, info, debug};
use rand::Rng;
use regex::bytes::ReplacerRef;

use crate::raft::{message::{Address, Message, Event}, node::candidate::Candidate};

use super::{leader, ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX, RoleNode};


pub struct Follower {
    leader: Option<String>,
    leader_seen_ticks: u64,
    leader_seen_timeout: u64,
    voted_for: Option<String>
}

impl Follower {
    pub fn new(leader: Option<&str>, voted_for: Option<&str>) -> Self {
        Self {
            leader: leader.map(String::from),
            leader_seen_ticks: 0,
            leader_seen_timeout: rand::thread_rng()
                .gen_range(ELECTION_TIMEOUT_MIN..=ELECTION_TIMEOUT_MAX),
            voted_for: voted_for.map(String::from),
        }
    }


}

impl RoleNode<Follower> {
    

    pub fn become_follower(mut self, leader: &str, term: u64) -> Result<RoleNode<Follower>> {
        let mut voted_for = None;
        if term > self.term {
            info!("Discoverd new term {}, following leader {}", term , leader);
            self.term = term;
            self.log.save_term(term, None)?;
        } else {
            info!("Discovered leader {}, following", leader);
            voted_for = self.role.voted_for;
        }
        //why
        self.role = Follower::new(Some(leader), voted_for.as_deref());
        // self.abort_proxied()?;
        // self.forward_queued()
        Ok(())
    }

    pub fn become_candidate(self) -> Result<RoleNode<Candidate>> {
        info!("starting election for term {}", self.term + 1);
        let mut node = self.become_role(Candidate::new())?;
        node.term += 1;
        node.log.save_term(node.term, None)?;
        node.send(
            Address::Peers, 
            Event::SolicitVote { last_index: node.log.last_index, last_term: node.log.last_term },
        )?;
        Ok(node)
    }

    fn is_leader(&self, from: &Address) -> bool {
        matches!((&self.role.leader, from), (Some(leader), Address::Peer(from)) if leader == from)
    }

    pub fn step(mut self, msg: Message) -> Result<Node> {
        if let Err(err) = self.validate(&msg) {
            warn!("Ignoring invalid message {}", err);
            return Ok(self.into())
        }
        if let Address::Peer(from) = &msg.from {
            if msg.term > self.term || self.role.leader.is_none() {
                return self.become_follower(from, msg.term)?.step(msg);
            }
        }
        if self.is_leader(&msg.from) {
            self.role.leader_seen_ticks = 0;
        }

        match msg.event {
            Event::Heartbeat { commit_index, commit_term } => {
                if self.is_leader(&msg.from) {
                    let has_commited = self.log.has(commit_index, commit_term)?;
                    if has_commited && commit_index > self.log.commited_index {
                        let old_commited_index = self.log.commited_index;
                        self.log.commited_index = commit_index;
                        let mut scan = self.log.scan(old_commited_index + 1..commit_index);
                        while let Some(entry) = scan.next().transpose()? {
                            self.state_tx.send(Apply { entry })?;
                        }
                    }
                }
                self.send(msg.from, Event::ConfirmLeader { commit_index, has_commited })
            }

            Event::ReplicateEntries { base_index, bas_term, entries } => {
                if self.is_leader(&msg.from) {
                    if base_index > 0 && !self.log.has(base_index, base_term)? {
                        debug!("Rejection log entries at base {}", base_index);
                        self.send(msg.from, Event::RejectEntries)?;
                    } else {
                        let last_index = self.log.splice(entries)?;
                        self.send(msg.from, Event::AcceptEntries { last_index, })?;
                    }
                }
            }

            Event::ClientRequest { ref id, .. } => {
                if let Some(leader) = self.role.leader.as_deref() {
                    self.proxied_reqs.insert(id.clone(), msg.from);
                    self.send(Address::Peer(leader.to_string()), msg.event)?;
                } else {
                    self.queued_reqs.insert(msg.from, msg.event);
                }
            },
        }
        Ok(())
    }

    pub fn tick(mut self) -> Result<Node> {
        self.role.leader_seen_ticks += 1;
        if self.role.leader_seen_ticks >= self.role.leader_seen_timeout {
            Ok(self.become_candidate()?.into())
        } else {
            Ok(self.into())
        }
    }

}