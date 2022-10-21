use std::collections::HashMap;
use serde_derive::{Deserialize, Serialize};
use tokio::sync::mpsc;
use super::{log::Log, message::{Message, Address, Event}, status::Instruction};
mod candidate;
use candidate::Candidate;
mod follower;
use follower::Follower;
mod leader;
use leader::Leader;
use crate::error::{Error, Result};

const HEARTBEAT_INTERVAL: u64 = 1;
const ELECTION_TIMEOUT_MIN: u64 = 8 * HEARTBEAT_INTERVAL;
const ELECTION_TIMEOUT_MAX: u64 = 15 * HEARTBEAT_INTERVAL;



#[derive(Debug, Serialize, Deserialize)]
pub struct Status {
    pub server: String,
    pub leader: String,
    pub term: u64,
    pub node_last_index: HashMap<String, u64>,
    pub commit_index: u64,
    pub apply_index: u64,
    pub storage: String,
    pub storage_size: u64,
}

pub enum Node {
    Leader(RoleNode<Leader>),
    Candidate(RoleNode<Candidate>),
    Follower(RoleNode<Follower>),
}



pub struct RoleNode<R> {
    id: String,
    peers: Vec<String>,
    term: u64,
    log: Log,
    node_tx: mpsc::UnboundedSender<Message>,
    state_tx: mpsc::UnboundedSender<Instruction>,
    queued_reqs: Vec<(Address, Event)>,
    proxied_reqs: HashMap<Vec<u8>, Address>,
    role: R,
}

impl<R> RoleNode<R> {
    fn become_role<T>(self, role: T) -> Result<RoleNode<T>> {
        Ok(
            RoleNode {
                id: self.id,
                peers: self.peers,
                term: self.term,
                log: self.log,
                node_tx: self.node_tx,
                state_tx: self.state_tx,
                queued_reqs: self.queued_reqs,
                proxied_reqs: self.proxied_reqs,
                role,
            })
    }

    fn send(mut self, to: Address, event: Event) -> Result<()> {
        let msg = Message {
            term: self.term,
            from: Address::Local,
            to,
            event,
        };
        Ok(self.node_tx.send(msg)?)
    }

    fn validate(&self, msg: &Message) -> Result<()> {
        match msg.from {
            Address::Peers => return Err(Error::Internal("Message from broadcast address".into())),
            Address::Local => return Err(Error::Internal("Message from local node".into())),
            Address::Client if !matches!(msg.event, Event::ClientRequest { .. }) => {
                return Err(Error::Internal("Non-request message from client".into()));
            }
            _ => {}
        }

        if msg.term < self.term 
            && !matches!(msg.event, Event::ClientRequest { .. } | Event::ClientResponse { .. })
        {
            return Err(Error::Internal(format!("Message from past term {}", msg.term)));
        }
    }


}
