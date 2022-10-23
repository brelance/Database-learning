use std::collections::HashMap;
use log::info;
use serde_derive::{Deserialize, Serialize};
use tokio::sync::mpsc;
use super::{
    log::Log, 
    message::{Message, Address, Event}, status::Instruction,
    status::Driver
};
mod candidate;
use candidate::Candidate;
mod follower;
use follower::Follower;
mod leader;
use leader::Leader;
use crate::error::{Error, Result};
use crate::State;

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

impl Node {
    pub async fn new(
        id: &str,
        peers: Vec<String>,
        log: Log,
        mut state: Box<dyn State>,
        node_tx: mpsc::UnboundedSender<Message>,
    ) -> Result<Self> {
        let applied_index = state.applied_index();
        if applied_index > log.commited_index {
            return Err(Error::Internal(format!(
                "State machine applied index {} greater than log commited index {}",
                applied_index, log.commited_index
            )));
        }
        let (state_tx, state_rx) = mpsc::unbounded_channel();
        let mut driver = Driver::new(state_rx, node_tx);
        if log.commited_index > applied_index {
            info!("Replaying log entries {} to {}", applied_index + 1, log.commited_index);
            driver.replay(&mut *state, log.scan((applied_index + 1)..=log.commited_index))?;
        }
        tokio::spawn(driver.driver(state));
        let (term, voted_for) = log.load_term()?;
        let node = RoleNode {
            id: id.to_owned(),
            peers,
            term,
            log,
            node_tx,
            state_tx,
            queued_reqs: Vec::new(),
            proxied_reqs: HashMap::new(),
            role: Follower::new(None, voted_for.as_deref()),
        };
        if node.peers.is_empty() {
            info!("No peers specified, starting as leader");
            let last_index = node.log.last_index;
            Ok(node.become_role(Leader::new(vec![], last_index))?.into())
        } else {
            Ok(node.into())
        }

    }

    pub fn id(&self) -> String {
        match self {
            Node::Candidate(n) => n.id.clone(),
            Node::Follower(n) => n.id.clone(),
            Node::Leader(n) => n.id.clone(),
        }
    } 
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

    fn send(mut self, to: Addrwess, event: Event) -> Result<()> {
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
        match &msg.to {
            Address::Peer(id) if id == &self.id => Ok(()),
            Address::Local => Ok(()),
            Address::Peers => Ok(()),
            Address::Peer(id) => {
                Err(Error::Internal(format!("Received message for other node {}", id)))
            }
            Address::Client => Err(Error::Internal("Received message for client".into())),
        }
    }

    fn abort_proxied(&mut self) -> Result<()> {
        for (id, address) in std::mem::take(&mut self.proxied_reqs) {
            self.send(address, Event::ClientResponse { id, response: Err(Error::Abort) })?;
        }
        Ok(())
    }

    fn forward_queued(&mut self, leader: Address) -> Result<()> {
        for (from, event) in std::mem::take(&mut self.queued_reqs) {
            if let Event::ClientRequest { id, .. } = &event {
                self.proxied_reqs.insert(id.clone(), from.clone());
                self.node_tx.send(
                    Message {
                        term: 0,
                        from: match from {
                            Address::Client => Address::Local,
                            address => address,
                        },
                        to: leader.clone(),
                        event,
                    }
                )?;
            }
        }
        Ok(())
    }

    fn quorum(&self) -> u64 {
        (self.peers.len as u64 + 1) / 2 + 1
    }


}

impl From<RoleNode<Candidate>> for Node {
    fn from(rn: RoleNode<Candidate>) -> Self {
        Node::Candidate(rn)
    }
}

impl From<RoleNode<Leader>> for Node {
    fn from(rn: RoleNode<Leader>) -> Self {
        Node::Leader(rn)
    }
}

impl From<RoleNode<Follower>> for Node {
    fn from(rn: RoleNode<Follower>) -> Self {
        Node::Follower(rn)
    }
}