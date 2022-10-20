use std::collections::HashMap;
use serde_derive::{Deserialize, Serialize};
use tokio::sync::mpsc;
use super::{log::Log, message::{Message, Address, Event}};
mod candidate;
use candidate::Candidate;
mod follower;
use follower::Follower;
mod leader;
use leader::Leader;



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
    state_tx: mpsc::UnboundedSender<Message>,
    queued_reqs: Vec<(Address, Event)>,
    proxied_reqs: HashMap<Vec<u8>, Address>,
    role: R,
}