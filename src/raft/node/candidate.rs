use rand::Rng;

use super::{ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX, RoleNode, leader, follower::Follower};


pub struct Candidate {
    election_ticks: u64,
    election_timeout: u64,
    votes: u64,
}

impl Candidate {
    pub fn new() -> Self {
        Self {
            election_ticks: 0,
            election_timeout: rand::thread_rng()
                .gen_range(ELECTION_TIMEOUT_MIN..=ELECTION_TIMEOUT_MAX),
            votes: 1,
        }
    }
}

impl RoleNode<Candidate> {
    fn become_follower(mut self, term: u64, leader: &str) -> Result<RoleNode<Follower>> {
        
    }
}