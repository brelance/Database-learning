use rand::Rng;

use super::{leader, ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX};


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

