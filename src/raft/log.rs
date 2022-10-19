use serde_derive::{Deserialize, Serialize};
use super::Status;


#[derive(Deserialize, Serialize)]
pub struct Entry {
    pub index: u64,
    pub term: u64,
    pub command: Option<Vec<u8>>,
}