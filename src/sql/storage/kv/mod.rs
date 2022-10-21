mod memory;
pub use memory::Memory;
mod mvcc;
pub use mvcc::{Mode, Mvcc};
pub mod coding;
pub use coding::*;
use std::{ops::{Bound, RangeBounds}, fmt::Display};
use crate::error::Result;
use self::memory::Scan;
use super::{
    Value, Datatype, 
    engine::{Catalog, Row, Tables, IndexScan, KScan, Transaction},
    Expression,
};
pub mod kv;
pub use kv::Kv;

pub trait Store: Display + Send + Sync {
    fn set(&mut self, key: &[u8], val:Vec<u8>) -> Result<()>;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn delete(&mut self, key: &[u8]) -> Result<()>;

    fn flush(&self) -> Result<()>;

    fn scan(&self, range: Range) -> Scan;
}


pub struct Range {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
}

impl Range {

    pub fn from<R: RangeBounds<Vec<u8>>>(range: R) -> Self {
        Self {
            start: match range.start_bound() {
                Bound::Excluded(k) =>  Bound::Excluded(k.to_vec()),
                Bound::Included(k) =>  Bound::Included(k.to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
            
            end: match range.end_bound() {
                Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
                Bound::Included(k) => Bound::Included(k.to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            }
        }
    }

    pub fn contained(&self, key: &Vec<u8>) -> bool {
        (match &self.start {
            Bound::Excluded(k) => key > k,
            Bound::Included(k) => key >= k,
            Bound::Unbounded => true,
        } && match &self.end {
            Bound::Excluded(k) => key < k,
            Bound::Included(k) => key <= k,
            Bound::Unbounded => true,
        })
    }

}



