mod log;
use std::ops::{Bound, RangeBounds};

pub use crate::error::{Error, Result};
pub use self::log::LogStore;

use self::log::Scan;


pub trait Store {
    fn append(&mut self, entry: Vec<u8>) -> Result<u64>;
    
    fn commit(&mut self, index: u64) -> Result<()>;

    fn len(&self) -> u64;

    fn get(&self, index: u64) -> Result<Option<Vec<u8>>>;

    fn scan(&self, range: Range) -> Scan;

    fn size(&self) -> u64;

    fn commited(&self) -> u64;

    fn truncate(&mut self, index: u64) -> Result<u64>;

    fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;
    
}

pub struct Range {
    start: Bound<u64>,
    end: Bound<u64>,
}

impl Range {
    pub fn from(range: impl RangeBounds<u64>) -> Self {
        Self {
            start: match range.start_bound() {
                Bound::Excluded(v) => Bound::Excluded(*v),
                Bound::Included(v) => Bound::Included(*v),
                Bound::Unbounded => Bound::Unbounded,
            },
            end: match range.end_bound() {
                Bound::Excluded(v) => Bound::Excluded(*v),
                Bound::Included(v) => Bound::Included(*v),
                Bound::Unbounded => Bound::Unbounded,
            }
        }
    }
}