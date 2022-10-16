mod memory;
pub use memory::Memory;
// mod mvcc;
use std::ops::{Bound, RangeBounds};
pub mod coding;
pub mod types;


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



