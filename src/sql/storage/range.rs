use std::ops::{Bound, RangeBounds};


pub struct Range {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
}

impl Range {

    pub fn from<R: RangeBounds<Vec<u8>>>(range: R) -> Self {
        Self {
            start: match range.start_bound() {
                Bound::Excluded(v) =>  Bound::Excluded(*v),
                Bound::Included(v) =>  Bound::Included(*v),
                Bound::Unbounded => Bound::Unbounded,
            },
            
            end: match range.end_bound() {
                Bound::Excluded(v) => Bound::Excluded(*v),
                Bound::Included(v) => Bound::Included(*v),
                Bound::Unbounded => Bound::Unbounded,
            }
        }
    }

    pub fn contained(&self, key: &Vec<u8>) -> bool {

    }

}



