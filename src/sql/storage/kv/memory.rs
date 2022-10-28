use std::cell::RefCell;
use std::cmp::Ordering;
use std::fmt::Display;
use std::num::NonZeroIsize;
use std::ops::Bound;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ops::Index;
use std::process::Child;
use std::sync::Arc;
use std::sync::RwLock;
use crate::error::Result;
use std::mem;
use super::Range;
use super::coding::*;
use super::Store;
use std::borrow::Cow;

const DEFAULT_NODE_NUM: usize = 8;

pub struct Memory {
    mem: Arc<RwLock<Node>>,
}

#[derive(PartialEq, Eq, Debug)]
enum Node {
    Root(Children),
    Inner(Children),
    Leaf(Values),
}

#[derive(PartialEq, Eq, Debug)]
struct Children {
    keys: Vec<Vec<u8>>,
    nodes: Vec<Node>,
}

#[derive(PartialEq, Eq, Debug)]
struct Values(Vec<(Vec<u8>, Vec<u8>)>);

impl Deref for Children {
    type Target = Vec<Node>;
    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
}

impl DerefMut for Children {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.nodes
    }
}

impl Deref for Values {
    type Target = Vec<(Vec<u8>, Vec<u8>)>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Values {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}


impl Memory {
    pub fn new() -> Self {
        Self::new_with_order(DEFAULT_NODE_NUM)
    }

    pub fn new_with_order(node_num: usize) -> Self {
        Memory { 
            mem: Arc::new(RwLock::new(
                Node::Root(Children::new(node_num)
            ))) ,
        }
    }

}

impl Store for Memory {
    
    fn set(&mut self, key: &[u8], val: Vec<u8>) -> Result<()> {
        self.mem.write()?.set(key, val);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.mem.read()?.get(key))
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.mem.write()?.delete(key);
        Ok(())
    }
    
    fn flush(&self) -> Result<()> {
        Ok(())
    }

    fn scan(&self, range: Range) -> Scan {
        Box::new(Iter::new(self.mem.clone(), range))
    }

}

impl Display for Memory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "memory")
    }
}

impl Node {
    fn set(&mut self, key: &[u8], val: Vec<u8>) -> Option<(Vec<u8>, Node)> {
        match self {
            Node::Root(children) => match children.set(key, val) {
                Some((split_key, new_rnode)) => {
                    let mut new_child = Children::new(children.capacity());
                    new_child.push(Node::Inner(mem::replace(children, Children::empty())));
                    new_child.push(Node::Inner(new_rnode));
                    new_child.keys.push(split_key);
                    *children = new_child;
                    return None;
                },
                None => { None },
            },

            Node::Inner(children) => {
                children.set(key, val).map(|(split_key, child)|(split_key, Node::Inner(child)))
            },

            Node::Leaf(values) => {
                values.set(key, val).map(|(split_key, child)| (split_key, Node::Leaf(child)))
            },
        }
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self {
            Node::Root(children) |Node::Inner(children) => {
                children.get(key)
            }
            Node::Leaf(value) => value.get(key),
        }
    }

    fn delete(&mut self, key: &[u8]) {
        match self {
            Node::Root(children) => {
                children.delete(key);
                while children.len() == 1 && matches!(children[0], Node::Inner(..)) {
                    if let Node::Inner(c) = children.remove(0) {
                        *children = c;
                    }
                }
                if children.len() == 1 && children[0].size() == 0 {
                    children.remove(0);
                }

            }

            Node::Inner(children) => { children.delete(key) }
            Node::Leaf(values) => { values.delete(key) }
        }
    }

    fn get_next(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        match self {
            Node::Leaf(value) => value.get_next(key),
            Node::Inner(child) | Node::Root(child) => child.get_next(key),
        }
    }

    fn get_prev(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        match self {
            Node::Leaf(value) => value.get_prev(key),
            Node::Root(child) | Node::Inner(child) => child.get_prev(key),
        }
    }

    fn get_last(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        match self {
            Node::Leaf(value) => value.get_last(),
            Node::Inner(child) | Node::Root(child) => child.get_last(),
        }
    }

    fn get_first(&self) ->Option<(Vec<u8>, Vec<u8>)> {
        match self {
            Node::Leaf(value) => value.get_first(),
            Node::Inner(child) | Node::Root(child) => child.get_first(),
        }
    }

    fn size(&self) -> usize {
        match self {
            Node::Root(child) | Node::Inner(child) => {
                child.len()
            }
            Node::Leaf(values) => { values.len() }
        }
    }

    fn capacity(&self) -> usize {
        match self {
            Node::Root(child) | Node::Inner(child) => {
                child.capacity()
            }
            Node::Leaf(values) => { values.capacity() }
        }
    }
}


impl Children {
    fn new(node_num: usize) -> Self{
        Children {
            keys: Vec::with_capacity(node_num),
            nodes: Vec::with_capacity(node_num),
        }
    }

        //todo: replace key with ref
    fn set(&mut self, key: &[u8], val: Vec<u8>) -> Option<(Vec<u8>, Children)>{
        if self.is_empty() {
            let mut value= Values::with_capacity(self.capacity());
            value.push((key.to_vec(), val));
            self.nodes.push(Node::Leaf(value));
            return None;
        }

        let (mut insert_at, child) = self.lookup_mut(key);

        if let Some((split_key, split_child)) = child.set(key, val) {
            insert_at += 1;

            if self.len() < self.capacity() {
                self.insert(insert_at, split_child);
                self.keys.insert(insert_at - 1, split_key);
                return None;
            }

            let mut split_at = self.len() / 2;
            if insert_at > split_at {
                split_at += 1;
            }

            let mut new_rnode = Children::new(self.capacity());
            new_rnode.extend(self.drain(split_at..));
            new_rnode.keys.extend(self.keys.drain(self.capacity() - new_rnode.len()..));

            let split_key = match insert_at.cmp(&split_at) {
                Ordering::Equal => {
                    new_rnode.insert(0, split_child);
                    new_rnode.keys.insert(0, self.keys.pop().unwrap());
                    split_key
                },
                Ordering::Greater => {
                    new_rnode.insert(self.capacity() - self.len(), split_child);
                    new_rnode.keys.insert(insert_at - self.len() - 1, split_key);
                    self.keys.pop().unwrap()
                },
                Ordering::Less => {
                    self.insert(insert_at, split_child);
                    self.keys.insert(insert_at - 1, split_key);
                    self.keys.pop().unwrap()
                },
            };
            Some((split_key, new_rnode))
            
        } else {
            None
        }
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if !self.is_empty() {
            let (_, node) = self.lookup(key);
            match node {
                Node::Inner(child) => {
                    child.get(key)
                },
                Node::Leaf(values) => {
                    values.get(key)
                },
                _ => { None } 
            }
        } else {
            None
        }
    }

    fn delete(&mut self, key: &[u8]) {
        if self.is_empty() {
            return;
        }

        let (index, node) = self.lookup_mut(key);
        node.delete(key);

        let nsize = node.size();

        if nsize > (node.capacity() + 1) / 2 {
            return;
        }

        let (lsize, lcap) = if index > 0 {
            (self[index - 1].size(), self[index - 1].capacity())
        } else {
            (0, 0)
        };

        let (rsize, rcap) = if index < self.len() - 1 {
            (self[index + 1].size(), self[index + 1].capacity())
        } else {
            (0, 0)
        };

        if lsize > (lcap + 1) / 2 {
            self.rotate_right(index - 1);
            
        } else if rsize > (rcap + 1) / 2 {
            self.rotate_left(index + 1);

        } else if lsize + nsize < (lcap + 1) / 2 {
            self.merge(index - 1);     
        } else if rsize + nsize < (rcap + 1) / 2 {
            self.merge(index);
        }
        
    }

    fn get_next(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.is_empty() {
            return None;
        }

        let (index, node) = self.lookup(key);
        if let Some(val) = node.get_next(key) {
            Some(val)
        } else if index < self.len() - 1 {
            self[index + 1].get_next(key)
        } else {
            None
        }
    }

    fn get_prev(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.is_empty() {
            return None;
        }

        let (index, node) = self.lookup(key);
        if let Some(val) = node.get_prev(key) {
            Some(val)
        } else if index > 0 {
            self[index - 1].get_prev(key)
        } else {
            None
        }
    }

    fn get_first(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.is_empty() {
            return None;
        }

        self[0].get_first()
    }

    fn get_last(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.is_empty() {
            return None;
        }

        self[self.len() - 1].get_last()
    }

    fn lookup(&self, key: &[u8]) -> (usize, &Node) {
        let id = self.keys.iter().position(|k| &**k > key).unwrap_or_else(|| self.keys.len() );
        (id, &self.nodes[id])
    }

    fn lookup_mut(&mut self, key: &[u8]) -> (usize, &mut Node) {
        let id = self.keys.iter().position(|k| &**k > key).unwrap_or_else(|| self.keys.len());
        (id, &mut self.nodes[id])
    }

    fn rotate_left(&mut self, index: usize) {
        match &mut self[index] {

            Node::Inner(child) => {
                let (key, value) = (child.keys.remove(index), child.remove(index));
                let kkey = std::mem::replace(&mut self.keys[index - 1], key);
                match &mut self[index - 1] {
                    Node::Inner(lchild) => {
                        lchild.push(value);
                        lchild.keys.insert(index, kkey);

                    },
                    _ => panic!("error left rotate "),
                }

            }
            Node::Leaf(values) => {
                let key = values[1].0.clone();
                let val = values.remove(0);
                self.keys[index] = values[1].0.clone();
                match &mut self[index - 1] {
                    Node::Leaf(lvalues) => {
                        lvalues.push(val);
                    }
                    _ => panic!("error left rotate"),
                }
            }
            _ => {}
        }
    }

    fn rotate_right(&mut self, index: usize) {
        match &mut self[index] {
            Node::Inner(child) => {
                let (key, node) = (child.keys.pop().unwrap(), child.pop().unwrap());
                match &mut self[index + 1] {
                    Node::Inner(rchild) => {
                        rchild.insert(0, node);
                        self.keys[index] = key;
                    },
                    _ => panic!("error right rotate"),
                }
            }
            Node::Leaf(values) => {
                let lvalues= values.pop().unwrap();
                self.keys[index] = lvalues.0.clone();
                match &mut self[index + 1] {
                    Node::Leaf(rvalues) => {
                        rvalues.push(lvalues);
                    }
                    _ => panic!("error right rotate"),
                }
            }
            _ => {}
        }
    }

    fn merge(&mut self, index: usize) {
        let parent_key = self.keys.remove(index);
        let rnode = &mut self.remove(index + 1);
        let lnode = &mut self[index];

        match (lnode, rnode) {
            (Node::Inner(lc), Node::Inner(rc)) => {
                lc.keys.push(parent_key);
                lc.keys.append(&mut rc.keys);
                lc.append(&mut rc.nodes);
            }
            (Node::Leaf(lc), Node::Leaf(rc)) => {
                lc.append(rc);
            }
            (_, _) => panic!("merge error"),
        }
    }

    fn empty() -> Self {
        Children { keys: Vec::new(), nodes: Vec::new() }
    }


}

impl Values {
    fn with_capacity(node_num: usize) -> Self {
        Values(Vec::with_capacity(node_num))
    }

    fn set(&mut self, key: &[u8], val: Vec<u8>) -> Option<(Vec<u8>, Values)> {
        let mut insert_at  = self.len();
        for (i, (k, value)) in self.iter_mut().enumerate() {
            match key.cmp(&**k) {
                Ordering::Equal => {
                    *value = val.to_vec();
                }
                Ordering::Greater => {
                    {}
                }
                Ordering::Less => {
                    insert_at = i;
                    break;
                }
            }
        }

        if self.len() < self.capacity() {
            self.insert(insert_at, (key.to_vec(), val));
            return None;
        }

        let mut split_at = self.capacity() / 2;
        if insert_at > split_at {
            split_at += 1;
        }

        let mut new_rleaf = Values::with_capacity(self.capacity());
        new_rleaf.extend(self.drain(split_at..));

        if insert_at < self.len() {
            self.insert(insert_at, (key.to_vec(), val));
        } else {
            new_rleaf.insert(insert_at - self.len(), (key.to_vec(), val));
        }
        let split_key = new_rleaf[0].0.clone();

        Some((split_key, new_rleaf))
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self
            .iter()
            .find_map(|(k, v)| 
                match key.cmp(&*k) {
                    Ordering::Equal => Some(v.to_vec()),
                    _ => None,
                }
            )
    }

    fn delete(&mut self, key: &[u8]) {
        for (i, (k, val)) in self.iter().enumerate() {
            match key.cmp(&*k) {
                Ordering::Equal => {
                    self.remove(i);
                    break;
                },
                Ordering::Greater => { },
                Ordering::Less => { break; },
            }
        }
    }

    //todo: return ref not clone
    fn get_last(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.0.last().cloned()
    }

    fn get_first(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.0.first().cloned()
    }

    fn get_next(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        self.iter().find_map(|(k, val)| {
            match key.cmp(&**k) {
                Ordering::Less => Some((k.to_vec(), val.to_vec())),
                _ => None
            }
        })
    }

    fn get_prev(&self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        self.iter().rev().find_map(|(k, val)| {
            match key.cmp(&**k) {
                Ordering::Greater => Some((k.to_vec(), val.to_vec())),
                _ => None,
            }
        })
    }

}

pub type Scan = Box<dyn DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Send>;

struct Iter {
    root: Arc<RwLock<Node>>,
    range: Range,
    front: Option<Vec<u8>>,
    back: Option<Vec<u8>>,
}

impl Iter {
    fn new(root: Arc<RwLock<Node>>, range: Range) -> Self {
        Iter { root, range, front: None, back: None }
    }

    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let root = self.root.read()?;
        let next = match &self.front {
            None => {
                match &self.range.start {
                    Bound::Included(start) => { 
                        root.get(start)
                            .map(|val| (start.to_vec(), val))
                            .or_else(||root.get_next(start))
                    },
                    Bound::Excluded(start) => root.get_next(start),
                    Bound::Unbounded => root.get_first(),
                }

            }
            Some(prev) => root.get_next(prev),
        };

        if let Some((k, _ )) = &next {
            if !self.range.contained(&k) {
                return Ok(None);
            }
            if let Some(bc) = &self.back {
                if k > bc {
                    return Ok(None);
                }
            }
            self.front = Some(k.clone());
        }
        Ok(next)

    }

    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let root = self.root.read()?;
        let prev = match &self.back {
            None => {
                match &self.range.end {
                    Bound::Included(key) => { 
                        root.get(key)
                        .map(|val| (key.to_vec(), val))
                        .or_else(||root.get_prev(key))
                     },
                    Bound::Excluded(key) => root.get_prev(key),
                    Bound::Unbounded => root.get_last(),
                }
            },
            Some(key) => root.get_prev(key),
        };
        if let Some((k, _ )) = &prev {
            if !self.range.contained(&k) {
                return Ok(None);
            }
            if let Some(fc) = &self.front {
                if fc > k {
                    return Ok(None);
                }
            }
            self.back = Some(k.clone());
        }
        Ok(prev)
    }
}

impl Iterator for Iter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl DoubleEndedIterator for Iter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

#[cfg(test)]
mod test {
    use std::{env, vec};

    use super::*;
    use pretty_assertions::assert_eq;
    use serde::{Deserialize, Serialize};
    use serde_derive::{Deserialize, Serialize};
    use crate::error::{Error, Result};

    enum Key<'a> {
        TxnNext,
        TxnActive(u64),
        TxnSnapshot(u64),
        TxnUpdate(u64, Cow<'a, [u8]>),
        Metadata(Cow<'a, [u8]>),
        Record(Cow<'a, [u8]>, u64),
    }
    
    #[derive(Copy, Debug, PartialEq, Serialize, Deserialize, Clone)]
    pub enum Mode {
        ReadWrite,
        ReadOnly,
        Snapshot { version: u64 }
    }
    
    impl Mode {
        fn mutable(&self) -> bool {
            match self {
                Mode::ReadWrite => true,
                _ => false,
            }
        }
    }
    
    impl<'a> Key<'a> {
        fn encode(&self) -> Vec<u8> {
            match &self {
                Key::TxnNext => vec![0x01],
                Key::TxnActive(id) => [&[0x02][..], &encode_u64(*id)].concat(),
                Key::TxnSnapshot(version) => [&[0x03][..], &encode_u64(*version)].concat(),
                Key::TxnUpdate(id, key) => [&[0x04][..], &encode_u64(*id), &encode_bytes(key)].concat(),
                Key::Metadata(meta) => [&[0x05][..], &encode_bytes(meta)].concat(),
                Key::Record(key, version) => [&[0xff][..], &encode_bytes(key), &encode_u64(*version)].concat(),
    
            }
        }
    
        fn decode(mut bytes: &[u8]) -> Result<Self> {
            let bytes = &mut bytes;
            
            match take_byte(bytes)? {
                0x01 => Ok(Key::TxnNext),
                0x02 => Ok(Key::TxnActive(take_u64(bytes)?)),
                0x03 => Ok(Key::TxnSnapshot(take_u64(bytes)?)),
                0x04 => Ok(Key::TxnUpdate(take_u64(bytes)?, take_bytes(bytes)?.into())),
                0x05 => Ok(Key::Metadata(take_bytes(bytes)?.into())),
                0xff => Ok(Key::Record(take_bytes(bytes)?.into(), take_u64(bytes)?)),
                _ => return Err(Error::Value("decode error".to_string()))
            }
        }
    }
    
    fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
        Ok(bincode::serialize(value)?)
    }
    
    fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
        Ok(bincode::deserialize(bytes)?)
    }

    #[test]
    fn set_test() -> Result<()> {
        env::set_var("RUST_BACKTRACE", "1");
        let mut mem = Memory::new();
        let mut char = vec![0x0001];
        let mut val = 0x0001;
        for i in 0..200 {
            mem.set(&char, vec![val]);
            assert_eq!(mem.get(&char)?.unwrap(), vec![val]);
            char[0] += 1;
            val += 1;
        }


        let mut char = vec![0x55];
        for i in 55..169 {
            // assert!(mem.get(&char)?.is_some());
            mem.delete(&char);
            // assert!(mem.get(&char)?.is_none());
            char[0] += 1;
        }

        let mut tval = 0x01;
        let mut scan = mem.scan(Range::from(&vec![0x01]..=&vec![0x20]));
        for i in 0..=20 {
            if let Some((_, val)) = scan.next().transpose()? {
                assert_eq!(val, vec![tval]);
                tval += 1;
            }
        }
        
        let mut rev_scan = mem.scan(Range::from(&vec![0x01]..=&vec![0x20])).rev();
        if let Some((_, val)) = rev_scan.next().transpose()? {
            assert_eq!(val, vec![0x20]);
        }
        Ok(())
    }

    #[test]
    fn scan_test() -> Result<()> {
        let mut mem = Memory::new();

        mem.set(&vec![0x01], vec![0x10]);
        mem.set(&vec![0x02], vec![0x20]);
        mem.set(&vec![0x03], vec![0x30]);
        let mut scan = mem.scan(Range::from(&vec![0x01]..=&vec![0x03]));
        let (_, val) = scan.next().transpose()?.unwrap();
        assert_eq!(val, vec![0x10]);
        let (_, val) = scan.next().transpose()?.unwrap();
        assert_eq!(val, vec![0x20]);
        let (_, val) = scan.next().transpose()?.unwrap();
        assert_eq!(val, vec![0x30]);

        let mut rscan = mem.scan(Range::from(&vec![0x01]..=&vec![0x03])).rev();
        let (_, val) = rscan.next().transpose()?.unwrap();
        assert_eq!(val, vec![0x30]);
        let (_, val) = rscan.next().transpose()?.unwrap();
        assert_eq!(val, vec![0x20]);
        let (_, val) = rscan.next().transpose().map_err(|err| Error::Internal("err here".into()))?.unwrap();
        assert_eq!(val, vec![0x10]);
     
        Ok(())
    }

    #[test]
    fn delete() -> Result<()> {
        let mut mem = Memory::new();

        mem.set(&Key::TxnActive(1).encode(), vec![0x12]);
        mem.delete(&Key::TxnActive(1).encode());
        assert!(mem.get(&Key::TxnActive(1).encode())?.is_none());

        mem.set(&Key::TxnActive(2).encode(), vec![0x12]);
        mem.delete(&Key::TxnActive(2).encode());
        assert!(mem.get(&Key::TxnActive(2).encode())?.is_none());

        mem.set(&Key::TxnActive(3).encode(), vec![0x12]);
        mem.delete(&Key::TxnActive(3).encode());
        assert!(mem.get(&Key::TxnActive(3).encode())?.is_none());

        mem.set(&Key::TxnActive(4).encode(), vec![0x12]);
        mem.delete(&Key::TxnActive(4).encode());
        assert!(mem.get(&Key::TxnActive(4).encode())?.is_none());

        mem.set(&Key::TxnActive(5).encode(), vec![0x12]);
        mem.delete(&Key::TxnActive(5).encode());
        assert!(mem.get(&Key::TxnActive(5).encode())?.is_none());

        mem.set(&Key::TxnActive(6).encode(), vec![0x12]);
        mem.delete(&Key::TxnActive(6).encode());
        assert!(mem.get(&Key::TxnActive(6).encode())?.is_none());

        mem.delete(&[0x01]);
        assert!(mem.get(&[0x01])?.is_none());
        Ok(())
    }

    fn debug() {
        
    }
}






