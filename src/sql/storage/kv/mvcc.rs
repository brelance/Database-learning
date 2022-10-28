use super::{Store, encode_u64, encode_bytes, memory};
use std::{borrow::Cow, collections::HashSet, sync::{Arc, RwLock, RwLockWriteGuard, RwLockReadGuard}, clone, mem, ops::{RangeBounds, Bound}, iter::Peekable};
use super::{coding::*, Range};
use crate::{error::{Result, Error}};
use super::{Value};

use log::{info, debug};
use serde::{Serialize, Deserialize};
use serde_derive::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
pub struct Status {
    pub txns: u64,
    pub txns_active: u64,
    pub storage: String,
}



pub struct Mvcc {
    store: Arc<RwLock<Box<dyn Store>>>,
}

impl Mvcc {
    pub fn begin(&self) -> Result<Transaction> {
        Transaction::begin(self.store.clone(), Mode::ReadWrite)
    }

    pub fn begin_with_mode(&self, mode: Mode) -> Result<Transaction>{
        Transaction::begin(self.store.clone(), mode)
    }

    pub fn resume(&self, txn_id: u64) -> Result<Transaction> {
        Transaction::resume(self.store.clone(), txn_id)
    }

    pub fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let mut session = self.store.write()?;
        session.set(key, value);
        Ok(())
    }

    pub fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let session = self.store.read()?;
        session.get(key)
    }

    pub fn status(&self) -> Result<Status> {
        let store = self.store.read()?;
        return Ok(
            Status {
                txns: match store.get(&Key::TxnNext.encode())? {
                    Some(ref v) => deserialize(v)?,
                    None => 1,
                } - 1,
                txns_active: store
                .scan(Range::from(
                    Key::TxnActive(0).encode()..Key::TxnActive(std::u64::MAX).encode(),
                ))
                .try_fold(0, |count, r| r.map(|_| count + 1))?,
            storage: store.to_string(),     
            });
    }
}

impl Clone for Mvcc {
    fn clone(&self) -> Self {
        Mvcc { store: self.store.clone() }
    }
}

pub struct Transaction {
    storage: Arc<RwLock<Box<dyn Store>>>,
    txn_id: u64,
    mode: Mode,
    snapshot: Snapshot,
}

impl Transaction {
    pub fn begin(store: Arc<RwLock<Box<dyn Store>>>, mode: Mode) -> Result<Self> {
        let mut session = store.write()?;
        let txn_id: u64 = match session.get(&Key::TxnNext.encode())? {
            Some(id) => deserialize(&id)?,
            None => 1,
        };

        session.set(&Key::TxnNext.encode(), serialize(&(txn_id + 1))?)?;
        session.set(&Key::TxnActive(txn_id).encode(), serialize(&mode)?)?;
        let mut snapshot = Snapshot::take(&mut session, txn_id)?;


        mem::drop(session);

        if let Mode::Snapshot { version } = &mode {
            snapshot = Snapshot::restore(&store.read()?, *version)?;
        }

        Ok(Self {
            storage: store.clone(),
            txn_id,
            mode,
            snapshot,
        })
    }

    pub fn resume(store: Arc<RwLock<Box<dyn Store>>>, id: u64) -> Result<Self>{
        let session = store.read()?;
        let snapshot = Snapshot{version: 0, invisible: HashSet::new() };
        let (mode, snapshot) = if let Some(mode) = session.get(&Key::TxnActive(id).encode())? {
            match deserialize(&mode)? {
                Mode::Snapshot { version } => (mode, Snapshot::restore(&session, version)?),
                _ => (mode, Snapshot::restore(&session, id)?),
            }
        } else {
            return Err(Error::Value(format!("No active transaction {}", id)));
        };

        Ok(Transaction { storage: store.clone(), txn_id: id, mode: deserialize(&mode)?, snapshot, })
    }



    pub fn rollback(&self) -> Result<()> {
        let mut session = self.storage.write()?;
        let mut rollback = Vec::new();
        if self.mode.mutable() {
            let mut scan = 
                session
                .scan(Range::from(Key::TxnUpdate(self.txn_id, vec![].into()).encode()..Key::TxnUpdate(self.txn_id + 1,vec![].into()).encode()));
            while let Some((k, value)) = scan.next().transpose()? {
                match Key::decode(&k)? {
                    Key::TxnUpdate(_, key) => rollback.push(key),
                    _ => return Err(Error::Internal("rollback error".to_string())),
                }
            }
            for key in rollback {
                session.delete(&key)?;
            }
            mem::drop(session);
        }

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let session = self.storage.read()?;
        let mut scan = 
            session.scan(Range::from(Key::Record(key.into(), 0).encode()
            ..=Key::Record(key.into(), self.id()).encode()))
            .rev();

        while let Some((k, v)) = scan.next().transpose()? {
            match Key::decode(&k)? {
                Key::Record(_ , version) => {
                    if self.snapshot.is_visiable(version) {
                        return deserialize(&v);
                    }
                }
                k => return Err(Error::Internal(format!("Expected Txn::Record, got {:?}", k))),
            };
        }
        Ok(None)
    }

    pub fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.write(key, Some(value))
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write(key,None )
    }

    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        if !self.mode.mutable() {
            return Err(Error::ReadOnly);
        }
        let mut session = self.storage.write()?;
        let min = self.snapshot.invisible.iter().min().cloned().unwrap_or(self.txn_id + 1);
        println!("min = {}", min);

        let mut scan = 
            session.scan(Range::from(Key::Record(key.into(), min).encode()..=Key::Record(key.into(), u64::MAX).encode()))
            .rev();
        
        while let Some((k, _ )) = scan.next().transpose()? {
            match Key::decode(&k)? {
                Key::Record(k, version) => {
                    println!("kye = {:?}", k.to_vec());
                    if !self.snapshot.is_visiable(version) {
                        return Err(Error::Serialization);
                    }
                }
                k => return Err(Error::Internal(format!("Expected Txn::Record, got {:?}", k))),
            };
        }
        std::mem::drop(scan);
        
        let update = Key::TxnUpdate(self.txn_id, key.into()).encode();
        println!("update = {:?}", update);
        let record = Key::Record(key.into(), self.txn_id).encode();
        session.set(&update, vec![])?;
        println!("recore = {:?}", record);
        session.set(&record, serialize(&value)?)
    }

    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<super::Scan> {
        let start = match range.start_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Record(k.into(), std::u64::MAX).encode()),
            Bound::Included(k) => Bound::Included(Key::Record(k.into(), 0).encode()),
            Bound::Unbounded => Bound::Included(Key::Record(vec![].into(), 0).encode()),
        };

        let end = match range.end_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Record(k.into(), 0).encode()),
            Bound::Included(k) => Bound::Included(Key::Record(k.into(), std::u64::MAX).encode()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let scan = self.storage.read()?.scan(Range::from((start, end)));
        Ok(Box::new(Scan::new(scan, self.snapshot.clone())))
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<super::Scan> {
        if prefix.is_empty() {
            return Err(Error::Internal("Scan prefix cannot be empty".into()));
        }

        let start = prefix.to_vec();
        let mut end = start.clone();
        for i in (0..end.len()).rev() {
            match end[i] {
                0xff if i == 0 => return Err(Error::Internal("Invalid prefix scan range".into())),
                0xff => {
                    end[i] = 0x00;
                    continue;
                }
                v => {
                    end[i] = v + 1;
                    break;
                }
            }
        }
        self.scan(start..end)
    }


    pub fn commit(self) -> Result<()>{
        println!("commit here");
        let mut session = self.storage.write()?;
        session.delete(&Key::TxnActive(self.txn_id).encode())?;
        println!("delete txnActive");
        Ok(())
    }

    pub fn id(&self) -> u64 {
        self.txn_id
    }

    pub fn mode(&self) -> Mode {
        self.mode.clone()
    }
    
}

#[derive(Clone)]
struct Snapshot {
    version: u64,
    invisible: HashSet<u64>,
}

impl Snapshot {
    fn take(session: &mut RwLockWriteGuard<Box<dyn Store>>, version: u64) -> Result<Self> {
        let mut scan = 
            session.scan(Range::from(Key::TxnActive(1).encode()..Key::TxnActive(version).encode()));
        let mut invisible = HashSet::new();
        while let Some((key, _ )) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxnActive(id) => { 
                    invisible.insert(id);
                    println!("invisiableid {}", id); 
                }
                val => return Err(Error::Internal("snapshot take error".to_string())),
            };
        }

        mem::drop(scan);
        session.set(&Key::TxnActive(version).encode(), serialize(&invisible)?);
        
        Ok(Snapshot { version, invisible })
    }

    fn restore(session: &RwLockReadGuard<Box<dyn Store>>, version: u64) -> Result<Self> {
        match session.get(&Key::TxnSnapshot(version).encode())? {
            Some(inner) => Ok(Snapshot {version, invisible: deserialize(&inner)? }),
            None => return Err(Error::Internal("restore error".to_string())),
        }
    }

    fn is_visiable(&self, version: u64) -> bool {
        version <= self.version && self.invisible.get(&version).is_none()
    }
}

pub struct Scan {
    scan: Peekable<super::Scan>,
    next_back_seen: Option<Vec<u8>>,
}

impl Scan {
    fn new(mut scan: super::Scan, snapshot: Snapshot) -> Self {
        scan = Box::new(scan.filter_map(move |r| {
            r.and_then(|(k, v)| match Key::decode(&k)? {
                Key::Record(_, version) if !snapshot.is_visiable(version) => Ok(None),
                Key::Record(key, _) => Ok(Some((key.into_owned(), v))),
                k => Err(Error::Internal(format!("Expected Record, got {:?}", k))),
            })
            .transpose()
        }));
        Self {scan: scan.peekable(), next_back_seen: None}
    }

    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.scan.next().transpose()? {
            if match self.scan.peek() {
                Some(Ok((peek_key, _))) if *peek_key != key => true,
                Some(Ok(_)) => false,
                Some(Err(err)) => return Err(err.clone()),
                None => true,
            } {
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }

    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {

        while let Some((key, value)) = self.scan.next_back().transpose()? {
            if match &self.next_back_seen {
                Some(seen_key) if *seen_key != key => true,
                Some(_) => false,
                None => true,
            } {
                self.next_back_seen = Some(key.clone());
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }
}

impl Iterator for Scan {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl DoubleEndedIterator for Scan {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}

#[derive(Debug)]
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


#[cfg(test)]
mod test {
    use log::RecordBuilder;
    use memory::Memory;

    use crate::sql::storage::kv::kv::Txn;

    use super::*;
    use std::{env, vec};

    fn setup() -> Mvcc {
        Mvcc { store: Arc::new(RwLock::new(Box::new(Memory::new()))) }
    }

    #[test]
    fn mvcc_test() -> Result<()> {
        let mut mvcc = setup();
        let mut txn = mvcc.begin()?;
        assert_eq!(txn.id(), 1);
        let key = "key".as_bytes();
        txn.set(key, vec![0x01]);

        let val = txn.get(key)?.unwrap();
        let version = txn.storage
            .read()?
            .get(&Key::TxnUpdate(1, Cow::Borrowed(key)).encode())?.unwrap();
        assert_eq!(val, vec![0x01]);

        let record = txn.storage
            .read()?
            .get(&Key::Record(Cow::Borrowed(key), 1).encode())?
            .map(|val| deserialize::<Option<Vec<u8>>>(&val))
            .transpose()?
            .unwrap();
        println!("record1 = {:?}", record.unwrap());

        let record = txn.storage
            .read()?
            .get(&Key::TxnActive(1).encode())?;
        println!("record2 = {:?}", record.unwrap());

        txn.storage.write()?.delete(&Key::TxnActive(1).encode())?;
        let record = txn.storage
            .read()?
            .get(&Key::TxnActive(1).encode())?;
        println!("record3 = {:?}", record);

        txn.commit()?;

        let mut txn2 = mvcc.begin()?;
        assert_eq!(txn2.id(), 2);
        // let record = txn2.storage
        //     .read()?
        //     .get(&Key::TxnActive(1).encode())?;
        // println!("record4 = {:?}", record.unwrap());

        let record = txn2.storage
            .read()?
            .get(&Key::TxnActive(2).encode())?;
        println!("record5 = {:?}", record.unwrap());

        assert_eq!(txn2.snapshot.version, 2);
        txn2.set(key, vec![0x10]);
        txn2.storage.write()?.delete(&Key::TxnActive(1).encode())?;
        txn2.commit()?;

        let mut txn3 = mvcc.begin()?;
        let mut txn4 = mvcc.begin()?;
        let mut txn5 = mvcc.begin()?;
        let mut txn6 = mvcc.begin()?;
        Ok(())
    }

    #[test]
    fn encode_test() -> Result<()> {
        let mvcc = setup();

        let mut txn = mvcc.begin()?;
        let recore = Key::Record(Cow::Owned(vec![0x00, 0x01, 0x03]), 1);
        let encode = recore.encode();
        txn.set(&encode, vec![0x01])?;

        let recore = Key::Record(Cow::Owned(vec![0x00, 0x01, 0x03]), 2);
        let encode = recore.encode();
        txn.set(&encode, vec![0x02])?;

        let recore = Key::Record(Cow::Owned(vec![0x00, 0x01, 0x03]), 3);
        let encode = recore.encode();
        txn.set(&encode, vec![0x03])?;

        let recore = Key::Record(Cow::Owned(vec![0x00, 0x01, 0x03]), 4);
        let encode = recore.encode();
        txn.set(&encode, vec![0x04])?;
        assert_eq!(txn.get(&encode)?.unwrap(), vec![0x04]);

        let mut txn2 = mvcc.begin()?;
        println!("here");
        txn2.set(&encode, vec![0x05])?;
        assert!(txn2.get(&encode)?.is_none());

        Ok(())
    }
}