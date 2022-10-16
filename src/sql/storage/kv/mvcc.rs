use super::{Memory, Store, encode_u64, encode_bytes};
use std::{borrow::Cow, collections::HashSet, sync::{Arc, RwLock, RwLockWriteGuard, RwLockReadGuard}, clone, mem};
use super::{coding::*, Range};
use crate::error::{Result, Error};

use serde::{Serialize, Deserialize, de::value};
use serde_derive::{Serialize, Deserialize};



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

    pub fn set_metadata() {}

    pub fn get_metadata() {}

    pub fn status() {}
}

impl Clone for Mvcc {
    fn clone(&self) -> Self {
        Mvcc { store: self.store.clone() }
    }
}



struct Transaction {
    storage: Arc<RwLock<Box<dyn Store>>>,
    txn_id: u64,
    mode: Mode,
    snapshot: Snapshot,
}

impl Transaction {
    fn begin(store: Arc<RwLock<Box<dyn Store>>>, mode: Mode) -> Result<Self> {
        let mut session = store.write()?;
        let txn_id: u64 = match session.get(&Key::TxnNext.encode())? {
            Some(id) => deserialize(&id)?,
            None => 1,
        };

        session.set(&Key::TxnNext.encode(), serialize(&(txn_id + 1))?);
        session.set(&Key::TxnActive(txn_id).encode(), serialize(&mode)?);
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

    fn resume(store: Arc<RwLock<Box<dyn Store>>>, id: u64) -> Result<Self>{
        let session = store.read()?;
        let mut snapshot = Snapshot{version: 0, invisible: HashSet::new() };
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



    fn rollback(&self) -> Result<()> {
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

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>{
        let session = self.storage.read()?;
        let mut scan = 
            session.scan(Range::from(Key::Record(key.into(), 0).encode()..=Key::Record(key.into(), self.id()).encode()));
        

    }

    fn commit(self) -> Result<()>{
        let mut session = self.storage.write()?;
        session.delete(&Key::TxnActive(self.txn_id).encode())?;
        Ok(())
    }

    fn id(&self) -> u64 {
        self.txn_id
    }

    fn mode(&self) -> Mode {
        self.mode.clone()
    }
    
}

struct Snapshot {
    version: u64,
    invisible: HashSet<u64>,
}

impl Snapshot {
    fn take(session: &mut RwLockWriteGuard<Box<dyn Store>>, version: u64) -> Result<Self> {
        let mut scan = 
            session.scan(Range::from(Key::TxnActive(0).encode()..Key::TxnActive(version).encode()));
        let mut invisible = HashSet::new();
        while let Some((key, _ )) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxnActive(id) => invisible.insert(id),
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

    fn is_visiable() {}
}

enum Key<'a> {
    TxnNext,
    TxnActive(u64),
    TxnSnapshot(u64),
    TxnUpdate(u64, Cow<'a, [u8]>),
    Metadata(Cow<'a, [u8]>),
    Record(Cow<'a, [u8]>, u64),
}

#[derive(Serialize, Deserialize, Clone)]
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