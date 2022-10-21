use regex::bytes;
use serde_derive::{Deserialize, Serialize};
use serde::{Serialize, Deserialize};
use crate::Store;
use crate::error::{Error, Result};

#[derive(Deserialize, Serialize)]
pub struct Entry {
    pub index: u64,
    pub term: u64,
    pub command: Option<Vec<u8>>,
}

pub enum Key {
    TernVote,
}

impl Key {
    fn encode(self) -> Vec<u8> {
        match self {
            Self::TernVote => vec![0x00],
        }
    }
}

pub struct Log {
    store: Box<dyn Store>,
    commited_index: u64,
    commited_term: u64,
    last_index: u64,
    last_term: u64,
}

impl Log {
    fn new(store: Box<dyn Store>) -> Result<Self> {
        let (commited_index, commited_term) = match store.commited() {
            0 => (0, 0),
            i => {
                store.get(i)?
                    .map(|val| deserialize::<Entry>(&val))
                    .transpose()?
                    .map(|entry| (entry.index, entry.term))
                    .ok_or_else(|| Error::Internal("Last entry not found".into()))?
            },
        };
        let (last_index, last_term) = match store.len() {
            0 => (0, 0),
            i => {
                store.get(i)?
                    .map(|val| deserialize::<Entry>(&val))
                    .transpose()?
                    .map(|entry| (entry.index, entry.term))
                    .ok_or_else(|| Error::Internal("Last entry not found".into()))?
            },
        };
        Ok(Log {
            store,
            commited_index,
            commited_term,
            last_index,
            last_term,
        })
    }

    fn append(&mut self, term: u64, command: Option<Vec<u8>>) -> Result<Entry> {
        let entry = Entry {index: self.last_index + 1, term, command,};
        self.store.append(serialize(&entry)?);
        self.last_index = self.last_index + 1;
        self.last_term = term;
        Ok(entry)
    }

    fn commit(&mut self, index: u64) -> Result<u64> {
        let entry = self
            .get(index)?
            .ok_or_else(|| Error::Internal(format!("Entry {} not found", index)))?;
        self.store.commit(index)?;
        self.commited_index = entry.index;
        self.commited_term = entry.term;
        Ok(index)
    }

    fn get(&self, index: u64) -> Result<Option<Entry>> {
        self.store
            .get(index)?
            .map(|val| deserialize::<Entry>(&val))
            .transpose()
    }

    pub fn splice(&mut self, entries: Vec<Entry>) -> Result<u64> {
        for i in 0..entries.len() {
            if i == 0 && entries.get(i).unwrap().index > self.last_index + 1 {
                return Err(Error::Internal("Spliced entries cannot begin past last index".into()));
            }
            if entries.get(i).unwrap().index != entries.get(0).unwrap().index + i as u64 {
                return Err(Error::Internal("Spliced entries must be contiguous".into()));
            }   
        }
        for entry in entries {
            if let Some(ref current) = self.get(entry.index)? {
                if current.term == entry.term {
                    continue;
                }
                self.truncate(entry.index - 1)?;
            }
            self.append(entry.term, entry.command)?;
        }
        Ok(self.last_index)
    }

    pub fn truncate(&mut self, index: u64) -> Result<u64> {
        let (index, term) = match self.store.truncate(index)? {
            0 => (0, 0),
            i => {
                self.store
                    .get(index)?
                    .map(|val| deserialize::<Entry>(&val))
                    .transpose()?
                    .map(|entry| (entry.index, entry.term))
                    .ok_or_else(|| Error::Internal(format!("Entry {} not found", index)))?
            },
        };
        self.last_index = index;
        self.last_term = term;
        Ok(index)
    }
    
    pub fn save_term(&mut self, term: u64, voted_for: Option<&str>) -> Result<()> {
        self.store.set_metadata(&Key::TernVote.encode(), serialize(&(term , voted_for))?)
    }

    pub fn load_term(&self) -> Result<(u64, Option<String>)> {
        let (term, voted_for) = self.store
            .get_metadata(&Key::TernVote.encode())?
            .map(|val| deserialize(&val))
            .transpose()?
            .unwrap_or((0, None));
        Ok((term, voted_for))
    }


}

fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)

}

fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(bytes)?)
}
