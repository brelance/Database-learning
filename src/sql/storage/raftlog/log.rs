use std::sync::{Mutex, MutexGuard};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{File,create_dir_all, OpenOptions,};
use std::path::Path;
use std::io::{BufReader, Read, Seek, SeekFrom, BufWriter, Write};
use crate::raft::log::Log;

use super::{Error, Result, Range};
use std::ops::Bound;
use std::cmp::{min, max};

pub struct LogStore {
    file: Mutex<File>,
    index: BTreeMap<u64, (u64, u32)>,
    uncommited: VecDeque<Vec<u8>>,
    metadata: HashMap<Vec<u8>, Vec<u8>>,
    meta_file: File,
    sync: bool,
}

impl LogStore {
    fn new(dir: &Path, sync: bool) -> Result<Self> {
        create_dir_all(dir)?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dir.join("raft-log"))?;
        
        let meatadata = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dir.join("raft-metadata"))?;
        
        Ok(LogStore {
            file: Mutex::new(file),
            index: Self::load_index(&file)?,
            uncommited: VecDeque::new(),
            metadata: HashMap::new(),
            meta_file: meatadata,
            sync,
        })
    }

    fn load_index(file: &File) -> Result<BTreeMap<u64, (u64, u32)>> {
        let filesize = file.metadata()?.len();
        let mut bufreader = BufReader::new(file);
        let mut sizebuf = [0; 4];
        let mut pos = 0;
        let mut i = 1;
        let mut index = BTreeMap::new();
        while pos < filesize {
            bufreader.read_exact(&mut sizebuf)?;
            pos += 4;
            let size = u32::from_be_bytes(sizebuf);
            index.insert(i, (pos, size));
            let mut buf = vec![0; size as usize];
            //todo 
            // bufreader.seek(SeekFrom::Current(size as i64));
            bufreader.read_exact(&mut buf)?;
        }
        Ok(index)
    }

    fn load_meatadata(file: &File) -> Result<HashMap<Vec<u8>, Vec<u8>>> {
        match bincode::deserialize_from(file) {
            Ok(metadata) => Ok(metadata),
            Err(err) => {
                if let bincode::ErrorKind::Io(err) = &*err {
                    if err.kind() == std::io::ErrorKind::UnexpectedEof {
                        return Ok(HashMap::new());
                    }
                }
                Err(err.into())
            }
        }
    }
}

impl super::Store for LogStore {
    fn append(&mut self, entry: Vec<u8>) -> Result<u64> {
        self.uncommited.push_back(entry);
        Ok(self.len())
    }

    fn commit(&mut self, index: u64) -> Result<()> {
        if index > self.len() {
            return Err(Error::Internal(format!("Cannot commit non-existant index {}", index)));
        }
        if index < self.index.len() as u64 {
            return Err(Error::Internal(format!(
                "Cannot commit below current commited index {}",
                self.index.len() as u64
            )))
        }
        if index == self.index.len() as u64 {
            return Ok(());
        }
        let mut file = self.file.lock()?;
        let mut pos = file.seek(SeekFrom::End(0))?;
        let mut bufwrite = BufWriter::new(&mut *file);
        for i in (self.index.len() + 1) as u64..=index {
            let entry = self.uncommited
                .pop_front()
                .ok_or_else(|| Error::Internal("Unexpected end of uncommited entries".to_string()))?;
            pos += 4;
            let size = entry.len() as u32;
            self.index.insert(i, (pos, size));
            bufwrite.write_all(&size.to_be_bytes())?;
            bufwrite.write_all(&entry)?;
            pos += size as u64;
        }
        bufwrite.flush()?;
        drop(bufwrite);
        if self.sync {
            file.sync_data()?;
        }
        Ok(())

    }

    fn len(&self) -> u64 {
        self.len() + self.uncommited.len() as u64
    }
 
    fn get(&self, index: u64) -> Result<Option<Vec<u8>>> {
        match index {
            0 => Ok(None),
            i if index < self.index.len() as u64 => {
                let (pos, size) = self.index.get(&index).copied().ok_or_else(
                    || Error::Internal(format!("Indexed position not found for entry {}", i)) 
                )?;
                let file = self.file.lock()?;
                let mut entry = vec![0; size as usize];
                file.seek(SeekFrom::Start(pos))?;
                file.read_exact(&mut entry)?;
                Ok(Some(entry))

            },
            i => Ok(self.uncommited.get(index as usize - self.index.len() - 1).cloned())
        }
    }

    fn scan(&self, range: Range) -> Scan {
        let start = match range.start {
            Bound::Included(0) => 1,
            Bound::Included(n) => n,
            Bound::Excluded(n) => n + 1,
            Bound::Unbounded => 1,
        };
        let end = match range.end {
            Bound::Included(n) => n,
            Bound::Excluded(0) => 0,
            Bound::Excluded(n) => n - 1,
            Bound::Unbounded => self.len(),
        };

        let mut scan: Scan = Box::new(std::iter::empty());
        if start > end {
            return scan;
        }
        
        if let Some((offset, _)) = self.index.get(&start) {
            let mut file = self.file.lock().unwrap();
            file.seek(SeekFrom::Start(*offset - 4)).unwrap(); // seek to length prefix
            let mut bufreader = BufReader::new(MutexReader(file)); // FIXME Avoid MutexReader
            scan =
                Box::new(scan.chain(self.index.range(start..=end).map(move |(_, (_, size))| {
                    let mut sizebuf = vec![0; 4];
                    bufreader.read_exact(&mut sizebuf)?;
                    let mut entry = vec![0; *size as usize];
                    bufreader.read_exact(&mut entry)?;
                    Ok(entry)
                })));
        }
        if end > self.index.len() as u64 {
            scan = Box::new(
                scan.chain(
                    self.uncommited
                        .iter()
                        .skip(start as usize - min(start as usize, self.index.len() + 1))
                        .take(end as usize - max(start as usize, self.index.len()) + 1)
                        .cloned()
                        .map(Ok),
                ),
            )
        }
        scan
    }

    fn size(&self) -> u64 {
        self.index.iter().next_back().map(|(_, (pos, size))| *pos + *size as u64).unwrap()
    }

    fn commited(&self) -> u64 {
        self.index.len() as u64
    }

    fn truncate(&mut self, index: u64) -> Result<u64> {
        if index < self.index.len() as u64 {
            return Err(Error::Internal(format!(
                "Cannot truncate below commited index {}", 
                self.index.len() as u64
            )));
        }
        self.uncommited.truncate(index as usize- self.index.len());
        Ok(self.len())
    }

    fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.metadata.get(key).cloned())
    }

    fn set_metadata(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.metadata.insert(key.to_vec(), value);
        self.meta_file.set_len(0)?;
        self.meta_file.seek(SeekFrom::Start(0))?;
        bincode::serialize_into(&mut self.meta_file, &self.metadata)?;
        if self.sync {
            self.meta_file.sync_data()?;
        }
        Ok(())
    }


}

impl<'a> Read for MutexReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buf)
    }
}

impl Drop for LogStore {
    fn drop(&mut self) {
        self.meta_file.sync_all().ok();
        self.file.lock().map(|f| f.sync_all()).ok();
    }
}

struct MutexReader<'a>(MutexGuard<'a, File>);


pub type Scan<'a> = Box<dyn Iterator<Item = Result<Vec<u8>>> + 'a>;



