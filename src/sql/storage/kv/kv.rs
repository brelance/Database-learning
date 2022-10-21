use std::collections::HashSet;
use std::{borrow::Cow};
use super::coding::*;
use super::{Mvcc, Mode, mvcc, Row};
use serde::{Deserialize, Serialize};
use crate::sql::storage::{Expression, engine::Transaction};
use crate::{error::{Error, Result}, sql::storage::{Catalog, Value}};


pub struct Kv {
    pub kv: Mvcc,
}

impl Kv {
    pub fn new(mvcc: Mvcc) -> Self {
        Kv { kv: mvcc }
    }

    pub fn set_metadata(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.kv.set_metadata(key, value)
    }

    pub fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.kv.get_metadata(key)
    }
}

impl Kv {

    pub fn begin(&self, mode: Mode) -> Result<Txn> {
        Ok(Txn::new(self.kv.begin_with_mode(mode)?))
    }

    pub fn resume(&self, txn_id: u64) -> Result<Txn> {
        Ok(Txn::new(self.kv.resume(txn_id)?))
    }

}

pub struct Txn {
    txn: mvcc::Transaction,
}

impl Txn {
    fn new(txn: mvcc::Transaction) -> Self {
        Txn { txn, }
    }

    fn index_load(&self, table_name: &str, column: &str, value: &Value) -> Result<HashSet<Value>> {
        Ok(self.txn
            .get(&Key::Index(table_name.into(), column.into(), Some(value.into())).encode())?
            .map(|set| deserialize(&set))
            .transpose()?
            .unwrap_or_else(HashSet::new))
    }

    fn index_store(&mut self, table_name: &str, column: &str, value: &Value, index: HashSet<Value>) -> Result<()> {
        let key = Key::Index(table_name.into(), column.into(), Some(value.into())).encode();
        if index.is_empty() {
            self.txn.delete(&key)
        } else {
            self.txn.set(&key, serialize(&index)?)
        }
    }
}

impl Transaction for Txn {
    fn id(&self) -> u64 {
        self.txn.id()
    }

    fn mode(&self) -> Mode {
        self.txn.mode()
    }

    fn commit(self) -> Result<()> {
        self.txn.commit()
    }

    fn rollback(self) -> Result<()> {
        self.txn.rollback()  
    }

    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<()> {
        let table = self.must_read_table(table)?;
        if id != &table.get_row_key(&row)? {
            self.delete(&table.name, id)?;
            self.create(&table.name, row)?;
            return Ok(());
        }

        let indexes: Vec<_> = table.columns.iter().enumerate().filter(|(_, c)| c.index).collect();
        if !indexes.is_empty() {
            let old = self.read(&table.name, id)?.unwrap();
            for (i, column) in indexes {
                if old[i] == row[i] {
                    continue;
                }
                let mut index = self.index_load(&table.name, &column.name, &old[i])?;
                index.remove(id);
                self.index_store(&table.name, &column.name, &old[i], index)?;

                let mut index = self.index_load(&table.name, &column.name, &row[i])?;
                index.insert(id.clone());
                self.index_store(&table.name, &column.name, &row[i], index)?;
            }
        }

        table.validate_row(&row, self)?;
        self.txn.set(&Key::Row(table.name.into(), Some(id.into())).encode(), serialize(&row)?)  
    }

    fn create(&mut self, table_name: &str, row: Row) -> Result<()> {
        let table = self.must_read_table(table_name)?;
        table.validate_row(&row, self)?;
        let id = table.get_row_key(&row)?;
        if self.read(table_name, &id)?.is_some() {
            return Err(Error::Value(format!(
                "Primary key {} already exists for table {}",
                id, table_name
            )));
        }

        self.txn.set(
            &Key::Row(Cow::Borrowed(&table_name), Some(Cow::Borrowed(&id))).encode(), 
            serialize(&row)?
        )?;

        for (i, column) in table.columns.iter().enumerate().filter(|(_, col)| col.index ) {
            let mut index =  self.index_load(table_name, &column.name, &row[i])?;
            index.insert(id.clone());
            self.index_store(table_name, &column.name, &row[i], index)?;
        }
        Ok(())   
    }

    fn delete(&mut self, table: &str, id: &Value) -> Result<()> {
        let table = self.must_read_table(table)?;
        for (t, cs) in self.table_references(&table.name, true)? {
            let t = self.must_read_table(&t)?;
            let cs = cs
                .into_iter()
                .map(|c| Ok((t.get_column_index(&c)?, c)))
                .collect::<Result<Vec<_>>>()?;
            let mut scan = self.scan(&t.name, None)?;
            while let Some(row) = scan.next().transpose()? {
                for (i, c) in &cs {
                    if &row[*i] == id && (table.name != t.name || id != &table.get_row_key(&row)?) {
                        return Err(Error::Value(format!(
                            "Primary key {} is referenced by table {} column {}",
                            id, t.name, c
                        )));
                    }
                }
            }
        }

            let indexes: Vec<_> = table.columns.iter().enumerate().filter(|(_, c)| c.index).collect();
            if !indexes.is_empty() {
                if let Some(row) = self.read(&table.name, id)? {
                    for (i, column) in indexes {
                        let mut index = self.index_load(&table.name, &column.name, &row[i])?;
                        index.remove(id);
                        self.index_store(&table.name, &column.name, &row[i], index)?;
                    }
                }
            }

            self.txn.delete(&Key::Row(table.name.into(), Some(id.into())).encode())
        }

        
    fn read(&self, table: &str, id: &crate::sql::storage::Value) -> Result<Option<Row>> {
        self.txn
            .get(&Key::Row(table.into(), Some(id.into())).encode())?
            .map(|val| deserialize(&val))
            .transpose()

    }

    fn read_index(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>> {
        if !self.must_read_table(table)?.get_column(column)?.index {
            return Err(Error::Value(format!("No index on {}.{}", table, column)));
        }
        self.index_load(table, column, value)
    }

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<super::KScan> {
        let table = self.must_read_table(table)?;
        Ok(Box::new(
            self.txn
                .scan_prefix(&Key::Row((&table.name).into(), None).encode())?
                .map(|r| r.and_then(|(_, val)| deserialize(&val)))
                .filter_map(move |r| match r {
                    Ok(row) => match &filter {
                        Some(filter) => match filter.evaluate(Some(&row)) {
                            Ok(Value::Boolean(b)) if b => Some(Ok(row)),
                            Ok(Value::Boolean(_)) | Ok(Value::Null) => None,
                            Ok(v) => Some(Err(Error::Value(format!(
                                "Filter returned {}, expected boolean",
                                v
                            )))),
                            Err(err) => Some(Err(err)),
                        },
                        None => Some(Ok(row)),
                    },
                    err => Some(err),
                }),
        ))
    }

    fn scan_index(&self, table: &str, column: &str) -> Result<super::IndexScan> {
        let table = self.must_read_table(table)?;
        let col = table.get_column(column)?;
        Ok(Box::new(self.txn    
            .scan_prefix(
                &Key::Index((&table.name).into(), (&col.name).into(), None).encode()
            )?
            .map(|r| -> Result<(Value, HashSet<Value>)> {
                let (k, v) = r?;
                let value = match Key::decode(&k)? {
                    Key::Index(_, _, Some(pk)) => pk.into_owned(),
                    _ =>return Err(Error::Internal("Invalid index key".into())),
                };
                Ok((value, deserialize(&v)?))
            })))
    }
        
}


impl super::Catalog for Txn {
    fn create_table(&mut self, table: crate::sql::storage::Table) -> Result<()> {
        if self.read_table(&table.name)?.is_some() {
            return Err(Error::Value(format!("Table {} already exists", table.name)));
        }
        table.validate(self)?;
        self.txn.set(&Key::Table(Some((&table.name).into())).encode(), serialize(&table)?)
    }
    
    fn delete_table(&mut self, table_name: &str) -> Result<()> {
        let table = self.must_read_table(table_name)?;
        if let Some((t, cs)) = self.table_references(&table.name, false)?.first() {
            return Err(Error::Value(format!(
                "Table {} is referenced by table {} column {}",
                table.name, t, cs[0]
            )));
        }
        let mut scan = self.scan(&table.name, None)?;
        while let Some(row) = scan.next().transpose()? {
            self.delete(&table.name, &table.get_row_key(&row)?)?
        }
        self.txn.delete(&Key::Table(Some(table.name.into())).encode())
    }

    fn read_table(&self, table: &str) -> Result<Option<crate::sql::storage::Table>> {
        self.txn.get(&Key::Table(Some(table.into())).encode())?.map(|v| deserialize(&v)).transpose()
    }

    fn scan_tables(&self) -> Result<super::Tables> {
        Ok(Box::new(
            self.txn
                .scan_prefix(&Key::Table(None).encode())?
                .map(|r| r.and_then(|(_, v)| deserialize(&v)))
                .collect::<Result<Vec<_>>>()?
                .into_iter(),
        ))
    }

}

fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(bytes)?)
}

enum Key<'a> {
    Table(Option<Cow<'a, str>>),
    Index(Cow<'a, str>, Cow<'a, str>, Option<Cow<'a, Value>>),
    Row(Cow<'a, str>, Option<Cow<'a, Value>>),

}

impl<'a> Key<'a> {
    fn encode(self) -> Vec<u8> {
        match &self {
            Self::Table(None) => vec![0x01],
            Self::Table(Some(name)) => [&[0x01][..], &encode_string(name)].concat(),
            Self::Index(table, column, None) 
                => [&[0x02][..], &encode_string(table), &encode_string(column)].concat(),
            Self::Index(table, column, Some(value)) 
                => [&[0x02][..], &encode_string(table), &encode_string(column), &encode_value(value.into_owned())].concat(),
            Self::Row(table, None) => [&[0x03][..], &encode_string(table)].concat(),
            Self::Row(table, Some(pk)) => {
                    [&[0x03][..], &encode_string(&table), &encode_value(pk.into_owned())].concat()
                } 
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let bytes = &mut bytes;
        let key = match take_byte(bytes)? {
            0x01 => Self::Table(Some(take_string(bytes)?.into())),
            0x02 => Self::Index(
                take_string(bytes)?.into(),
                take_string(bytes)?.into(),
                Some(take_value(bytes)?.into()),
            ),
            0x03 => Self::Row(take_string(bytes)?.into(), Some(take_value(bytes)?.into())),
            b => return Err(Error::Internal(format!("Unknown SQL key prefix {:x?}", b))),
        };
        if !bytes.is_empty() {
            return Err(Error::Internal("Unexpected data remaining at end of key".into()));
        }
        Ok(key)
    }
}

