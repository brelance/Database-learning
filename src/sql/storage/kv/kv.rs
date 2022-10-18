use std::{any::Any, borrow::Cow};
use super::coding::*;
use super::{Mvcc, Mode, mvcc, Row};
use crate::sql::storage::Column;
use crate::{error::{Error, Result}, sql::storage::{Catalog, Value, Table}};


pub struct Kv {
    kv: Mvcc,
}

impl Kv {
    fn new(mvcc: Mvcc) -> Self {
        Kv { kv: mvcc }
    }

    fn set_metadata(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.kv.set_metadata(key, value)
    }

    fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.kv.get_metadata(key)
    }
}

impl Kv {

    fn begin(&self, mode: Mode) -> Result<Transaction> {
        Ok(Transaction::new(self.kv.begin_with_mode(mode)?))
    }

    fn resume(&self, txn_id: u64) -> Result<Transaction> {
        Ok(Transaction::new(self.kv.resume(txn_id)?))
    }

}

pub struct Transaction {
    txn: mvcc::Transaction,
}

impl Transaction {
    fn new(txn: mvcc::Transaction) -> Self {
        Transaction { txn, }
    }

}



impl super::Transaction for Transaction {
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
        
    }

    fn create(&mut self, table: &str, row: Row) -> Result<()> {
        let table = self.must_read_table(table)?;
        table

        
    }

    fn delete(&mut self, table: &str, id: &[crate::sql::storage::Value]) -> Result<()> {
        
    }

    fn scan(&self, table: &str, columnL: &str) -> Result<IndexScan> {
        
    }

    fn scan_index(&self, table: &str, column: &str) -> Result<IndexScan> {
        
    }

    fn read(&self, table: &str, id: &crate::sql::storage::Value) -> Result<Option<Row>> {
        
    }

    fn read_index(&self, table: &str, column: &str, value: &crate::sql::storage::Value) -> Result<std::collections::HashSet<crate::sql::storage::Value>> {
        
    }


}

impl super::Catalog for Transaction {
    fn creat_table(&mut self, table: crate::sql::storage::Table) -> Result<()> {
        
    }
    
    fn delete_table(&mut self, table_name: &str) -> Result<()> {
        
    }

    fn read_table(&self, table: &str) -> Result<Option<crate::sql::storage::Table>> {
        
    }

    fn scan_tables(&self) -> Result<Tables> {
        
    }

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

