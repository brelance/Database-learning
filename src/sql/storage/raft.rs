use super::{engine::Transaction, Value, Row, Table, Expression};
use serde::{Serialize, Deserialize, de::value};
use serde_derive::{Deserialize, Serialize};
use crate::error::{Error, Result};
use super::{Mode, Client, Tables};

#[derive(Serialize, Deserialize)]
enum Mutation {
    Begin(Mode),
    Commit(u64),
    Rollback(u64),
    Create{txn_id: u64, table: String, row: Row},
    Delete{txn_id: u64, table: String, id: Value},
    Update{txn_id: u64, table: String, id: Value, row: Row},
    CreateTable {txn_id: u64, schema: Table},
    DeleteTable {txn_id: u64, table: String},
}

#[derive(Serialize, Deserialize)]
enum Query {
    Status,
    Resume(u64),
    Read { txn_id: u64, table: String, id: Value},
    ReadIndex { txn_id: u64, table: String, column: String, value: Value},
    Scan {txn_id: u64, table: String, filter: Option<Expression> },
    ScanIndex {txn_id: u64, table: String, column: String, },
    ScanTables { txn_id: u64 },
    ReadTable { txn_id: u64, table: String },
}

pub struct RaftTxn {
    client: Client,
    id: u64,
    mode: Mode,
}

impl RaftTxn {
    fn begin(client: Client, mode: Mode) -> Result<RaftTxn> {
        let id = deserialize(&futures::executor::block_on(
            client.mutate(serialize(&Mutation::Begin(mode))?)
        )?)?;
        Ok( RaftTxn {client, id, mode} )
    }

    fn resume(client: Client, id: u64) -> Result<Self> {
        let (id, mode) = deserialize(&futures::executor::block_on(
            client.mutate(serialize(&Query::Resume(id))?)
        )?)?;
        Ok(RaftTxn {client, id, mode} )
    }

    fn mutate(&self, mutation: Mutation) -> Result<Vec<u8>> {
        futures::executor::block_on(self.client.mutate(serialize(&mutation)?))
    }

    fn query(&self, query: Query) -> Result<Vec<u8>> {
        futures::executor::block_on(self.client.query(serialize(&query)?))
    }

}

impl super::Transaction for RaftTxn {
    fn id(&self) -> u64 {
        self.id
    }

    fn mode(&self) -> Mode {
        self.mode
    }

    fn commit(self) -> Result<()> {
        deserialize(&self.mutate(Mutation::Commit(self.id))?)
    }

    fn create(&mut self, table: &str, row: Row) -> Result<()> {
        deserialize(&self.mutate(
            Mutation::Create { txn_id: self.id, table: table.to_string(), row, }
        )?)
    }

    fn delete(&mut self, table: &str, id: &Value) -> Result<()> {
        deserialize(&self.mutate(
            Mutation::Delete { txn_id: self.id, table: table.to_string(), id: id.clone(), }
        )?)
    }

    fn read(&self, table: &str, id: &Value) -> Result<Option<Row>> {
        deserialize(&self.query(
            Query::Read { txn_id: self.id, table: table.to_string(), id: id.clone() }
        )?)
    }

    fn read_index(&self, table: &str, column: &str, value: &Value) -> Result<std::collections::HashSet<Value>> {
        deserialize(&self.query(
            Query::ReadIndex { txn_id: self.id, table: table.to_string(), column: column.to_string(), value: value.clone() }
        )?)
    }

    fn rollback(self) -> Result<()> {
        deserialize(&self.mutate(
            Mutation::Rollback(self.id)
        )?)
    }

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<super::engine::KScan> {
        Ok(Box::new(
            deserialize::<Vec<_>>(&self.query(Query::Scan { txn_id: self.id, table: table.to_string(), filter, }
        )?)?
        .into_iter()
        .map(Ok),
        ))
    }

    fn scan_index(&self, table: &str, column: &str) -> Result<super::IndexScan> {
        Ok(
            Box::new(
                deserialize::<Vec<_>>(&self.query(
                    Query::ScanIndex { txn_id: self.id, table: table.to_string(), column: column.to_string() }
                    )?)?
                    .into_iter()
                    .map(Ok)
            ))
    }

    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<()> {
        deserialize(&self.mutate(Mutation::Update {
            txn_id: self.id,
            table: table.to_string(),
            id: id.clone(),
            row,
        })?)
    }

}

impl super::Catalog for RaftTxn {
    fn create_table(&mut self, table: Table) -> Result<()> {
        deserialize(&self.mutate(Mutation::CreateTable { txn_id: self.id, schema: table })?)
    }

    fn delete_table(&mut self, table: &str) -> Result<()> {
        deserialize(
            &self.mutate(Mutation::DeleteTable { txn_id: self.id, table: table.to_string() })?,
        )
    }

    fn read_table(&self, table: &str) -> Result<Option<Table>> {
        deserialize(
            &self.query(Query::ReadTable { txn_id: self.id, table: table.to_string() })?,
        )
    }

    fn scan_tables(&self) -> Result<Tables> {
        Ok(Box::new(
            deserialize::<Vec<_>>(&self.query(Query::ScanTables { txn_id: self.id })?)?
                .into_iter(),
        ))
    }
}

fn serialize<V: Serialize>(value: &V) -> Result<Vec<u8>> {
    Ok(bincode::serialize(value)?)
}

fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a Vec<u8>) -> Result<V> {
    Ok(bincode::deserialize(bytes)?)
}


