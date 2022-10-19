use super::{Mode, Value, Table, Expression};
use crate::error::{Result, Error};
use std::collections::HashSet;


pub trait Catalog {
    fn creat_table(&mut self, table: Table) -> Result<()>;
    
    fn delete_table(&mut self, table_name: &str) -> Result<()>;

    fn read_table(&self, table: &str) -> Result<Option<Table>>;

    fn scan_tables(&self) -> Result<Tables>;

    fn must_read_table(&self, table_name: &str) -> Result<Table> {
        self.read_table(table_name)?.ok_or_else(|| Error::Value(format!("Table {} does not exist", table_name)))
    }

    
    fn table_references(&self, table_name: &str, with_self: bool) -> Result<Vec<(String, Vec<String>)>> {
        Ok(self.scan_tables()?
            .filter(|t| with_self || t.name != table_name)
            .map(|t| {
                (t.name,
                t.columns
                    .iter()
                    .filter(|col| col.reference.as_deref() == Some(table_name))
                    .map(|col| col.name.clone()).collect::<Vec<_>>(),
                )
            })
            .filter(|(_, cs)| !cs.is_empty())
            .collect()
        )
    }
}

pub trait Transaction: Catalog {
    fn id(&self) -> u64;

    fn mode(&self) -> Mode;

    fn commit(self) -> Result<()>;

    fn update(&mut self, table: &str, id: &Value, row: Row) -> Result<()>;

    fn rollback(self) -> Result<()>;

    fn create(&mut self, table: &str, row: Row) -> Result<()>;

    fn delete(&mut self, table: &str, id: &Value) -> Result<()>;

    fn read(&self, table: &str, id: &Value) -> Result<Option<Row>>;

    fn read_index(&self, table: &str, column: &str, value: &Value) -> Result<HashSet<Value>>;

    fn scan(&self, table: &str, filter: Option<Expression>) -> Result<KScan>;

    fn scan_index(&self, table: &str, column: &str) -> Result<IndexScan>;

}

pub type Row = Vec<Value>;

pub type Tables = Box<dyn DoubleEndedIterator<Item = Table> + Send>;

pub type KScan = Box<dyn DoubleEndedIterator<Item = Result<Row>> + Send>;

pub type IndexScan = Box<dyn DoubleEndedIterator<Item = Result<(Value, HashSet<Value>)>> + Send>;


