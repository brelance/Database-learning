use std::fmt::format;

use super::Value;
use super::Datatype;
use crate::error::{Error, Result};
use super::Transcation;



pub struct Table {
    name: String,
    columns: Vec<Column>,
}

impl Table {
    fn new(name: String, columns: Vec<Column>) -> Self {
        Table { name, columns, }
    }

    fn get_column(&self, col_name: String) -> Result<&Column> {
        self.columns
            .iter()
            .find(|col| col.name == col_name )
            .ok_or_else(|| Error::Value(format!("cannot find column {} in table {}", col_name, self.name)))
    }

    fn get_column_index(&self, col_name: String) -> Result<usize> {
        self.columns
            .iter()
            .position(|col| col.name == col_name)
            .ok_or_else(|| Error::Value(format!("cannot find column {} in table {}", col_name, self.name)))
    }

    fn get_primary_key(&self) -> Result<&Column> {
        self.columns
            .iter()
            .find(|col| col.primary_key)
            .ok_or_else(|| Error::Value(format!("Primary key not found in table {}", self.name)))
    }

    fn get_row_key(&self, row: &[Value]) -> Result<Value> {
        row.get(
            self.columns
                .iter()
                .position(|col| col.primary_key)
                .ok_or_else(|| Error::Value(format!("Primary key not found in table {}", self.name)))?
        )
        .cloned()
        .ok_or_else(|| Error::Value(format!("Primary key not found in table {}", self.name)))
    }

    fn validate(&self, txn: &mut dyn Transcation) -> Result<()> {
        if self.columns.is_empty() {
            return Err(Error::Value(format!("Table {} has no columns", self.name)));
        }

        match self.columns.iter().filter(|col| col.primary_key ).count() {
            1 => {},
            0 => return Err(Error::Value(format!("No primary key in table {}", self.name))),
            _ => return Err(Error::Value(format!("Multiple primary keys in table {}", self.name))),
        }

        for col in &self.columns {
            col.validate(self, txn)?
        }
        Ok(())
    }

    fn validate_row() {}
}

pub struct Column {
    name: String,

    datatype: Datatype,

    primary_key: bool,

    nullalbe: bool,

    default: Option<Value>,

    unique: bool,

    reference: Option<String>,

    index: bool,
}

impl Column {

    fn validate(&self, table: &Table, txn: &mut dyn Transcation) -> Result<()> {
        if self.nullalbe && self.primary_key {
            return Err(Error::Value(format!("Primary key {} cannot be nullable", self.name)));
        }

        if self.primary_key && !self.unique {
            return Err(Error::Value(format!("Primary key {} must be unique", self.name)));
        }

        if let Some(value) = &self.default {
            if let Some(dtype) = value.datatype() {
                if dtype != self.datatype {
                    return Err(Error::Value(format!(
                        "Default value for column {} has datatype {}, must be {}",
                        self.name, dtype, self.datatype
                    )));
                }
            } else if !self.nullalbe {
                return Err(Error::Value(format!(
                    "Can't use NULL as default value for non-nullable column {}",
                    self.name
                )));
            } 
        } else if self.nullalbe {
            return Err(Error::Value(format!(
                "Nullable column {} must have a default value",
                self.name
            )));
        }

        if let Some(reference) = self.reference {
            let target = if reference == table.name {
                table
            } else if let Some(reftab) = txn.read_table(&reference)? {
                &reftab
            } else {
                return Err(Error::Value(format!(
                    "Table {} referenced by column {} does not exist",
                    reference, self.name
                )));
            };

            if self.datatype != target.get_primary_key()?.datatype {
                return Err(Error::Value(format!(
                    "Can't reference {} primary key of table {} from {} column {}",
                    target.get_primary_key()?.datatype,
                    target.name,
                    self.datatype,
                    self.name
                )));
            }
        }

        Ok(())

    }
}
