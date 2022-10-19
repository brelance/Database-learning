use std::fmt::format;

use super::Value;
use super::Datatype;
use super::engine::Row;
use crate::error::{Error, Result};
use super::Transaction;



pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Table { name, columns, }
    }

    pub fn get_column(&self, col_name: &str) -> Result<&Column> {
        self.columns
            .iter()
            .find(|col| col.name == col_name )
            .ok_or_else(|| Error::Value(format!("cannot find column {} in table {}", col_name, self.name)))
    }

    pub fn get_column_index(&self, col_name: &str) -> Result<usize> {
        self.columns
            .iter()
            .position(|col| &col.name == col_name)
            .ok_or_else(|| Error::Value(format!("cannot find column {} in table {}", col_name, self.name)))
    }

    pub fn get_primary_key(&self) -> Result<&Column> {
        self.columns
            .iter()
            .find(|col| col.primary_key)
            .ok_or_else(|| Error::Value(format!("Primary key not found in table {}", self.name)))
    }

    pub fn get_row_key(&self, row: &[Value]) -> Result<Value> {
        row.get(
            self.columns
                .iter()
                .position(|col| col.primary_key)
                .ok_or_else(|| Error::Value(format!("Primary key not found in table {}", self.name)))?
        )
        .cloned()
        .ok_or_else(|| Error::Value(format!("Primary key not found in table {}", self.name)))
    }

    pub fn validate(&self, txn: &mut dyn Transaction) -> Result<()> {
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

    pub fn validate_row(&self, row: &[Value], txn: &mut dyn Transaction) -> Result<()>{
        if row.len() != self.columns.len() {
            return Err(Error::Value("error row".to_string()));
        }
        
        let pk = self.get_row_key(row)?;
        for (val, column) in row.iter().zip(self.columns.iter()) {
            column.validate_val(self, val, &pk, txn)?;
        }
        Ok(())
    }
}

pub struct Column {
    pub name: String,

    pub datatype: Datatype,

    pub primary_key: bool,

    pub nullalbe: bool,

    pub default: Option<Value>,

    pub unique: bool,

    pub reference: Option<String>,

    pub index: bool,
}

impl Column {

    fn validate(&self, table: &Table, txn: &mut dyn Transaction) -> Result<()> {
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

    fn validate_val(&self, table: &Table, val: &Value, pk: &Value, txn: &mut dyn Transaction) ->Result<()> {
        match val.datatype() {
            None if self.nullalbe => Ok(()),
            None => Err(Error::Value(format!("NULL value not allowed for column {}", self.name))),
            Some(dtype) if dtype != self.datatype => Err(Error::Value(format!(
                "Invalid datatype {} for {} column {}",
                dtype, self.datatype, self.name
            ))),
            _ => Ok(()),
        }?;

        match val {
            Value::String(s) if s.len() > 1024 => {
                Err(Error::Value("Strings cannot be more than 1024 bytes".into()))
            }
            _ => Ok(()),
        }?;

        if let Some(target) = self.reference {
            match val {
                Value::Null => Ok(()),
                Value::Float(f) if f.is_nan() => Ok(()),
                v if &target == &table.name && v == pk =>Ok(()),
                v if txn.read(target.as_str(), v)?.is_none() => Err(Error::Value(format!(
                    "Referenced primary key {} in table {} does not exist",
                    v, target,
                ))),
                _ => Ok(()),
            }?;
        }

        if self.unique && !self.primary_key && val != &Value::Null {
            let index = table.get_column_index(&self.name)?;
            let mut scan = txn.scan(&table.name, None)?;
            while let Some(row) = scan.next().transpose()? {
                if row.get(index).unwrap_or(&Value::Null) == val
                    && &table.get_row_key(&row)? != pk
                {
                    return Err(Error::Value(format!(
                        "Unique value {} already exists for column {}",
                        val, self.name
                    )));
                }
            }
        }
        Ok(())
    }
}
