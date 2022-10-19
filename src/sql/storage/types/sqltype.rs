use std::borrow::Cow;
use std::hash::{Hash, Hasher};

use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl Value {
    pub fn datatype(&self) -> Option<Datatype> {
        match &self {
            Self::Boolean(_) => Some(Datatype::Boolean),
            Self::Integer(_) => Some(Datatype::Integer),
            Self::Float(_) => Some(Datatype::Float),
            Self::String(_) => Some(Datatype::String),
            _ => None 
        }
    }
}

#[allow(clippy::derive_hash_xor_eq)]
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.datatype().hash(state);
        match self {
            Value::Null => self.hash(state),
            Value::Boolean(v) => v.hash(state),
            Value::Integer(v) => v.hash(state),
            Value::Float(v) => v.to_be_bytes().hash(state),
            Value::String(v) => v.hash(state),
        }
    }
}

impl std::cmp::Eq for Value {
}


impl<'a> From<Value> for Cow<'a, Value> {
    fn from(v: Value) -> Self {
        Cow::Owned(v)
    }
}

impl<'a> From<&'a Value> for Cow<'a, Value> {
    fn from(v: &'a Value) -> Self {
        Cow::Borrowed(v)
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            match self {
                Self::Null => "NULL".to_string(),
                Self::Boolean(b) if *b => "TRUE".to_string(),
                Self::Boolean(_) => "FALSE".to_string(),
                Self::Integer(i) => i.to_string(),
                Self::Float(f) => f.to_string(),
                Self::String(s) => s.clone(),
            }
            .as_ref(),
        )  
    }
}



#[derive(PartialEq, Eq, Hash, Serialize, Deserialize, Clone, Debug)]
pub enum Datatype{
    Boolean,
    Integer,
    String,
    Float,
}

impl std::fmt::Display for Datatype {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(match self {
            Self::Boolean => "BOOLEAN",
            Self::Integer => "INTEGER",
            Self::Float => "FLOAT",
            Self::String => "STRING",
        })
    }
}