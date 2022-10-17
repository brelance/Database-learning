
#[derive(Clone)]
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

#[derive(PartialEq, Eq)]
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