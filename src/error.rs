use std::fmt::Display;


pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Abort,
    Config(String),
    Internal(String),
    Parse(String),
    ReadOnly,
    Serialization,
    Value(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Config(s) | Error::Internal(s) 
            | Error::Parse(s) | Error::Value(s) => {
                write!(f, "{}", s)
            },

            Error::Abort => { write!(f, "Operation aborted") },
            Error::ReadOnly => { write!(f, "Read-Only transaction") },
            Error::Serialization => { write!(f, "serialization failure, retry transaction") },
        }
    }
}


impl std::error::Error for Error {}


impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        Error::Parse(err.to_string()) 
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Error::Internal(err.to_string())
    }
}

impl From<std::array::TryFromSliceError> for Error {
    fn from(err: std::array::TryFromSliceError) -> Self {
        Error::Internal(err.to_string())
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        Error::Internal(err.to_string())
    }
}