#[macro_use(bson, doc)]
extern crate bson;
extern crate mongodb;
extern crate chrono;

use std::error;
use std::fmt;
use std::result;

pub use operation::Operation;
pub use oplog::Oplog;

mod operation;
mod oplog;

/// Result is a type alias which fixes the type of the error to the `Error` type defined below.
pub type Result<T> = result::Result<T, Error>;

/// Error enumerates the list of possible error conditions when tailing an oplog.
#[derive(Debug)]
pub enum Error {
    MissingField(bson::ValueAccessError),
    Database(mongodb::Error),
    UnknownOperation(String),
    InvalidOperation,
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::MissingField(ref err) => err.description(),
            Error::Database(ref err) => err.description(),
            Error::UnknownOperation(_) => "unknown operation type",
            Error::InvalidOperation => "invalid operation",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::MissingField(ref err) => err.fmt(f),
            Error::Database(ref err) => err.fmt(f),
            Error::UnknownOperation(ref op) => write!(f, "Unknown operation type found: {}", op),
            Error::InvalidOperation => write!(f, "Invalid operation"),
        }
    }
}

impl From<bson::ValueAccessError> for Error {
    fn from(original: bson::ValueAccessError) -> Error {
        Error::MissingField(original)
    }
}

impl From<mongodb::Error> for Error {
    fn from(original: mongodb::Error) -> Error {
        Error::Database(original)
    }
}
