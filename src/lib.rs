#![warn(missing_docs)]

//! A library for iterating over a MongoDB replica set oplog.
//!
//! Given a MongoDB `Client` connected to a replica set, this crate allows you to iterate over an
//! `Oplog` as if it were a collection of statically typed `Operation`s.
//!
//! # Example
//!
//! At its most basic, an `Oplog` will yield _all_ operations in the oplog when iterated over:
//!
//! ```rust,no_run
//! # extern crate mongodb;
//! # extern crate oplog;
//! use mongodb::{Client, ThreadedClient};
//! use oplog::Oplog;
//!
//! # fn main() {
//! let client = Client::connect("localhost", 27017).expect("Failed to connect to MongoDB.");
//!
//! if let Ok(oplog) = Oplog::new(&client) {
//!     for operation in oplog {
//!         // Do something with operation...
//!     }
//! }
//! # }
//! ```
//!
//! Alternatively, an `Oplog` can be built with a filter via `OplogBuilder` to restrict the
//! operations yielded:
//!
//! ```rust,no_run
//! # #[macro_use]
//! # extern crate bson;
//! # extern crate mongodb;
//! # extern crate oplog;
//! use mongodb::{Client, ThreadedClient};
//! use oplog::OplogBuilder;
//!
//! # fn main() {
//! let client = Client::connect("localhost", 27017).expect("Failed to connect to MongoDB.");
//!
//! if let Ok(oplog) = OplogBuilder::new(&client).filter(Some(doc! { "op" => "i" })).build() {
//!     for insert in oplog {
//!         // Do something with insert operation...
//!     }
//! }
//! # }
//! ```

#[macro_use]
extern crate bson;
extern crate mongodb;
extern crate chrono;

use std::error;
use std::fmt;
use std::result;

pub use operation::Operation;
pub use oplog::{Oplog, OplogBuilder};

mod operation;
mod oplog;

/// A type alias for convenience so we can fix the error to our own `Error` type.
pub type Result<T> = result::Result<T, Error>;

/// Error enumerates the list of possible error conditions when tailing an oplog.
#[derive(Debug)]
pub enum Error {
    /// A database connectivity error raised by the MongoDB driver.
    Database(mongodb::Error),
    /// An error when converting a BSON document to an `Operation` and it has a missing field or
    /// unexpected type.
    MissingField(bson::ValueAccessError),
    /// An error when converting a BSON document to an `Operation` and it has an unsupported
    /// operation type.
    UnknownOperation(String),
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Database(ref err) => err.description(),
            Error::MissingField(ref err) => err.description(),
            Error::UnknownOperation(_) => "unknown operation type",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Database(ref err) => err.fmt(f),
            Error::MissingField(ref err) => err.fmt(f),
            Error::UnknownOperation(ref op) => write!(f, "Unknown operation type found: {}", op),
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
