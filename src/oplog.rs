//! The oplog module is responsible for building an iterator over a MongoDB replica set oplog with
//! any optional filtering criteria applied.

use bson::Document;
use mongodb::coll::options::{FindOptions, CursorType};
use mongodb::cursor::Cursor;
use mongodb::db::ThreadedDatabase;
use mongodb::{Client, ThreadedClient};

use {Operation, Result};

/// Oplog represents a MongoDB replica set oplog.
///
/// It implements the `Iterator` trait so can be iterated over, yielding successive `Operation`
/// enums as they are read from the server. This will effectively iterate forever as it will await
/// new operations.
pub struct Oplog {
    /// The internal MongoDB cursor for the current position in the oplog.
    cursor: Cursor,
}

impl Iterator for Oplog {
    type Item = Operation;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.cursor.next() {
                Some(Ok(document)) => return Operation::new(document).ok(),
                _ => continue,
            }
        }
    }
}

impl Oplog {
    /// Returns a new `Oplog` for the given MongoDB client with the default options.
    pub fn new(client: &Client) -> Result<Oplog> {
        OplogBuilder::new(client).build()
    }
}

/// A builder for an `Oplog`.
///
/// This builder enables configuring a filter on the oplog so that only documents matching a given
/// criteria are returned (e.g. to set a start time or filter out unwanted operation types).
///
/// The lifetime `'a` refers to the lifetime of the MongoDB client.
#[derive(Clone)]
pub struct OplogBuilder<'a> {
    client: &'a Client,
    filter: Option<Document>,
}

impl<'a> OplogBuilder<'a> {
    /// Create a new builder for the given MongoDB client.
    ///
    /// The oplog is not built until `build` is called.
    pub fn new(client: &'a Client) -> OplogBuilder<'a> {
        OplogBuilder {
            client: client,
            filter: None,
        }
    }

    /// Executes the query and builds the `Oplog`.
    pub fn build(&self) -> Result<Oplog> {
        let coll = self.client.db("local").collection("oplog.rs");

        let mut opts = FindOptions::new();
        opts.cursor_type = CursorType::TailableAwait;
        opts.no_cursor_timeout = true;

        let cursor = coll.find(self.filter.clone(), Some(opts))?;

        Ok(Oplog { cursor: cursor })
    }

    /// Provide an optional filter for the oplog.
    ///
    /// This is empty by default so all operations are returned.
    #[allow(dead_code)]
    pub fn filter(&mut self, filter: Option<Document>) -> &mut OplogBuilder<'a> {
        self.filter = filter;
        self
    }
}
