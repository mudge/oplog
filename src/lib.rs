#[macro_use(bson, doc)]
extern crate bson;
extern crate mongodb;
extern crate chrono;

use std::result;
use mongodb::{Client, ThreadedClient};
use mongodb::cursor::Cursor;
use mongodb::db::ThreadedDatabase;
use mongodb::coll::options::{FindOptions, CursorType};
use chrono::{DateTime, UTC, TimeZone};

#[derive(Debug)]
pub enum OplogError {
    MissingField(bson::ValueAccessError),
    Database(mongodb::Error),
    UnknownOperation(String),
}

type Result<T> = result::Result<T, OplogError>;

pub struct Oplog {
    cursor: Cursor,
}

#[derive(PartialEq, Debug)]
pub enum Operation<'a> {
    Insert { id: i64, timestamp: DateTime<UTC>, namespace: &'a str, document: &'a bson::Document },
    Update { id: i64, namespace: &'a str },
    Delete { id: i64, namespace: &'a str },
    Command { id: i64, namespace: &'a str },
    Database { id: i64, namespace: &'a str },
    Noop { id: i64, timestamp: DateTime<UTC>, document: &'a bson::Document },
    Unknown
}

impl<'a> Operation<'a> {
    pub fn new(document: &'a bson::Document) -> Result<Operation<'a>> {
        let op = try!(document.get_str("op").map_err(OplogError::MissingField));

        match op {
            "n" => document_to_noop(document),
            "i" => document_to_insert(document),
            _ => Err(OplogError::UnknownOperation(op.to_owned())),
        }
    }
}

fn document_to_noop(document: &bson::Document) -> Result<Operation> {
    let h = try!(document.get_i64("h").map_err(OplogError::MissingField));
    let ts = try!(document.get_time_stamp("ts").map_err(OplogError::MissingField));
    let o = try!(document.get_document("o").map_err(OplogError::MissingField));

    Ok(Operation::Noop { id: h, timestamp: timestamp_to_datetime(ts), document: o })
}

fn document_to_insert(document: &bson::Document) -> Result<Operation> {
    let h = try!(document.get_i64("h").map_err(OplogError::MissingField));
    let ts = try!(document.get_time_stamp("ts").map_err(OplogError::MissingField));
    let o = try!(document.get_document("o").map_err(OplogError::MissingField));
    let ns = try!(document.get_str("ns").map_err(OplogError::MissingField));

    Ok(Operation::Insert { id: h, timestamp: timestamp_to_datetime(ts), document: o, namespace: ns })
}

fn timestamp_to_datetime(timestamp: i64) -> DateTime<UTC> {
    let seconds = timestamp >> 32;
    let nanoseconds = ((timestamp & 0xFFFFFFFF) * 1000000) as u32;

    UTC.timestamp(seconds, nanoseconds)
}

impl Iterator for Oplog {
    type Item = bson::Document;

    fn next(&mut self) -> Option<bson::Document> {
        loop {
            if let Some(Ok(op)) = self.cursor.next() {
                return Some(op);
            }
        }
    }
}

impl Oplog {
    pub fn new(client: Client) -> Result<Oplog> {
        let coll = client.db("local").collection("oplog.rs");

        let mut opts = FindOptions::new();
        opts.cursor_type = CursorType::TailableAwait;
        opts.no_cursor_timeout = true;

        coll.find(None, Some(opts)).map(|cursor| Oplog { cursor: cursor }).map_err(OplogError::Database)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::Bson;
    use bson::oid::ObjectId;
    use chrono::{UTC, TimeZone};

    #[test]
    fn operation_converts_noops() {
        let doc = doc! {
            "ts" => (Bson::TimeStamp(1479419535 << 32)),
            "h" => (-2135725856567446411i64),
            "v" => 2,
            "op" => "n",
            "ns" => "",
            "o" => {
                "msg" => "initiating set"
            }
        };
        let operation = Operation::new(&doc).unwrap();

        match operation {
            Operation::Noop { id, timestamp, document } => {
                assert_eq!(-2135725856567446411i64, id);
                assert_eq!(UTC.timestamp(1479419535, 0), timestamp);
                assert_eq!("initiating set", document.get_str("msg").unwrap());
            },
            _ => panic!("Unexpected type of operation"),
        }
    }

    #[test]
    fn operation_converts_inserts() {
        let oid = ObjectId::with_string("583050b26813716e505a5bf2").unwrap();
        let doc = doc! {
            "ts" => (Bson::TimeStamp(1479561394 << 32)),
            "h" => (-1742072865587022793i64),
            "v" => 2,
            "op" => "i",
            "ns" => "foo.bar",
            "o" => {
                "_id" => (Bson::ObjectId(oid)),
                "foo" => "bar"
            }
        };
        let operation = Operation::new(&doc).unwrap();

        match operation {
            Operation::Insert { id, timestamp, namespace, document } => {
                assert_eq!(-1742072865587022793i64, id);
                assert_eq!(UTC.timestamp(1479561394, 0), timestamp);
                assert_eq!("foo.bar", namespace);
                assert_eq!("bar", document.get_str("foo").unwrap());
            },
            _ => panic!("Unexpected type of operation"),
        }
    }
}
