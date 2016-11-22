#[macro_use(bson, doc)]
extern crate bson;
extern crate mongodb;
extern crate chrono;

use bson::ValueAccessError;
use mongodb::{Client, ThreadedClient};
use mongodb::cursor::Cursor;
use mongodb::db::ThreadedDatabase;
use mongodb::coll::options::{FindOptions, CursorType};
use chrono::*;

pub struct Oplog {
    cursor: Cursor,
}

#[derive(PartialEq, Debug)]
pub enum Operation<'a> {
    Insert { id: i64, namespace: &'a str },
    Update { id: i64, namespace: &'a str },
    Delete { id: i64, namespace: &'a str },
    Command { id: i64, namespace: &'a str },
    Database { id: i64, namespace: &'a str },
    Noop { id: i64, timestamp: DateTime<UTC> },
    Unknown
}

impl<'a> Operation<'a> {
    pub fn new(document: bson::Document) -> Result<Operation<'a>, ValueAccessError> {
        match document.get_str("op") {
            Ok("n") => Operation::noop(document),
            _ => Err(ValueAccessError::UnexpectedType),
        }
    }

    pub fn noop(document: bson::Document) -> Result<Operation<'a>, ValueAccessError> {
        let h = try!(document.get_i64("h"));
        let ts = try!(document.get_time_stamp("ts"));

        let seconds = ts >> 32;
        let nanoseconds = ((ts & 0xFFFFFFFF) * 1000000) as u32;

        Ok(Operation::Noop { id: h, timestamp: UTC.timestamp(seconds, nanoseconds) })
    }
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
    pub fn new(client: Client) -> Result<Oplog, mongodb::Error> {
        let coll = client.db("local").collection("oplog.rs");

        let mut opts = FindOptions::new();
        opts.cursor_type = CursorType::TailableAwait;
        opts.no_cursor_timeout = true;

        coll.find(None, Some(opts)).map(|cursor| Oplog { cursor: cursor })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::Bson;
    use chrono::*;

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

        match Operation::new(doc).unwrap() {
            Operation::Noop { id, timestamp } => {
                assert_eq!(-2135725856567446411i64, id);
                assert_eq!(UTC.timestamp(1479419535, 0), timestamp);
            },
            _ => panic!("Unexpected type of operation"),
        }
    }
}
