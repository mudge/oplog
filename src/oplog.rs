use mongodb::coll::options::{FindOptions, CursorType};
use mongodb::cursor::Cursor;
use mongodb::db::ThreadedDatabase;
use mongodb::{Client, ThreadedClient};
use operation::{Operation, Error};

pub struct Oplog {
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
    pub fn new(client: &Client) -> Result<Oplog, Error> {
        let coll = client.db("local").collection("oplog.rs");

        let mut opts = FindOptions::new();
        opts.cursor_type = CursorType::TailableAwait;
        opts.no_cursor_timeout = true;

        let cursor = try!(coll.find(None, Some(opts)));

        Ok(Oplog { cursor: cursor })
    }
}
