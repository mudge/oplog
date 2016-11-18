extern crate bson;
extern crate mongodb;

use mongodb::{Client, ThreadedClient};
use mongodb::cursor::Cursor;
use mongodb::db::ThreadedDatabase;
use mongodb::coll::options::{FindOptions, CursorType};

struct Oplog {
    cursor: Cursor,
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

fn oplog(client: Client) -> Oplog {
    let coll = client.db("local").collection("oplog.rs");

    let mut opts = FindOptions::new();
    opts.cursor_type = CursorType::TailableAwait;
    opts.no_cursor_timeout = true;

    let cursor = coll.find(None, Some(opts)).expect("Failed to execute find");

    Oplog { cursor: cursor }
}

fn main() {
    let client = Client::connect("localhost", 27017)
        .ok().expect("Failed to connect to MongoDB.");

    for doc in oplog(client) {
        println!("{}", doc);
    }
}
