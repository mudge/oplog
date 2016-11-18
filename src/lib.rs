#[macro_use(bson, doc)]
extern crate bson;
extern crate mongodb;

use mongodb::{Client, ThreadedClient};
use mongodb::cursor::Cursor;
use mongodb::db::ThreadedDatabase;
use mongodb::coll::options::{FindOptions, CursorType};

pub struct Oplog {
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

pub fn oplog(client: Client) -> Oplog {
    let coll = client.db("local").collection("oplog.rs");

    let mut opts = FindOptions::new();
    opts.cursor_type = CursorType::TailableAwait;
    opts.no_cursor_timeout = true;

    let cursor = coll.find(None, Some(opts)).expect("Failed to execute find");

    Oplog { cursor: cursor }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::{Client, ThreadedClient};
    use mongodb::db::ThreadedDatabase;

    #[test]
    fn it_works() {
        let client = Client::connect("localhost", 27017)
            .ok().expect("Failed to connect to MongoDB.");
        let coll = client.db("test").collection("hats");
        let doc = doc! { "foo" => "bar" };
        coll.insert_one(doc.clone(), None).ok().expect("Failed to insert document.");
        oplog(client).next();
    }
}
