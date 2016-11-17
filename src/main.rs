extern crate bson;
extern crate mongodb;

use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use mongodb::coll::options::{FindOptions, CursorType};

fn main() {
    let client = Client::connect("localhost", 27017)
        .ok().expect("Failed to connect to MongoDB.");

    let coll = client.db("local").collection("oplog.rs");

    let mut opts = FindOptions::new();
    opts.cursor_type = CursorType::TailableAwait;
    opts.no_cursor_timeout = true;

    let mut cursor = coll.find(None, Some(opts)).expect("Failed to execute find");

    loop {
        if let Some(Ok(doc)) = cursor.next() {
            println!("{}", doc);
        }
    }
}
