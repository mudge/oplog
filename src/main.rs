#[macro_use(bson, doc)]
extern crate bson;
extern crate mongodb;

use bson::Bson;
use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;
use mongodb::coll::options::{FindOptions, CursorType};

fn main() {
    let client = Client::connect("localhost", 27017)
        .ok().expect("Failed to connect to MongoDB.");

    let coll = client.db("local").collection("oplog.rs");

    loop {
        let mut opts = FindOptions::new();
        opts.cursor_type = CursorType::TailableAwait;
        opts.no_cursor_timeout = true;
        let cursor = coll.find(None, Some(opts)).expect("Failed to execute find");
        for result in cursor {
            if let Ok(item) = result {
                if let Some(&Bson::String(ref op)) = item.get("op") {
                    println!("op: {}", op);
                }
            }
        }
    }
}
