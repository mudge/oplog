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

    let mut opts = FindOptions::new();
    opts.cursor_type = CursorType::Tailable;
    let mut cursor = coll.find(None, Some(opts)).expect("Failed to execute find");
    let results = cursor.next_n(3).expect("Failed to retrieve documents");

    for result in results {
        match result.get("op") {
            Some(&Bson::String(ref op)) => println!("op: {}", op),
            _ => panic!("Expected string!"),
        };
    }
}
