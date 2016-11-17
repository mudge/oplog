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
    opts.cursor_type = CursorType::TailableAwait;
    opts.no_cursor_timeout = true;

    let mut cursor = coll.find(None, Some(opts)).expect("Failed to execute find");

    loop {
        let item = cursor.next();

        match item {
            Some(Ok(doc)) => log_operation(doc),
            Some(Err(_)) => panic!("Failed to get next from server!"),
            None => println!("Reached end of oplog"),
        }
    }
}

fn log_operation(doc: bson::Document) {
    let op = match doc.get("op") {
        Some(&Bson::String(ref op)) => op,
        _ => panic!("Expected op to be a string!"),
    };

    let ns = match doc.get("ns") {
        Some(&Bson::String(ref ns)) => ns,
        _ => panic!("Expected ns to be a string!"),
    };

    let v = match doc.get("v") {
        Some(&Bson::I32(ref v)) => v,
        _ => panic!("Expected v to be a 32-bit integer!"),
    };

    let h = match doc.get("h") {
        Some(&Bson::I64(ref h)) => h,
        _ => panic!("Expected h to be a 64-bit integer!"),
    };

    let ts = match doc.get("ts") {
        Some(&Bson::TimeStamp(ref ts)) => ts,
        _ => panic!("Expected ts to be a timestamp!"),
    };

    let o = match doc.get("o") {
        Some(&Bson::Document(ref o)) => o,
        _ => panic!("Expected o to be a document!"),
    };

    println!("op: {} ns: {} v: {} h: {} ts: {} o: {}", op, ns, v, h, ts, o);
}
