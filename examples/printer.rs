extern crate mongodb;
extern crate oplog;

use mongodb::{Client, ThreadedClient};
use oplog::Oplog;

fn main() {
    let client = Client::connect("localhost", 27017).expect("Failed to connect to MongoDB.");

    if let Ok(oplog) = Oplog::new(&client) {
        for doc in oplog {
            println!("{}", doc);
        }
    }
}
