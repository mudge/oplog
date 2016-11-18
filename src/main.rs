extern crate tailspin;
extern crate mongodb;

use mongodb::{Client, ThreadedClient};
use tailspin::oplog;

fn main() {
    let client = Client::connect("localhost", 27017)
        .ok().expect("Failed to connect to MongoDB.");

    for doc in oplog(client) {
        println!("{}", doc);
    }
}
