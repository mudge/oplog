#[macro_use]
extern crate bson;
extern crate mongodb;
extern crate oplog;

use mongodb::{Client, ThreadedClient};
use oplog::OplogBuilder;

fn main() {
    let client = Client::connect("localhost", 27017).expect("Failed to connect to MongoDB.");

    if let Ok(oplog) = OplogBuilder::new(&client).filter(Some(doc! { "op" => "i" })).build() {
        for insert in oplog {
            println!("{}", insert);
        }
    }
}

