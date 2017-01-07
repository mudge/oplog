extern crate mongodb;
extern crate oplog;

use mongodb::{Client, ThreadedClient};
use oplog::{Operation, Oplog};

fn main() {
    let client = Client::connect("localhost", 27017).expect("Failed to connect to MongoDB.");

    if let Ok(oplog) = Oplog::new(&client) {
        for operation in oplog {
            match operation {
                Operation::Noop { timestamp, .. } => println!("No-op at {}", timestamp),
                Operation::Insert { timestamp, .. } => println!("Insert at {}", timestamp),
                Operation::Update { timestamp, .. } => println!("Update at {}", timestamp),
                Operation::Delete { timestamp, .. } => println!("Delete at {}", timestamp),
                Operation::Command { timestamp, .. } => println!("Command at {}", timestamp),
                Operation::ApplyOps { timestamp, .. } => println!("ApplyOps at {}", timestamp),
            }
        }
    }
}
