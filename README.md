# Oplog [![Build Status](https://travis-ci.org/mudge/oplog.svg?branch=master)](https://travis-ci.org/mudge/oplog)

A Rust library for
[iterating](https://doc.rust-lang.org/1.14.0/std/iter/index.html) over a
[MongoDB replica set
oplog](https://docs.mongodb.com/v3.0/core/replica-set-oplog/).

**Current version:** 0.3.0  
**Supported Rust versions:** 1.14

## Install

Install Oplog by adding the following to your `Cargo.toml`:

```toml
oplog = "0.3.0"
```

## Usage

```rust
#[macro_use]
extern crate bson;
extern crate mongodb;
extern crate oplog;

use mongodb::{Client, ThreadedClient};
use oplog::{Operation, Oplog, OplogBuilder};

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

    // Or, if you want to filter out certain operations:

    if let Ok(oplog) = OplogBuilder::new(&client).filter(Some(doc! { "op" => "i" })).build() {
        for insert in oplog {
            println!("{}", insert);
        }
    }
}
```

## Documentation

Full API documentation is available at http://mudge.name/oplog

## References

* [Iterators, Rust by Example](http://rustbyexample.com/trait/iter.html)
* [Replication Internals](https://www.kchodorow.com/blog/2010/10/12/replication-internals/)
* [applyOps](https://docs.mongodb.com/manual/reference/command/applyOps/)
* [ripgrep](https://github.com/BurntSushi/ripgrep/) was invaluable as a source of idiomatic Rust code (see also [ripgrep code review](http://blog.mbrt.it/2016-12-01-ripgrep-code-review/))

And many thanks to [Ryman](https://github.com/Ryman) for his help along the way.

## License

Copyright © 2016-2018 Paul Mucur.

Distributed under the MIT License.
