# Tailspin [![Build Status](https://travis-ci.org/mudge/tailspin.svg?branch=master)](https://travis-ci.org/mudge/tailspin)

This is an in-progress exercise in learning [Rust](https://www.rust-lang.org/)
by implementing a library that tails a [MongoDB
oplog](https://docs.mongodb.com/v3.0/core/replica-set-oplog/).

```rust
#[macro_use]
extern crate bson;
extern crate tailspin;
extern crate mongodb;

use mongodb::{Client, ThreadedClient};
use tailspin::{Oplog, OplogBuilder};

fn main() {
    let client = Client::connect("localhost", 27017).expect("Failed to connect to MongoDB.");

    if let Ok(oplog) = Oplog::new(&client) {
        for doc in oplog {
            println!("{}", doc);
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

## References

* [Iterators, Rust by Example](http://rustbyexample.com/trait/iter.html)
* [Replication Internals](https://www.kchodorow.com/blog/2010/10/12/replication-internals/)
* [ripgrep code review](http://blog.mbrt.it/2016-12-01-ripgrep-code-review/)
* [ripgrep](https://github.com/BurntSushi/ripgrep/)

## License

Copyright © 2016 Paul Mucur.

Distributed under the MIT License.
