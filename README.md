# Oplog [![Build Status](https://travis-ci.org/mudge/oplog.svg?branch=master)](https://travis-ci.org/mudge/oplog)

This is an in-progress exercise in learning [Rust](https://www.rust-lang.org/)
by implementing a library that tails a [MongoDB
oplog](https://docs.mongodb.com/v3.0/core/replica-set-oplog/), exposing it as
an [`Iterator`](https://doc.rust-lang.org/1.14.0/std/iter/index.html).

```rust
#[macro_use]
extern crate bson;
extern crate mongodb;
extern crate oplog;

use mongodb::{Client, ThreadedClient};
use oplog::{Oplog, OplogBuilder};

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
