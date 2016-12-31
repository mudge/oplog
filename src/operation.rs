//! The operation module is responsible for converting MongoDB BSON documents into specific
//! `Operation` types, one for each type of document stored in the MongoDB oplog. As much as
//! possible, we convert BSON types into more typical Rust types (e.g. BSON timestamps into UTC
//! datetimes). As we accept _any_ document, it may not be a valid operation so wrap any failed
//! conversions in a `Result`.

use std::fmt;

use bson::Document;
use chrono::{DateTime, UTC, TimeZone};
use {Error, Result};

/// A MongoDB oplog operation.
#[derive(Clone, Debug, PartialEq)]
pub enum Operation {
    /// A no-op as inserted periodically by MongoDB or used to initiate new replica sets.
    Noop {
        /// A unique identifier for this operation.
        id: i64,
        /// The time of the operation.
        timestamp: DateTime<UTC>,
        /// The message associated with this operation.
        message: String,
    },
    /// An insert of a document into a specific database and collection.
    Insert {
        /// A unique identifier for this operation.
        id: i64,
        /// The time of the operation.
        timestamp: DateTime<UTC>,
        /// The full namespace of the operation including its database and collection.
        namespace: String,
        /// The BSON document inserted into the namespace.
        document: Document,
    },
    /// An update of a document in a specific database and collection matching a given query.
    Update {
        /// A unique identifier for this operation.
        id: i64,
        /// The time of the operation.
        timestamp: DateTime<UTC>,
        /// The full namespace of the operation including its database and collection.
        namespace: String,
        /// The BSON selection criteria for the update.
        query: Document,
        /// The BSON update applied in this operation.
        update: Document,
    },
    /// The deletion of a document in a specific database and collection matching a given query.
    Delete {
        /// A unique identifier for this operation.
        id: i64,
        /// The time of the operation.
        timestamp: DateTime<UTC>,
        /// The full namespace of the operation including its database and collection.
        namespace: String,
        /// The BSON selection criteria for the delete.
        query: Document,
    },
    /// A command such as the creation or deletion of a collection.
    Command {
        /// A unique identifier for this operation.
        id: i64,
        /// The time of the operation.
        timestamp: DateTime<UTC>,
        /// The full namespace of the operation including its database and collection.
        namespace: String,
        /// The BSON command.
        command: Document,
    },
}

impl Operation {
    /// Try to create a new Operation from a BSON document.
    pub fn new(document: Document) -> Result<Operation> {
        let op = operation(&document);

        match op {
            Some('n') => noop(document),
            Some('i') => insert(document),
            Some('u') => update(document),
            Some('d') => delete(document),
            Some('c') => command(document),
            Some(unknown) => Err(Error::UnknownOperation(unknown.to_string())),
            None => Err(Error::InvalidOperation),
        }
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Operation::Noop { id, timestamp, ref message } => {
                write!(f, "No-op #{} at {}: {}", id, timestamp, message)
            }
            Operation::Insert { id, timestamp, ref namespace, ref document } => {
                write!(f,
                       "Insert #{} into {} at {}: {}",
                       id,
                       namespace,
                       timestamp,
                       document)
            }
            Operation::Update { id, timestamp, ref namespace, ref query, ref update } => {
                write!(f,
                       "Update #{} {} with {} at {}: {}",
                       id,
                       namespace,
                       query,
                       timestamp,
                       update)
            }
            Operation::Delete { id, timestamp, ref namespace, ref query } => {
                write!(f,
                       "Delete #{} from {} at {}: {}",
                       id,
                       namespace,
                       timestamp,
                       query)
            }
            Operation::Command { id, timestamp, ref namespace, ref command } => {
                write!(f,
                       "Command #{} {} at {}: {}",
                       id,
                       namespace,
                       timestamp,
                       command)
            }
        }
    }
}

/// Returns the operation type for a given document.
fn operation(document: &Document) -> Option<char> {
    document.get_str("op").ok().and_then(|op| op.chars().next())
}

/// Returns a no-op operation for a given document.
fn noop(document: Document) -> Result<Operation> {
    let h = document.get_i64("h")?;
    let ts = document.get_time_stamp("ts")?;
    let o = document.get_document("o")?;
    let msg = o.get_str("msg")?;

    Ok(Operation::Noop {
        id: h,
        timestamp: timestamp_to_datetime(ts),
        message: msg.to_owned(),
    })
}

/// Return an insert operation for a given document.
fn insert(document: Document) -> Result<Operation> {
    let h = document.get_i64("h")?;
    let ts = document.get_time_stamp("ts")?;
    let ns = document.get_str("ns")?;
    let o = document.get_document("o")?;

    Ok(Operation::Insert {
        id: h,
        timestamp: timestamp_to_datetime(ts),
        namespace: ns.to_owned(),
        document: o.to_owned(),
    })
}

/// Return an update operation for a given document.
fn update(document: Document) -> Result<Operation> {
    let h = document.get_i64("h")?;
    let ts = document.get_time_stamp("ts")?;
    let ns = document.get_str("ns")?;
    let o = document.get_document("o")?;
    let o2 = document.get_document("o2")?;

    Ok(Operation::Update {
        id: h,
        timestamp: timestamp_to_datetime(ts),
        namespace: ns.to_owned(),
        query: o2.to_owned(),
        update: o.to_owned(),
    })
}

/// Return a delete operation for a given document.
fn delete(document: Document) -> Result<Operation> {
    let h = document.get_i64("h")?;
    let ts = document.get_time_stamp("ts")?;
    let ns = document.get_str("ns")?;
    let o = document.get_document("o")?;

    Ok(Operation::Delete {
        id: h,
        timestamp: timestamp_to_datetime(ts),
        namespace: ns.to_owned(),
        query: o.to_owned(),
    })
}

/// Return a command operation for a given document.
fn command(document: Document) -> Result<Operation> {
    let h = document.get_i64("h")?;
    let ts = document.get_time_stamp("ts")?;
    let ns = document.get_str("ns")?;
    let o = document.get_document("o")?;

    Ok(Operation::Command {
        id: h,
        timestamp: timestamp_to_datetime(ts),
        namespace: ns.to_owned(),
        command: o.to_owned(),
    })
}

/// Convert a BSON timestamp into a UTC DateTime.
fn timestamp_to_datetime(timestamp: i64) -> DateTime<UTC> {
    let seconds = timestamp >> 32;
    let nanoseconds = ((timestamp & 0xFFFFFFFF) * 1000000) as u32;

    UTC.timestamp(seconds, nanoseconds)
}

#[cfg(test)]
mod tests {
    use Error;
    use bson::Bson;
    use bson::oid::ObjectId;
    use chrono::{UTC, TimeZone};

    use super::Operation;

    #[test]
    fn operation_converts_noops() {
        let doc = doc! {
            "ts" => (Bson::TimeStamp(1479419535 << 32)),
            "h" => (-2135725856567446411i64),
            "v" => 2,
            "op" => "n",
            "ns" => "",
            "o" => {
                "msg" => "initiating set"
            }
        };

        match Operation::new(doc) {
            Ok(Operation::Noop { id, timestamp, message }) => {
                assert_eq!(-2135725856567446411i64, id);
                assert_eq!(UTC.timestamp(1479419535, 0), timestamp);
                assert_eq!("initiating set", message);
            }
            _ => panic!("Unexpected operation"),
        }
    }

    #[test]
    fn operation_converts_inserts() {
        let oid = ObjectId::with_string("583050b26813716e505a5bf2").unwrap();
        let doc = doc! {
            "ts" => (Bson::TimeStamp(1479561394 << 32)),
            "h" => (-1742072865587022793i64),
            "v" => 2,
            "op" => "i",
            "ns" => "foo.bar",
            "o" => {
                "_id" => (Bson::ObjectId(oid)),
                "foo" => "bar"
            }
        };

        match Operation::new(doc) {
            Ok(Operation::Insert { id, timestamp, namespace, document }) => {
                assert_eq!(-1742072865587022793i64, id);
                assert_eq!(UTC.timestamp(1479561394, 0), timestamp);
                assert_eq!("foo.bar", namespace);
                assert_eq!("bar", document.get_str("foo").expect("foo missing"));
            }
            _ => panic!("Unexpected type of operation"),
        }
    }

    #[test]
    fn operation_converts_updates() {
        let oid = ObjectId::with_string("583033a3643431ab5be6ec35").unwrap();
        let doc = doc! {
            "ts" => (Bson::TimeStamp(1479561033 << 32)),
            "h" => (3511341713062188019i64),
            "v" => 2,
            "op" => "u",
            "ns" => "foo.bar",
            "o2" => {
                "_id" => (Bson::ObjectId(oid))
            },
            "o" => {
                "$set" => {
                    "foo" => "baz"
                }
            }
        };

        match Operation::new(doc) {
            Ok(Operation::Update { id, timestamp, namespace, query, update }) => {
                assert_eq!(3511341713062188019i64, id);
                assert_eq!(UTC.timestamp(1479561033, 0), timestamp);
                assert_eq!("foo.bar", namespace);
                assert_eq!(ObjectId::with_string("583033a3643431ab5be6ec35").unwrap(),
                           query.get_object_id("_id").expect("_id missing").to_owned());
                assert_eq!("baz",
                           update.get_document("$set").and_then(|o| o.get_str("foo")).unwrap());
            }
            _ => panic!("Unexpected type of operation"),
        }
    }

    #[test]
    fn operation_converts_deletes() {
        let oid = ObjectId::with_string("582e287cfedf6fb051b2efdf").unwrap();
        let doc = doc! {
            "ts" => (Bson::TimeStamp(1479421186 << 32)),
            "h" => (-5457382347563537847i64),
            "v" => 2,
            "op" => "d",
            "ns" => "foo.bar",
            "o" => {
                "_id" => (Bson::ObjectId(oid))
            }
        };

        match Operation::new(doc) {
            Ok(Operation::Delete { id, timestamp, namespace, query }) => {
                assert_eq!(-5457382347563537847i64, id);
                assert_eq!(UTC.timestamp(1479421186, 0), timestamp);
                assert_eq!("foo.bar", namespace);
                assert_eq!(ObjectId::with_string("582e287cfedf6fb051b2efdf").unwrap(),
                           query.get_object_id("_id").expect("_id missing").to_owned());
            }
            _ => panic!("Unexpected type of operation"),
        }
    }

    #[test]
    fn operation_converts_commands() {
        let doc = doc! {
            "ts" => (Bson::TimeStamp(1479553955 << 32)),
            "h" => (-7222343681970774929i64),
            "v" => 2,
            "op" => "c",
            "ns" => "test.$cmd",
            "o" => {
                "create" => "foo"
            }
        };

        match Operation::new(doc) {
            Ok(Operation::Command { id, timestamp, namespace, command }) => {
                assert_eq!(-7222343681970774929i64, id);
                assert_eq!(UTC.timestamp(1479553955, 0), timestamp);
                assert_eq!("test.$cmd", namespace);
                assert_eq!("foo", command.get_str("create").expect("create missing"));
            }
            _ => panic!("Unexpected type of operation"),
        }
    }

    #[test]
    fn operation_returns_unknown_operations() {
        let doc = doc! { "op" => "x" };

        match Operation::new(doc) {
            Err(Error::UnknownOperation(op)) => assert_eq!("x", op),
            _ => panic!("Expected unknown operation, got something else"),
        }
    }

    #[test]
    fn operation_returns_invalid_operations() {
        let doc = doc! { "foo" => "bar" };

        match Operation::new(doc) {
            Err(Error::InvalidOperation) => {},
            _ => panic!("Expected invalid operation, got something else"),
        }
    }
}
