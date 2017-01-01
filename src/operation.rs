//! The operation module is responsible for converting MongoDB BSON documents into specific
//! `Operation` types, one for each type of document stored in the MongoDB oplog. As much as
//! possible, we convert BSON types into more typical Rust types (e.g. BSON timestamps into UTC
//! datetimes).
//!
//! As we accept _any_ document, it may not be a valid operation so wrap any conversions in a
//! `Result`.

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
    pub fn new(document: &Document) -> Result<Operation> {
        let op = document.get_str("op")?;

        match op {
            "n" => Operation::from_noop(document),
            "i" => Operation::from_insert(document),
            "u" => Operation::from_update(document),
            "d" => Operation::from_delete(document),
            "c" => Operation::from_command(document),
            op => Err(Error::UnknownOperation(op.into())),
        }
    }

    /// Returns a no-op operation for a given document.
    fn from_noop(document: &Document) -> Result<Operation> {
        let h = document.get_i64("h")?;
        let ts = document.get_time_stamp("ts")?;
        let o = document.get_document("o")?;
        let msg = o.get_str("msg")?;

        Ok(Operation::Noop {
            id: h,
            timestamp: timestamp_to_datetime(ts),
            message: msg.into(),
        })
    }

    /// Return an insert operation for a given document.
    fn from_insert(document: &Document) -> Result<Operation> {
        let h = document.get_i64("h")?;
        let ts = document.get_time_stamp("ts")?;
        let ns = document.get_str("ns")?;
        let o = document.get_document("o")?;

        Ok(Operation::Insert {
            id: h,
            timestamp: timestamp_to_datetime(ts),
            namespace: ns.into(),
            document: o.to_owned(),
        })
    }

    /// Return an update operation for a given document.
    fn from_update(document: &Document) -> Result<Operation> {
        let h = document.get_i64("h")?;
        let ts = document.get_time_stamp("ts")?;
        let ns = document.get_str("ns")?;
        let o = document.get_document("o")?;
        let o2 = document.get_document("o2")?;

        Ok(Operation::Update {
            id: h,
            timestamp: timestamp_to_datetime(ts),
            namespace: ns.into(),
            query: o2.to_owned(),
            update: o.to_owned(),
        })
    }

    /// Return a delete operation for a given document.
    fn from_delete(document: &Document) -> Result<Operation> {
        let h = document.get_i64("h")?;
        let ts = document.get_time_stamp("ts")?;
        let ns = document.get_str("ns")?;
        let o = document.get_document("o")?;

        Ok(Operation::Delete {
            id: h,
            timestamp: timestamp_to_datetime(ts),
            namespace: ns.into(),
            query: o.to_owned(),
        })
    }

    /// Return a command operation for a given document.
    fn from_command(document: &Document) -> Result<Operation> {
        let h = document.get_i64("h")?;
        let ts = document.get_time_stamp("ts")?;
        let ns = document.get_str("ns")?;
        let o = document.get_document("o")?;

        Ok(Operation::Command {
            id: h,
            timestamp: timestamp_to_datetime(ts),
            namespace: ns.into(),
            command: o.to_owned(),
        })
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

/// Convert a BSON timestamp into a UTC DateTime.
fn timestamp_to_datetime(timestamp: i64) -> DateTime<UTC> {
    let seconds = timestamp >> 32;
    let nanoseconds = ((timestamp & 0xFFFFFFFF) * 1000000) as u32;

    UTC.timestamp(seconds, nanoseconds)
}

#[cfg(test)]
mod tests {
    use Error;
    use bson::{Bson, ValueAccessError};
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
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::Noop {
                 id: -2135725856567446411i64,
                 timestamp: UTC.timestamp(1479419535, 0),
                 message: "initiating set".into()
            }
        );
    }

    #[test]
    fn operation_converts_inserts() {
        let doc = doc! {
            "ts" => (Bson::TimeStamp(1479561394 << 32)),
            "h" => (-1742072865587022793i64),
            "v" => 2,
            "op" => "i",
            "ns" => "foo.bar",
            "o" => {
                "foo" => "bar"
            }
        };
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::Insert {
                id: -1742072865587022793i64,
                timestamp: UTC.timestamp(1479561394, 0),
                namespace: "foo.bar".into(),
                document: doc! { "foo" => "bar" },
            }
        );
    }

    #[test]
    fn operation_converts_updates() {
        let doc = doc! {
            "ts" => (Bson::TimeStamp(1479561033 << 32)),
            "h" => (3511341713062188019i64),
            "v" => 2,
            "op" => "u",
            "ns" => "foo.bar",
            "o2" => {
                "_id" => 1
            },
            "o" => {
                "$set" => {
                    "foo" => "baz"
                }
            }
        };
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::Update {
                id: 3511341713062188019i64,
                timestamp: UTC.timestamp(1479561033, 0),
                namespace: "foo.bar".into(),
                query: doc! { "_id" => 1 },
                update: doc! { "$set" => { "foo" => "baz" } },
            }
        );
    }

    #[test]
    fn operation_converts_deletes() {
        let doc = doc! {
            "ts" => (Bson::TimeStamp(1479421186 << 32)),
            "h" => (-5457382347563537847i64),
            "v" => 2,
            "op" => "d",
            "ns" => "foo.bar",
            "o" => {
                "_id" => 1
            }
        };
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::Delete {
                id: -5457382347563537847i64,
                timestamp: UTC.timestamp(1479421186, 0),
                namespace: "foo.bar".into(),
                query: doc! { "_id" => 1 },
            }
        );
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
        let operation = Operation::new(&doc).unwrap();

        assert_eq!(
            operation,
            Operation::Command {
                id: -7222343681970774929i64,
                timestamp: UTC.timestamp(1479553955, 0),
                namespace: "test.$cmd".into(),
                command: doc! { "create" => "foo" },
            }
        );
    }

    #[test]
    fn operation_returns_unknown_operations() {
        let doc = doc! { "op" => "x" };
        let operation = Operation::new(&doc);

        match operation {
            Err(Error::UnknownOperation(op)) => assert_eq!(op, "x"),
            _ => panic!("Expected unknown operation."),
        }
    }

    #[test]
    fn operation_returns_missing_fields() {
        let doc = doc! { "foo" => "bar" };
        let operation = Operation::new(&doc);

        match operation {
            Err(Error::MissingField(err)) => assert_eq!(err, ValueAccessError::NotPresent),
            _ => panic!("Expected missing field."),
        }
    }
}
