use mongodb;
use bson;

use std::fmt;
use std::result;
use std::error;
use chrono::{DateTime, UTC, TimeZone};

#[derive(Debug)]
pub enum Error {
    MissingField(bson::ValueAccessError),
    Database(mongodb::Error),
    UnknownOperation(String),
    InvalidOperation,
}

impl From<bson::ValueAccessError> for Error {
    fn from(original: bson::ValueAccessError) -> Error {
        Error::MissingField(original)
    }
}

impl From<mongodb::Error> for Error {
    fn from(original: mongodb::Error) -> Error {
        Error::Database(original)
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::MissingField(ref err) => err.description(),
            Error::Database(ref err) => err.description(),
            Error::UnknownOperation(_) => "unknown operation type",
            Error::InvalidOperation => "invalid operation",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::MissingField(ref err) => err.fmt(f),
            Error::Database(ref err) => err.fmt(f),
            Error::UnknownOperation(ref op) => write!(f, "Unknown operation type found: {}", op),
            Error::InvalidOperation => write!(f, "Invalid operation"),
        }
    }
}

type Result<T> = result::Result<T, Error>;

#[derive(PartialEq, Debug)]
pub enum Operation {
    Noop {
        id: i64,
        timestamp: DateTime<UTC>,
        message: String,
    },
    Insert {
        id: i64,
        timestamp: DateTime<UTC>,
        namespace: String,
        document: bson::Document,
    },
    Update {
        id: i64,
        timestamp: DateTime<UTC>,
        namespace: String,
        query: bson::Document,
        update: bson::Document,
    },
    Delete {
        id: i64,
        timestamp: DateTime<UTC>,
        namespace: String,
        query: bson::Document,
    },
    Command {
        id: i64,
        timestamp: DateTime<UTC>,
        namespace: String,
        command: bson::Document,
    },
}

impl Operation {
    pub fn new(document: bson::Document) -> Result<Operation> {
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

fn operation(document: &bson::Document) -> Option<char> {
    document.get_str("op").ok().and_then(|op| op.chars().next())
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

fn noop(document: bson::Document) -> Result<Operation> {
    let h = try!(document.get_i64("h"));
    let ts = try!(document.get_time_stamp("ts"));
    let o = try!(document.get_document("o"));
    let msg = try!(o.get_str("msg"));

    Ok(Operation::Noop {
        id: h,
        timestamp: timestamp_to_datetime(ts),
        message: msg.to_owned(),
    })
}

fn insert(document: bson::Document) -> Result<Operation> {
    let h = try!(document.get_i64("h"));
    let ts = try!(document.get_time_stamp("ts"));
    let ns = try!(document.get_str("ns"));
    let o = try!(document.get_document("o"));

    Ok(Operation::Insert {
        id: h,
        timestamp: timestamp_to_datetime(ts),
        namespace: ns.to_owned(),
        document: o.to_owned(),
    })
}

fn update(document: bson::Document) -> Result<Operation> {
    let h = try!(document.get_i64("h"));
    let ts = try!(document.get_time_stamp("ts"));
    let ns = try!(document.get_str("ns"));
    let o = try!(document.get_document("o"));
    let o2 = try!(document.get_document("o2"));

    Ok(Operation::Update {
        id: h,
        timestamp: timestamp_to_datetime(ts),
        namespace: ns.to_owned(),
        query: o2.to_owned(),
        update: o.to_owned(),
    })
}

fn delete(document: bson::Document) -> Result<Operation> {
    let h = try!(document.get_i64("h"));
    let ts = try!(document.get_time_stamp("ts"));
    let ns = try!(document.get_str("ns"));
    let o = try!(document.get_document("o"));

    Ok(Operation::Delete {
        id: h,
        timestamp: timestamp_to_datetime(ts),
        namespace: ns.to_owned(),
        query: o.to_owned(),
    })
}

fn command(document: bson::Document) -> Result<Operation> {
    let h = try!(document.get_i64("h"));
    let ts = try!(document.get_time_stamp("ts"));
    let ns = try!(document.get_str("ns"));
    let o = try!(document.get_document("o"));

    Ok(Operation::Command {
        id: h,
        timestamp: timestamp_to_datetime(ts),
        namespace: ns.to_owned(),
        command: o.to_owned(),
    })
}

fn timestamp_to_datetime(timestamp: i64) -> DateTime<UTC> {
    let seconds = timestamp >> 32;
    let nanoseconds = ((timestamp & 0xFFFFFFFF) * 1000000) as u32;

    UTC.timestamp(seconds, nanoseconds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::Bson;
    use bson::oid::ObjectId;
    use chrono::{UTC, TimeZone};

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
}
