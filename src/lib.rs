extern crate bson;
extern crate mongodb;

use std::result;

use mongodb::{Client, Error, ThreadedClient};
use mongodb::cursor::Cursor;
use mongodb::db::ThreadedDatabase;
use mongodb::coll::options::{FindOptions, CursorType};

type Result<T> = result::Result<T, Error>;

pub struct Oplog {
    cursor: Cursor,
}

#[derive(PartialEq, Debug)]
pub enum OperationType {
    Insert,
    Update,
    Delete,
    Command,
    Database,
    Noop
}

impl<'a> From<&'a str> for OperationType {
    fn from(a: &'a str) -> OperationType {
        match a {
            "i" => OperationType::Insert,
            "u" => OperationType::Update,
            "d" => OperationType::Delete,
            "c" => OperationType::Command,
            "db" => OperationType::Database,
            _ => OperationType::Noop
        }
    }
}

impl From<String> for OperationType {
    fn from(a: String) -> OperationType {
        OperationType::from(&*a)
    }
}

impl Iterator for Oplog {
    type Item = bson::Document;

    fn next(&mut self) -> Option<bson::Document> {
        loop {
            if let Some(Ok(op)) = self.cursor.next() {
                return Some(op);
            }
        }
    }
}

impl Oplog {
    pub fn new(client: Client) -> Result<Oplog> {
        let coll = client.db("local").collection("oplog.rs");

        let mut opts = FindOptions::new();
        opts.cursor_type = CursorType::TailableAwait;
        opts.no_cursor_timeout = true;

        coll.find(None, Some(opts)).map(|cursor| Oplog { cursor: cursor })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_converts_i_to_insert_operation() {
        assert_eq!(OperationType::Insert, OperationType::from("i"));
        assert_eq!(OperationType::Insert, OperationType::from("i".to_string()));
    }

    #[test]
    fn it_converts_u_to_update_operation() {
        assert_eq!(OperationType::Update, OperationType::from("u"));
        assert_eq!(OperationType::Update, OperationType::from("u".to_string()));
    }

    #[test]
    fn it_converts_d_to_delete_operation() {
        assert_eq!(OperationType::Delete, OperationType::from("d"));
        assert_eq!(OperationType::Delete, OperationType::from("d".to_string()));
    }

    #[test]
    fn it_converts_db_to_database_operation() {
        assert_eq!(OperationType::Database, OperationType::from("db"));
        assert_eq!(OperationType::Database, OperationType::from("db".to_string()));
    }

    #[test]
    fn it_converts_n_to_noop_operation() {
        assert_eq!(OperationType::Noop, OperationType::from("n"));
        assert_eq!(OperationType::Noop, OperationType::from("n".to_string()));
    }

    #[test]
    fn it_converts_c_to_command_operation() {
        assert_eq!(OperationType::Command, OperationType::from("c"));
        assert_eq!(OperationType::Command, OperationType::from("c".to_string()));
    }
}
