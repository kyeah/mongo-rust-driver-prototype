//! Interface for database-level operations.
//!
//! # Usage
//!
//! The database API provides methods for opening, creating, deleting, and listing collections.
//! It also handles user-level authentication over SCRAM-SHA-1.
//!
//! ## Collection Operations
//!
//! ```no_run
//! # use mongodb::{Client, ThreadedClient};
//! # use mongodb::db::ThreadedDatabase;
//! # let client = Client::connect("localhost", 27017).unwrap();
//! #
//! let db = client.db("movies");
//! db.create_collection("action", None).unwrap();
//! let collection_names = db.collection_names(None).unwrap();
//! assert!(!collection_names.is_empty());
//! ```
//!
//! ## Authentication
//!
//! ```no_run
//! # use mongodb::{Client, ThreadedClient};
//! # use mongodb::db::ThreadedDatabase;
//! # let client = Client::connect("localhost", 27017).unwrap();
//! #
//! let db = client.db("redacted");
//! db.create_user("saghm", "1234", None).unwrap();
//! db.auth("saghm", "1234").unwrap();
//!
//! let success = db.list_collections(None).unwrap();
//! ```
//!
//! ## Arbitrary Database Commands
//!
//! Any valid MongoDB database command can be sent to the server with the `command` and
//! `command_cursor` functions.
//!
//! ```no_run
//! # #[macro_use] extern crate bson;
//! # extern crate mongodb;
//! #
//! # use mongodb::{Client, CommandType, ThreadedClient};
//! # use mongodb::db::ThreadedDatabase;
//! # use bson::Bson;
//! # fn main() {
//! # let client = Client::connect("localhost", 27017).unwrap();
//! #
//! let db = client.db("movies");
//! let cmd = doc! { "connectionStatus": 1 };
//! let result = db.command(cmd, CommandType::Suppressed, None).unwrap();
//! if let Some(&Bson::Document(ref doc)) = result.get("authInfo") {
//!     // Read authentication info.
//! }
//! # }
//! ```
pub mod options;
pub mod roles;

use auth::Authenticator;
use bson;
use bson::Bson;
use {Client, CommandType, ThreadedClient, Result};
use Error::{CursorNotFoundError, OperationError, ResponseError};
use coll::Collection;
use coll::options::FindOptions;
use common::{ReadPreference, merge_options, WriteConcern};
use cursor::{Cursor, DEFAULT_BATCH_SIZE};
use self::options::{CreateCollectionOptions, CreateUserOptions, UserInfoOptions};
use semver::Version;
use std::error::Error;
use std::sync::Arc;

/// Interfaces with a MongoDB database.
pub struct DatabaseInner {
    /// The database name.
    pub name: String,
    /// A reference to the client that spawned this database.
    pub client: Client,
    /// Indicates how a server should be selected for read operations.
    pub read_preference: ReadPreference,
    /// Describes the guarantees provided by MongoDB when reporting the success of a write
    /// operation.
    pub write_concern: WriteConcern,
}

pub type Database = Arc<DatabaseInner>;

pub trait ThreadedDatabase {
    /// Creates a database representation with optional read and write controls.
    fn open(
        client: Client,
        name: &str,
        read_preference: Option<ReadPreference>,
        write_concern: Option<WriteConcern>,
    ) -> Database;
    // Returns the version of the MongoDB instance.
    fn version(&self) -> Result<Version>;
    /// Logs in a user using the SCRAM-SHA-1 mechanism.
    fn auth(&self, user: &str, password: &str) -> Result<()>;
    /// Creates a collection representation with inherited read and write controls.
    fn collection(&self, coll_name: &str) -> Collection;
    /// Creates a collection representation with custom read and write controls.
    fn collection_with_prefs(
        &self,
        coll_name: &str,
        create: bool,
        read_preference: Option<ReadPreference>,
        write_concern: Option<WriteConcern>,
    ) -> Collection;
    /// Return a unique operational request id.
    fn get_req_id(&self) -> i32;
    /// Generates a cursor for a relevant operational command.
    fn command_cursor(
        &self,
        spec: bson::Document,
        cmd_type: CommandType,
        read_pref: ReadPreference,
    ) -> Result<Cursor>;
    /// Sends an administrative command over find_one.
    fn command(
        &self,
        spec: bson::Document,
        cmd_type: CommandType,
        read_preference: Option<ReadPreference>,
    ) -> Result<bson::Document>;
    /// Returns a list of collections within the database.
    fn list_collections<D>(&self, filter: D) -> Result<Cursor> where
        D: Into<Option<bson::Document>>;
    /// Returns a list of collections within the database with a custom batch size.
    fn list_collections_with_batch_size<D>(
        &self,
        filter: D,
        batch_size: i32,
    ) -> Result<Cursor> where D: Into<Option<bson::Document>>;
    /// Returns a list of collection names within the database.
    fn collection_names<D>(&self, filter: D) -> Result<Vec<String>> where
        D: Into<Option<bson::Document>>;
    /// Creates a new collection.
    ///
    /// Note that due to the implicit creation of collections during insertion, this
    /// method should only be used to instantiate capped collections.
    fn create_collection<O>(&self, name: &str, options: O) -> Result<()> where
        O: Into<Option<CreateCollectionOptions>>;
    /// Creates a new user.
    fn create_user<O>(
        &self,
        name: &str,
        password: &str,
        options: O,
    ) -> Result<()> where O: Into<Option<CreateUserOptions>>;
    /// Permanently deletes all users from the database.
    fn drop_all_users<WC>(&self, write_concern: WC) -> Result<(i32)> where
        WC: Into<Option<WriteConcern>>;
    /// Permanently deletes the collection from the database.
    fn drop_collection(&self, name: &str) -> Result<()>;
    /// Permanently deletes the database from the server.
    fn drop_database(&self) -> Result<()>;
    /// Permanently deletes the user from the database.
    fn drop_user<WC>(&self, name: &str, write_concern: WC) -> Result<()> where
        WC: Into<Option<WriteConcern>>;
    /// Retrieves information about all users in the database.
    fn get_all_users(&self, show_credentials: bool) -> Result<Vec<bson::Document>>;
    /// Retrieves information about a given user from the database.
    fn get_user<O>(&self, user: &str, options: O) -> Result<bson::Document> where
        O: Into<Option<UserInfoOptions>>;
    /// Retrieves information about a given set of users from the database.
    fn get_users<O>(
        &self,
        users: Vec<&str>,
        options: O,
    ) -> Result<Vec<bson::Document>> where O: Into<Option<UserInfoOptions>>;
}

impl ThreadedDatabase for Database {
    fn open(
        client: Client,
        name: &str,
        read_preference: Option<ReadPreference>,
        write_concern: Option<WriteConcern>,
    ) -> Database {
        let rp = read_preference.unwrap_or_else(|| client.read_preference.to_owned());
        let wc = write_concern.unwrap_or_else(|| client.write_concern.to_owned());

        Arc::new(DatabaseInner {
            name: String::from(name),
            client: client,
            read_preference: rp,
            write_concern: wc,
        })
    }

    fn auth(&self, user: &str, password: &str) -> Result<()> {
        let authenticator = Authenticator::new(self.clone());
        authenticator.auth(user, password)
    }

    fn collection(&self, coll_name: &str) -> Collection {
        Collection::new(
            self.clone(),
            coll_name,
            false,
            self.read_preference.to_owned(),
            self.write_concern.to_owned(),
        )
    }

    fn collection_with_prefs(
        &self,
        coll_name: &str,
        create: bool,
        read_preference: Option<ReadPreference>,
        write_concern: Option<WriteConcern>,
    ) -> Collection {
        Collection::new(
            self.clone(),
            coll_name,
            create,
            read_preference,
            write_concern,
        )
    }

    fn get_req_id(&self) -> i32 {
        self.client.get_req_id()
    }

    fn command_cursor(
        &self,
        spec: bson::Document,
        cmd_type: CommandType,
        read_pref: ReadPreference,
    ) -> Result<Cursor> {
        Cursor::command_cursor(
            self.client.clone(),
            &self.name[..],
            spec,
            cmd_type,
            read_pref,
        )
    }

    fn command(
        &self,
        spec: bson::Document,
        cmd_type: CommandType,
        read_preference: Option<ReadPreference>,
    ) -> Result<bson::Document> {

        let coll = self.collection("$cmd");
        let mut options = FindOptions::new();
        options.batch_size = Some(1);
        options.read_preference = read_preference;
        let res = try!(coll.find_one_with_command_type(
            Some(spec.clone()),
            Some(options),
            cmd_type,
        ));
        res.ok_or_else(|| {
            OperationError(format!("Failed to execute command with spec {:?}.", spec))
        })
    }

    fn list_collections<D>(&self, filter: D) -> Result<Cursor> where
        D: Into<Option<bson::Document>>
    {
        self.list_collections_with_batch_size(filter, DEFAULT_BATCH_SIZE)
    }

    fn list_collections_with_batch_size<D>(
        &self,
        filter: D,
        batch_size: i32,
    ) -> Result<Cursor> where D: Into<Option<bson::Document>> {
        let mut spec = bson::Document::new();
        let mut cursor = bson::Document::new();

        cursor.insert("batchSize", Bson::I32(batch_size));
        spec.insert("listCollections", Bson::I32(1));
        spec.insert("cursor", Bson::Document(cursor));

        filter.into().map(|filter| {
            spec.insert("filter", Bson::Document(filter));
        });

        self.command_cursor(
            spec,
            CommandType::ListCollections,
            self.read_preference.to_owned(),
        )
    }

    fn collection_names<D>(&self, filter: D) -> Result<Vec<String>> where
        D: Into<Option<bson::Document>>
    {
        let mut cursor = try!(self.list_collections(filter));
        let mut results = vec![];
        loop {
            match cursor.next() {
                Some(Ok(doc)) => {
                    if let Some(&Bson::String(ref name)) = doc.get("name") {
                        results.push(name.to_owned());
                    }
                }
                Some(Err(err)) => return Err(err),
                None => return Ok(results),
            }
        }
    }

    fn version(&self) -> Result<Version> {
        let doc = doc! { "buildinfo": 1 };
        let out = try!(self.command(doc, CommandType::BuildInfo, None));

        match out.get("version") {
            Some(&Bson::String(ref s)) => {
                match Version::parse(s) {
                    Ok(v) => Ok(v),
                    Err(e) => Err(ResponseError(String::from(e.description()))),
                }
            }
            _ => Err(ResponseError(
                String::from("No version received from server"),
            )),
        }
    }

    fn create_collection<O>(
        &self,
        name: &str,
        options: O,
    ) -> Result<()> where O: Into<Option<CreateCollectionOptions>> {
        let mut doc = doc! { "create": name };
        options.into().map(|options| {
            doc = merge_options(doc, options)
        });

        self.command(doc, CommandType::CreateCollection, None).map(
            |_| (),
        )
    }

    fn create_user<O>(
        &self,
        name: &str,
        password: &str,
        options: O,
    ) -> Result<()> where O: Into<Option<CreateUserOptions>> {
        let mut doc =
            doc! {
            "createUser": name,
            "pwd": password
        };

        match options.into() {
            Some(user_options) => {
                doc = merge_options(doc, user_options);
            }
            None => {
                doc.insert("roles", Bson::Array(Vec::new()));
            }
        };

        self.command(doc, CommandType::CreateUser, None).map(|_| ())
    }

    fn drop_all_users<WC>(&self, write_concern: WC) -> Result<(i32)> where
        WC: Into<Option<WriteConcern>>
    {
        let mut doc = doc! { "dropAllUsersFromDatabase": 1 };

        write_concern.into().map(|concern| {
            doc.insert("writeConcern", Bson::Document(concern.to_bson()));
        });

        let response = try!(self.command(doc, CommandType::DropAllUsers, None));

        match response.get("n") {
            Some(&Bson::I32(i)) => Ok(i),
            Some(&Bson::I64(i)) => Ok(i as i32),
            _ => Err(CursorNotFoundError),
        }
    }

    fn drop_collection(&self, name: &str) -> Result<()> {
        let mut spec = bson::Document::new();
        spec.insert("drop", Bson::String(String::from(name)));
        try!(self.command(spec, CommandType::DropCollection, None));
        Ok(())
    }

    fn drop_database(&self) -> Result<()> {
        let mut spec = bson::Document::new();
        spec.insert("dropDatabase", Bson::I32(1));
        try!(self.command(spec, CommandType::DropDatabase, None));
        Ok(())
    }

    fn drop_user<WC>(&self, name: &str, write_concern: WC) -> Result<()> where
        WC: Into<Option<WriteConcern>>
    {
        let mut doc = doc! { "dropUser": name };

        write_concern.into().map(|concern| {
            doc.insert("writeConcern", concern.to_bson());
        });

        self.command(doc, CommandType::DropUser, None).map(|_| ())
    }

    fn get_all_users(&self, show_credentials: bool) -> Result<Vec<bson::Document>> {
        let doc =
            doc! {
            "usersInfo": 1,
            "showCredentials": show_credentials
        };

        let out = try!(self.command(doc, CommandType::GetUsers, None));
        let vec = match out.get("users") {
            Some(&Bson::Array(ref vec)) => vec.clone(),
            _ => return Err(CursorNotFoundError),
        };

        let mut users = vec![];

        for bson in vec {
            match bson {
                Bson::Document(doc) => users.push(doc),
                _ => return Err(CursorNotFoundError),
            };
        }

        Ok(users)
    }

    fn get_user<O>(&self, user: &str, options: O) -> Result<bson::Document> where
        O: Into<Option<UserInfoOptions>>
    {
        let mut doc =
            doc! {
            "usersInfo": { "user": user, "db": self.name.to_owned() }
        };

        options.into().map(|opts| {
            doc = merge_options(doc, opts);
        });

        let out = match self.command(doc, CommandType::GetUser, None) {
            Ok(doc) => doc,
            Err(e) => return Err(e),
        };

        let users = match out.get("users") {
            Some(&Bson::Array(ref v)) => v.clone(),
            _ => return Err(CursorNotFoundError),
        };

        match users.first() {
            Some(&Bson::Document(ref doc)) => Ok(doc.clone()),
            _ => Err(CursorNotFoundError),
        }
    }

    fn get_users<O>(
        &self,
        users: Vec<&str>,
        options: O,
    ) -> Result<Vec<bson::Document>> where O: Into<Option<UserInfoOptions>> {
        let vec: Vec<_> = users
            .into_iter()
            .map(|user| {
                let doc = doc! { "user": user, "db": self.name.to_owned() };
                Bson::Document(doc)
            })
            .collect();

        let mut doc = doc! { "usersInfo": vec };

        options.into().map(|opts| {
            doc = merge_options(doc, opts);
        });

        let out = try!(self.command(doc, CommandType::GetUsers, None));
        let vec = match out.get("users") {
            Some(&Bson::Array(ref vec)) => vec.clone(),
            _ => return Err(CursorNotFoundError),
        };

        let mut users = vec![];

        for bson in vec {
            match bson {
                Bson::Document(doc) => users.push(doc),
                _ => return Err(CursorNotFoundError),
            };
        }

        Ok(users)
    }
}
