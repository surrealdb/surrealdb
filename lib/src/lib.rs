#![forbid(unsafe_code)]

#[macro_use]
extern crate log;

#[macro_use]
mod mac;

mod cnf;
mod ctx;
mod dbs;
mod doc;
mod err;
mod fnc;
mod key;
mod kvs;

pub mod sql;

pub use err::Error;

pub use dbs::execute;
pub use dbs::process;
pub use dbs::Auth;
pub use dbs::Response;
pub use dbs::Responses;
pub use dbs::Session;

pub use kvs::Datastore;
pub use kvs::Key;
pub use kvs::Transaction;
pub use kvs::Val;
