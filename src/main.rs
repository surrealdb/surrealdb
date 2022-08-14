//! This binary is the web-platform server for [SurrealDB](https://surrealdb.com) the
//! ultimate cloud database for tomorrow's applications. SurrealDB is a scalable,
//! distributed, collaborative, document-graph database for the realtime web.
//!
//! This binary can be used to start a database server instance using an embedded
//! in-memory datastore, or an embedded datastore persisted to disk. In addition, it
//! can be used in distributed mode by connecting to a distributed [TiKV](https://tikv.org)
//! key-value store.

#![forbid(unsafe_code)]

#[macro_use]
extern crate log;

#[macro_use]
mod mac;

mod cli;
mod cnf;
mod dbs;
mod err;
mod iam;
mod net;
mod rpc;

fn main() {
	cli::init(); // Initiate the command line
}
