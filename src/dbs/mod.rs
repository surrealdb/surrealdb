use std::time::Duration;

use crate::cli::CF;
use crate::err::Error;
use clap::Args;
use once_cell::sync::OnceCell;
use surrealdb::kvs::Datastore;

pub static DB: OnceCell<Datastore> = OnceCell::new();

const LOG: &str = "surrealdb::dbs";

#[derive(Args, Debug)]
pub struct StartCommandDbsOptions {
	#[arg(help = "The maximum duration of any query")]
	#[arg(env = "SURREAL_QUERY_TIMEOUT", long)]
	#[arg(value_parser = super::cli::validator::duration)]
	query_timeout: Option<Duration>,
}

pub async fn init(
	StartCommandDbsOptions {
		query_timeout,
	}: StartCommandDbsOptions,
) -> Result<(), Error> {
	// Get local copy of options
	let opt = CF.get().unwrap();
	// Log authentication options
	match opt.strict {
		true => info!(target: LOG, "Database strict mode is enabled"),
		false => info!(target: LOG, "Database strict mode is disabled"),
	};
	// Parse and setup the desired kv datastore
	let dbs = Datastore::new(&opt.path).await?.query_timeout(query_timeout);
	// Store database instance
	let _ = DB.set(dbs);
	// All ok
	Ok(())
}
