use crate::cli::CF;
use crate::err::Error;
use clap::Args;
use once_cell::sync::OnceCell;
use std::time::Duration;
use surrealdb::kvs::Datastore;

pub static DB: OnceCell<Datastore> = OnceCell::new();

#[derive(Args, Debug)]
pub struct StartCommandDbsOptions {
	#[arg(help = "Whether strict mode is enabled on this database instance")]
	#[arg(env = "SURREAL_STRICT", short = 's', long = "strict")]
	#[arg(default_value_t = false)]
	strict_mode: bool,
	#[arg(help = "The maximum duration that a set of statements can run for")]
	#[arg(env = "SURREAL_QUERY_TIMEOUT", long)]
	#[arg(value_parser = super::cli::validator::duration)]
	query_timeout: Option<Duration>,
	#[arg(help = "The maximum duration that any single transaction can run for")]
	#[arg(env = "SURREAL_TRANSACTION_TIMEOUT", long)]
	#[arg(value_parser = super::cli::validator::duration)]
	transaction_timeout: Option<Duration>,
}

pub async fn init(
	StartCommandDbsOptions {
		strict_mode,
		query_timeout,
		transaction_timeout,
	}: StartCommandDbsOptions,
) -> Result<(), Error> {
	// Get local copy of options
	let opt = CF.get().unwrap();
	// Log specified strict mode
	debug!("Database strict mode is {strict_mode}");
	// Log specified query timeout
	if let Some(v) = query_timeout {
		debug!("Maximum query processing timeout is {v:?}");
	}
	// Log specified parse timeout
	if let Some(v) = transaction_timeout {
		debug!("Maximum transaction processing timeout is {v:?}");
	}
	// Parse and setup the desired kv datastore
	let dbs = Datastore::new(&opt.path)
		.await?
		.with_notifications()
		.with_strict_mode(strict_mode)
		.with_query_timeout(query_timeout)
		.with_transaction_timeout(transaction_timeout);
	// Store database instance
	let _ = DB.set(dbs);
	// All ok
	Ok(())
}
