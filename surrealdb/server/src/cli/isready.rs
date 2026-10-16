use anyhow::Result;
use clap::Args;
use serde_json::json;
use surrealdb::engine::any::connect;

use crate::cli::abstraction::DatabaseConnectionArguments;
use crate::cli::json;

#[derive(Args, Debug)]
pub struct IsReadyCommandArguments {
	#[command(flatten)]
	conn: DatabaseConnectionArguments,
	#[command(flatten)]
	json: json::JsonArg,
}

pub async fn init(
	IsReadyCommandArguments {
		conn: DatabaseConnectionArguments {
			endpoint,
		},
		json: json::JsonArg {
			json,
		},
	}: IsReadyCommandArguments,
) -> Result<()> {
	// Connect to the database engine
	connect(endpoint.as_str()).await.map_err(|error| {
		if json {
			json::network(error)
		} else {
			error.into()
		}
	})?;
	if json {
		return json::print(&json!({ "ready": true, "endpoint": endpoint }));
	}
	println!("OK");
	Ok(())
}
