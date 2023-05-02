use crate::err::Error;
use surrealdb::engine::any::connect;

#[tokio::main]
pub async fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
	// Initialize opentelemetry and logging
	crate::o11y::builder().with_log_level("error").init();
	// Parse all other cli arguments
	let endpoint = matches.value_of("conn").unwrap();
	// Connect to the database engine
	connect(endpoint).await?;
	println!("OK");
	Ok(())
}
