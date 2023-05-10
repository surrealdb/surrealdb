use super::config;
use crate::cnf::LOGO;
use crate::dbs;
use crate::env;
use crate::err::Error;
use crate::iam;
use crate::net;
use futures::Future;

pub fn init(matches: &clap::ArgMatches) -> Result<(), Error> {
	with_enough_stack(init_impl(matches))
}

async fn init_impl(matches: &clap::ArgMatches) -> Result<(), Error> {
	// Initialize opentelemetry and logging
	crate::o11y::builder().with_log_level(matches.get_one::<String>("log").unwrap()).init();
	// Check if a banner should be outputted
	if !matches.is_present("no-banner") {
		// Output SurrealDB logo
		println!("{LOGO}");
	}
	// Setup the cli options
	config::init(matches);
	// Initiate environment
	env::init().await?;
	// Initiate master auth
	iam::init().await?;
	// Start the kvs server
	dbs::init().await?;
	// Start the web server
	net::init().await?;
	// All ok
	Ok(())
}

/// Rust's default thread stack size of 2MiB doesn't allow sufficient recursion depth.
fn with_enough_stack<T>(fut: impl Future<Output = T> + Send) -> T {
	let stack_size = 8 * 1024 * 1024;

	// Stack frames are generally larger in debug mode.
	#[cfg(debug_assertions)]
	let stack_size = stack_size * 2;

	tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.thread_stack_size(stack_size)
		.build()
		.unwrap()
		.block_on(fut)
}
