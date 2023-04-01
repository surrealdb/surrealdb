use crate::cli::abstraction::AuthArguments;
use crate::cnf::SERVER_AGENT;
use crate::err::Error;
use clap::Args;
use futures::TryStreamExt;
use reqwest::header::CONTENT_TYPE;
use reqwest::header::USER_AGENT;
use reqwest::Client;
use std::io::ErrorKind;
use tokio::fs::OpenOptions;
use tokio::io::copy;
use tokio_util::io::StreamReader;

const TYPE: &str = "application/octet-stream";

#[derive(Args, Debug)]
pub struct BackupCommandArguments {
	#[arg(help = "Path to the remote database or file from which to export")]
	#[arg(value_parser = super::validator::into_valid)]
	from: String,
	#[arg(help = "Path to the remote database or file into which to import")]
	#[arg(value_parser = super::validator::into_valid)]
	into: String,
	#[command(flatten)]
	auth: AuthArguments,
}

pub async fn init(
	BackupCommandArguments {
		from,
		into,
		auth: AuthArguments {
			username: user,
			password: pass,
		},
	}: BackupCommandArguments,
) -> Result<(), Error> {
	// Initialize opentelemetry and logging
	crate::o11y::builder().with_log_level("error").init();

	// Process the source->destination response
	match (from.ends_with(".db"), into.ends_with(".db")) {
		// From File -> Into File
		(true, true) => {
			let mut from = OpenOptions::new().read(true).open(&from).await?;
			// Try to open the output file
			let mut into =
				OpenOptions::new().write(true).create(true).truncate(true).open(&into).await?;
			// Copy the data to the destination
			copy(&mut from, &mut into).await?;
			// Everything OK
			Ok(())
		}
		// From File -> Into HTTP
		(true, _) => {
			let from = OpenOptions::new().read(true).open(&from).await?;
			// Set the correct output URL
			// Copy the data to the destination
			Client::new()
				.post(format!("{into}/sync"))
				.basic_auth(&user, Some(&pass))
				.header(USER_AGENT, SERVER_AGENT)
				.header(CONTENT_TYPE, TYPE)
				.body(from)
				.send()
				.await?
				.error_for_status()?;
			// Everything OK
			Ok(())
		}
		// From HTTP -> Into File
		(_, true) => {
			let from = Client::new()
				.get(format!("{from}/sync"))
				.basic_auth(&user, Some(&pass))
				.header(USER_AGENT, SERVER_AGENT)
				.header(CONTENT_TYPE, TYPE)
				.send()
				.await?
				.error_for_status()?
				.bytes_stream()
				.map_err(|x| std::io::Error::new(ErrorKind::Other, x));

			let mut from = StreamReader::new(from);

			// Try to open the output file
			let mut into =
				OpenOptions::new().write(true).create(true).truncate(true).open(&into).await?;
			// Copy the data to the destination
			copy(&mut from, &mut into).await?;
			// Everything OK
			Ok(())
		}
		// From HTTP -> Into HTTP
		(_, _) => {
			// Try to open the source file
			let from = Client::new()
				.get(format!("{from}/sync"))
				.basic_auth(&user, Some(&pass))
				.header(USER_AGENT, SERVER_AGENT)
				.header(CONTENT_TYPE, TYPE)
				.send()
				.await?
				.error_for_status()?;
			// Copy the data to the destination
			Client::new()
				.post(format!("{into}/sync"))
				.basic_auth(&user, Some(&pass))
				.header(USER_AGENT, SERVER_AGENT)
				.header(CONTENT_TYPE, TYPE)
				.body(from)
				.send()
				.await?
				.error_for_status()?;
			// Everything OK
			Ok(())
		}
	}
}
