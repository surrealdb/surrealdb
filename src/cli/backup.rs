use crate::cli::abstraction::AuthArguments;
use crate::cnf::SERVER_AGENT;
use crate::err::Error;
use clap::Args;
use futures::TryStreamExt;
use reqwest::header::CONTENT_TYPE;
use reqwest::header::USER_AGENT;
use reqwest::{Body, Client, Response};
use std::io::ErrorKind;
use tokio::fs::OpenOptions;
use tokio::io::{copy, stdin, stdout, AsyncWrite, AsyncWriteExt};
use tokio_util::io::{ReaderStream, StreamReader};

const TYPE: &str = "application/octet-stream";

#[derive(Args, Debug)]
pub struct BackupCommandArguments {
	#[arg(help = "Path to the remote database or file from which to export")]
	#[arg(value_parser = super::validator::into_valid)]
	from: String,
	#[arg(help = "Path to the remote database or file into which to import")]
	#[arg(default_value = "-")]
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
	let into_local = into.ends_with(".db");
	let from_local = from.ends_with(".db");
	match (from.as_str(), into.as_str()) {
		// From Stdin -> Into Stdout (are you trying to make an ouroboros?)
		("-", "-") => Err(Error::OperationUnsupported),
		// From Stdin -> Into File (possible but meaningless)
		("-", _) if into_local => Err(Error::OperationUnsupported),
		// From File -> Into Stdout (possible but meaningless, could be useful for source validation but not for now)
		(_, "-") if from_local => Err(Error::OperationUnsupported),
		// From File -> Into File (also possible but meaningless,
		// but since the original function had this, I would choose to keep it as of now)
		(from, into) if from_local && into_local => {
			tokio::fs::copy(from, into).await?;
			Ok(())
		}
		// From File -> Into HTTP
		(from, into) if from_local => {
			// Copy the data to the destination
			let from = OpenOptions::new().read(true).open(from).await?;
			post_http_sync_body(from, into, &user, &pass).await
		}
		// From HTTP -> Into File
		(from, into) if into_local => {
			// Try to open the output file
			let into =
				OpenOptions::new().write(true).create(true).truncate(true).open(into).await?;
			backup_http_to_file(from, into, &user, &pass).await
		}
		// From HTTP -> Into Stdout
		(from, "-") => backup_http_to_file(from, stdout(), &user, &pass).await,
		// From Stdin -> Into File
		("-", into) => {
			let from = Body::wrap_stream(ReaderStream::new(stdin()));
			post_http_sync_body(from, into, &user, &pass).await
		}
		// From HTTP -> Into HTTP
		(from, into) => {
			// Copy the data to the destination
			let from = get_http_sync_body(from, &user, &pass).await?;
			post_http_sync_body(from, into, &user, &pass).await
		}
	}
}

async fn post_http_sync_body<B: Into<Body>>(
	from: B,
	into: &str,
	user: &str,
	pass: &str,
) -> Result<(), Error> {
	Client::new()
		.post(format!("{into}/sync"))
		.basic_auth(user, Some(pass))
		.header(USER_AGENT, SERVER_AGENT)
		.header(CONTENT_TYPE, TYPE)
		.body(from)
		.send()
		.await?
		.error_for_status()?;
	Ok(())
}

async fn get_http_sync_body(from: &str, user: &str, pass: &str) -> Result<Response, Error> {
	Ok(Client::new()
		.get(format!("{from}/sync"))
		.basic_auth(user, Some(pass))
		.header(USER_AGENT, SERVER_AGENT)
		.header(CONTENT_TYPE, TYPE)
		.send()
		.await?
		.error_for_status()?)
}

async fn backup_http_to_file<W: AsyncWrite + Unpin>(
	from: &str,
	mut into: W,
	user: &str,
	pass: &str,
) -> Result<(), Error> {
	let mut from = StreamReader::new(
		get_http_sync_body(from, user, pass)
			.await?
			.bytes_stream()
			.map_err(|x| std::io::Error::new(ErrorKind::Other, x)),
	);

	// Copy the data to the destination
	copy(&mut from, &mut into).await?;
	into.flush().await?;
	// Everything OK
	Ok(())
}
