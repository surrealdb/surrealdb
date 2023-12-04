use super::abstraction::LevelSelectionArguments;
use crate::cli::abstraction::auth::{CredentialsBuilder, CredentialsLevel};
use crate::cli::abstraction::AuthArguments;
use crate::cnf::SERVER_AGENT;
use crate::err::Error;
use clap::Args;
use futures::TryStreamExt;
use reqwest::header::CONTENT_TYPE;
use reqwest::header::USER_AGENT;
use reqwest::RequestBuilder;
use reqwest::{Body, Client, Response};
use std::io::ErrorKind;
use surrealdb::headers::AUTH_DB;
use surrealdb::headers::AUTH_NS;
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
	#[command(flatten)]
	level: LevelSelectionArguments,
}

pub async fn init(
	BackupCommandArguments {
		from,
		into,
		auth,
		level,
	}: BackupCommandArguments,
) -> Result<(), Error> {
	// Initialize opentelemetry and logging
	crate::telemetry::builder().with_log_level("error").init();

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
			post_http_sync_body(from, into, &auth, &level).await
		}
		// From HTTP -> Into File
		(from, into) if into_local => {
			// Try to open the output file
			let into =
				OpenOptions::new().write(true).create(true).truncate(true).open(into).await?;
			backup_http_to_file(from, into, &auth, &level).await
		}
		// From HTTP -> Into Stdout
		(from, "-") => backup_http_to_file(from, stdout(), &auth, &level).await,
		// From Stdin -> Into File
		("-", into) => {
			let from = Body::wrap_stream(ReaderStream::new(stdin()));
			post_http_sync_body(from, into, &auth, &level).await
		}
		// From HTTP -> Into HTTP
		(from, into) => {
			// Copy the data to the destination
			let from = get_http_sync_body(from, &auth, &level).await?;
			post_http_sync_body(from, into, &auth, &level).await
		}
	}
}

async fn post_http_sync_body<B: Into<Body>>(
	from: B,
	into: &str,
	auth: &AuthArguments,
	level: &LevelSelectionArguments,
) -> Result<(), Error> {
	let mut req = Client::new()
		.post(format!("{into}/sync"))
		.header(USER_AGENT, SERVER_AGENT)
		.header(CONTENT_TYPE, TYPE)
		.body(from);

	// Add authentication if needed
	if auth.username.is_some() {
		req = req_with_creds(req, auth, level)?;
	}

	req.send().await?.error_for_status()?;
	Ok(())
}

async fn get_http_sync_body(
	from: &str,
	auth: &AuthArguments,
	level: &LevelSelectionArguments,
) -> Result<Response, Error> {
	let mut req = Client::new()
		.get(format!("{from}/sync"))
		.header(USER_AGENT, SERVER_AGENT)
		.header(CONTENT_TYPE, TYPE);

	// Add authentication if needed
	if auth.username.is_some() {
		req = req_with_creds(req, auth, level)?;
	}

	Ok(req.send().await?.error_for_status()?)
}

async fn backup_http_to_file<W: AsyncWrite + Unpin>(
	from: &str,
	mut into: W,
	auth: &AuthArguments,
	level: &LevelSelectionArguments,
) -> Result<(), Error> {
	let mut from = StreamReader::new(
		get_http_sync_body(from, auth, level)
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

fn req_with_creds(
	req: RequestBuilder,
	AuthArguments {
		username,
		password,
		auth_level,
	}: &AuthArguments,
	LevelSelectionArguments {
		namespace,
		database,
	}: &LevelSelectionArguments,
) -> Result<RequestBuilder, Error> {
	let builder = CredentialsBuilder::default()
		.with_username(username.as_deref())
		.with_password(password.as_deref())
		.with_namespace(namespace.as_deref())
		.with_database(database.as_deref());

	let req = match auth_level {
		CredentialsLevel::Root => {
			let creds = builder.root()?;
			req.basic_auth(creds.username, Some(creds.password))
		}
		CredentialsLevel::Namespace => {
			let creds = builder.namespace()?;
			req.header(&AUTH_NS, creds.namespace).basic_auth(creds.username, Some(creds.password))
		}
		CredentialsLevel::Database => {
			let creds = builder.database()?;
			req.header(&AUTH_NS, creds.namespace)
				.header(&AUTH_DB, creds.database)
				.basic_auth(creds.username, Some(creds.password))
		}
	};

	Ok(req)
}
