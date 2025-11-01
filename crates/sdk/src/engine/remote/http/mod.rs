//! HTTP engine
use std::marker::PhantomData;
#[cfg(not(target_family = "wasm"))]
use std::path::PathBuf;

use futures::TryStreamExt;
use indexmap::IndexMap;
use reqwest::RequestBuilder;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use surrealdb_core::dbs::{QueryResult, QueryResultBuilder};
use surrealdb_core::iam::Token as CoreToken;
use surrealdb_core::rpc::{self, DbResponse, DbResult};
use surrealdb_types::{SurrealValue, Value, Variables};
#[cfg(not(target_family = "wasm"))]
use tokio::fs::OpenOptions;
#[cfg(not(target_family = "wasm"))]
use tokio::io;
#[cfg(not(target_family = "wasm"))]
use tokio_util::compat::FuturesAsyncReadCompatExt;
use url::Url;
#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local;

use crate::conn::cmd::RouterRequest;
use crate::conn::{Command, RequestData};
use crate::err::Error;
// use crate::engine::remote::Response;
use crate::headers::{AUTH_DB, AUTH_NS, DB, NS};
use crate::opt::IntoEndpoint;
use crate::opt::auth::{AccessToken, Token};
use crate::{Connect, Result, Surreal};

#[cfg(not(target_family = "wasm"))]
pub(crate) mod native;
#[cfg(target_family = "wasm")]
pub(crate) mod wasm;

// const SQL_PATH: &str = "sql";
const RPC_PATH: &str = "rpc";

// The HTTP scheme used to connect to `http://` endpoints
#[derive(Debug)]
pub struct Http;

/// The HTTPS scheme used to connect to `https://` endpoints
#[derive(Debug)]
pub struct Https;

/// An HTTP client for communicating with the server via HTTP
#[derive(Debug, Clone)]
pub struct Client(());

impl Surreal<Client> {
	/// Connects to a specific database endpoint, saving the connection on the
	/// static client
	///
	/// # Examples
	///
	/// ```no_run
	/// use std::sync::LazyLock;
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::remote::http::Client;
	/// use surrealdb::engine::remote::http::Http;
	///
	/// static DB: LazyLock<Surreal<Client>> = LazyLock::new(Surreal::init);
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// DB.connect::<Http>("localhost:8000").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn connect<P>(
		&self,
		address: impl IntoEndpoint<P, Client = Client>,
	) -> Connect<Client, ()> {
		Connect {
			surreal: self.inner.clone().into(),
			address: address.into_endpoint(),
			capacity: 0,
			response_type: PhantomData,
		}
	}
}

pub(crate) fn default_headers() -> HeaderMap {
	let mut headers = HeaderMap::new();
	headers.insert(ACCEPT, HeaderValue::from_static(surrealdb_core::api::format::FLATBUFFERS));
	headers
		.insert(CONTENT_TYPE, HeaderValue::from_static(surrealdb_core::api::format::FLATBUFFERS));
	headers
}

#[derive(Debug)]
enum Auth {
	Basic {
		user: String,
		pass: String,
		ns: Option<String>,
		db: Option<String>,
	},
	Bearer {
		token: AccessToken,
	},
}

trait Authenticate {
	fn auth(self, auth: &Option<Auth>) -> Self;
}

impl Authenticate for RequestBuilder {
	fn auth(self, auth: &Option<Auth>) -> Self {
		match auth {
			Some(Auth::Basic {
				user,
				pass,
				ns,
				db,
			}) => {
				let mut req = self.basic_auth(user, Some(pass));
				if let Some(ns) = ns {
					req = req.header(&AUTH_NS, ns);
				}
				if let Some(db) = db {
					req = req.header(&AUTH_DB, db);
				}
				req
			}
			Some(Auth::Bearer {
				token,
			}) => self.bearer_auth(token.as_insecure_token()),
			None => self,
		}
	}
}

#[derive(Debug, Serialize, Deserialize, SurrealValue)]
struct Credentials {
	user: String,
	pass: String,
	ac: Option<String>,
	ns: Option<String>,
	db: Option<String>,
}

#[derive(Debug, Deserialize)]
#[expect(dead_code)]
struct AuthResponse {
	code: u16,
	details: String,
	token: Option<Token>,
}

type BackupSender = async_channel::Sender<Result<Vec<u8>>>;

#[cfg(not(target_family = "wasm"))]
async fn export_file(request: RequestBuilder, path: PathBuf) -> Result<()> {
	let mut response = request
		.send()
		.await?
		.error_for_status()?
		.bytes_stream()
		.map_err(futures::io::Error::other)
		.into_async_read()
		.compat();
	let mut file =
		match OpenOptions::new().write(true).create(true).truncate(true).open(&path).await {
			Ok(path) => path,
			Err(error) => {
				return Err(Error::FileOpen {
					path,
					error,
				});
			}
		};
	if let Err(error) = io::copy(&mut response, &mut file).await {
		return Err(Error::FileRead {
			path,
			error,
		});
	}

	Ok(())
}

async fn export_bytes(request: RequestBuilder, bytes: BackupSender) -> Result<()> {
	let response = request.send().await?.error_for_status()?;

	let future = async move {
		let mut response = response.bytes_stream();
		while let Ok(Some(b)) = response.try_next().await {
			if bytes.send(Ok(b.to_vec())).await.is_err() {
				break;
			}
		}
	};

	#[cfg(not(target_family = "wasm"))]
	tokio::spawn(future);

	#[cfg(target_family = "wasm")]
	spawn_local(future);

	Ok(())
}

#[cfg(not(target_family = "wasm"))]
async fn import(request: RequestBuilder, path: PathBuf) -> Result<()> {
	let file = match OpenOptions::new().read(true).open(&path).await {
		Ok(path) => path,
		Err(error) => {
			return Err(Error::FileOpen {
				path,
				error,
			});
		}
	};

	let res =
		request.header(ACCEPT, surrealdb_core::api::format::FLATBUFFERS).body(file).send().await?;

	if res.error_for_status_ref().is_err() {
		let res = res.text().await?;

		match res.parse::<serde_json::Value>() {
			Ok(body) => {
				let error_msg = format!(
					"\n{}",
					serde_json::to_string_pretty(&body).unwrap_or_else(|_| "{}".into())
				);
				return Err(Error::Http(error_msg));
			}
			Err(_) => {
				return Err(Error::Http(res));
			}
		}
	}

	let bytes = res.bytes().await?;

	let value: Value = surrealdb_core::rpc::format::flatbuffers::decode(&bytes)
		.map_err(|x| format!("Failed to deserialize flatbuffers payload: {x:?}"))
		.map_err(crate::Error::InvalidResponse)?;

	// Convert Value::Array to Vec<QueryResult>
	let Value::Array(arr) = value else {
		return Err(Error::InvalidResponse("Expected array response from import".to_string()));
	};

	for val in arr.into_vec() {
		let result = QueryResult::from_value(val)
			.map_err(|e| Error::InvalidResponse(format!("Failed to parse query result: {}", e)))?;
		if let Err(e) = result.result {
			return Err(Error::Query(e.to_string()));
		}
	}

	Ok(())
}

pub(crate) async fn health(request: RequestBuilder) -> Result<()> {
	request.send().await?.error_for_status()?;
	Ok(())
}

async fn send_request(
	req: RouterRequest,
	base_url: &Url,
	client: &reqwest::Client,
	headers: &HeaderMap,
	auth: &Option<Auth>,
) -> Result<Vec<QueryResult>> {
	let url = base_url.join(RPC_PATH).expect("valid RPC path");

	let req_value = req.into_value();
	let body = surrealdb_core::rpc::format::flatbuffers::encode(&req_value)
		.map_err(|x| format!("Failed to serialize to flatbuffers: {x}"))
		.map_err(crate::Error::UnserializableValue)?;

	let http_req = client.post(url).headers(headers.clone()).auth(auth).body(body);
	let response = http_req.send().await?.error_for_status()?;
	let bytes = response.bytes().await?;

	let response: DbResponse = surrealdb_core::rpc::format::flatbuffers::decode(&bytes)
		.map_err(|x| format!("Failed to deserialize flatbuffers payload: {x}"))
		.map_err(crate::Error::InvalidResponse)?;

	match response.result? {
		DbResult::Query(results) => Ok(results),
		DbResult::Other(value) => {
			Ok(vec![QueryResultBuilder::started_now().finish_with_result(Ok(value))])
		}
		DbResult::Live(notification) => Ok(vec![
			QueryResultBuilder::started_now().finish_with_result(Ok(notification.into_value())),
		]),
	}
}

async fn refresh_token(
	token: CoreToken,
	base_url: &Url,
	client: &reqwest::Client,
	headers: &HeaderMap,
	auth: &Option<Auth>,
) -> Result<(Value, Vec<QueryResult>)> {
	let req = Command::Refresh {
		token,
	}
	.into_router_request(None)
	.expect("refresh should be a valid router request");
	let results = send_request(req, base_url, client, headers, auth).await?;
	let value = match results.first() {
		Some(result) => result.clone().result?,
		None => {
			error!("received invalid result from server");
			return Err(Error::InternalError("Received invalid result from server".to_string()));
		}
	};
	Ok((value, results))
}

async fn router(
	req: RequestData,
	base_url: &Url,
	client: &reqwest::Client,
	headers: &mut HeaderMap,
	vars: &mut IndexMap<String, Value>,
	auth: &mut Option<Auth>,
) -> Result<Vec<QueryResult>> {
	match req.command {
		Command::Use {
			namespace,
			database,
		} => {
			let req = Command::Use {
				namespace: namespace.clone(),
				database: database.clone(),
			}
			.into_router_request(None)
			.expect("USE command should convert to router request");
			// process request to check permissions
			let out = send_request(req, base_url, client, headers, auth).await?;
			if let Some(ns) = namespace {
				let value =
					HeaderValue::try_from(&ns).map_err(|_| Error::InvalidNsName(ns.clone()))?;
				headers.insert(&NS, value);
			};
			if let Some(db) = database {
				let value =
					HeaderValue::try_from(&db).map_err(|_| Error::InvalidDbName(db.clone()))?;
				headers.insert(&DB, value);
			};

			Ok(out)
		}
		Command::Signin {
			credentials,
		} => {
			let req = Command::Signin {
				credentials: credentials.clone(),
			}
			.into_router_request(None)
			.expect("signin should be a valid router request");

			let results = send_request(req, base_url, client, headers, auth).await?;

			let value = match results.first() {
				Some(result) => result.clone().result?,
				None => {
					error!("received invalid result from server");
					return Err(Error::InternalError(
						"Received invalid result from server".to_string(),
					));
				}
			};

			match Credentials::from_value(value.clone()) {
				Ok(credentials) => {
					*auth = Some(Auth::Basic {
						user: credentials.user,
						pass: credentials.pass,
						ns: credentials.ns,
						db: credentials.db,
					});
				}
				Err(err) => {
					debug!("Error converting Value to Credentials: {err}");
					let token = Token::from_value(value)?;
					*auth = Some(Auth::Bearer {
						token: token.access,
					});
				}
			}

			Ok(results)
		}
		Command::Authenticate {
			token,
		} => {
			let req = Command::Authenticate {
				token: token.clone(),
			}
			.into_router_request(None)
			.expect("authenticate should be a valid router request");
			let mut results = send_request(req, base_url, client, headers, auth).await?;
			if let Some(result) = results.first_mut() {
				match &mut result.result {
					Ok(result) => {
						let value = token.into_value();
						*auth = Some(Auth::Bearer {
							token: Token::from_value(value.clone())?.access,
						});
						*result = value;
					}
					Err(error) => {
						// Automatic refresh token handling:
						// If authentication fails with "token has expired" and we have a refresh
						// token, automatically attempt to refresh the authentication and
						// update the stored auth.
						if let CoreToken::WithRefresh {
							..
						} = &token
						{
							// If the error is due to token expiration, attempt automatic refresh
							if error.to_string().contains("token has expired") {
								// Call the refresh_token helper to get new tokens
								let (value, refresh_results) =
									refresh_token(token, base_url, client, headers, auth).await?;
								// Update the stored authentication with the new access token
								*auth = Some(Auth::Bearer {
									token: Token::from_value(value)?.access,
								});
								// Use the refresh results (which include the new token)
								results = refresh_results;
							}
						}
					}
				}
			}
			Ok(results)
		}
		Command::Refresh {
			token,
		} => {
			let (value, results) = refresh_token(token, base_url, client, headers, auth).await?;
			*auth = Some(Auth::Bearer {
				token: Token::from_value(value)?.access,
			});
			Ok(results)
		}
		Command::Invalidate => {
			*auth = None;
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		Command::Set {
			key,
			value,
		} => {
			surrealdb_core::rpc::check_protected_param(&key)?;
			vars.insert(key, value);
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		Command::Unset {
			key,
		} => {
			vars.shift_remove(&key);
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		#[cfg(target_family = "wasm")]
		Command::ExportFile {
			..
		}
		| Command::ExportMl {
			..
		}
		| Command::ImportFile {
			..
		}
		| Command::ImportMl {
			..
		} => {
			// TODO: Better error message here, some backups are supported
			Err(Error::BackupsNotSupported.into())
		}

		#[cfg(not(target_family = "wasm"))]
		Command::ExportFile {
			path,
			config,
		} => {
			let req_path = base_url.join("export")?;
			let config = config.unwrap_or_default();
			let config_value: Value = config.into_value();
			let request = client
				.post(req_path)
				.body(
					rpc::format::json::encode_str(config_value)
						.map_err(|e| Error::SerializeValue(e.to_string()))?,
				)
				.headers(headers.clone())
				.auth(auth)
				.header(CONTENT_TYPE, "application/json")
				.header(ACCEPT, "application/octet-stream");
			export_file(request, path).await?;
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		Command::ExportBytes {
			bytes,
			config,
		} => {
			let req_path = base_url.join("export")?;
			let config = config.unwrap_or_default();
			let config_value = config.into_value();
			let request = client
				.post(req_path)
				.body(
					rpc::format::json::encode_str(config_value)
						.map_err(|e| Error::SerializeValue(e.to_string()))?,
				)
				.headers(headers.clone())
				.auth(auth)
				.header(CONTENT_TYPE, "application/json")
				.header(ACCEPT, "application/octet-stream");
			export_bytes(request, bytes).await?;
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		#[cfg(not(target_family = "wasm"))]
		Command::ExportMl {
			path,
			config,
		} => {
			let req_path =
				base_url.join("ml")?.join("export")?.join(&config.name)?.join(&config.version)?;
			let request = client
				.get(req_path)
				.headers(headers.clone())
				.auth(auth)
				.header(ACCEPT, "application/octet-stream");
			export_file(request, path).await?;
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		Command::ExportBytesMl {
			bytes,
			config,
		} => {
			let req_path =
				base_url.join("ml")?.join("export")?.join(&config.name)?.join(&config.version)?;
			let request = client
				.get(req_path)
				.headers(headers.clone())
				.auth(auth)
				.header(ACCEPT, "application/octet-stream");
			export_bytes(request, bytes).await?;
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		#[cfg(not(target_family = "wasm"))]
		Command::ImportFile {
			path,
		} => {
			let req_path = base_url.join("import")?;
			let request = client
				.post(req_path)
				.headers(headers.clone())
				.auth(auth)
				.header(CONTENT_TYPE, "application/octet-stream");
			import(request, path).await?;
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		#[cfg(not(target_family = "wasm"))]
		Command::ImportMl {
			path,
		} => {
			let req_path = base_url.join("ml")?.join("import")?;
			let request = client
				.post(req_path)
				.headers(headers.clone())
				.auth(auth)
				.header(CONTENT_TYPE, "application/octet-stream");
			import(request, path).await?;
			Ok(vec![QueryResultBuilder::instant_none()])
		}
		Command::SubscribeLive {
			..
		} => Err(Error::LiveQueriesNotSupported),
		Command::Query {
			txn,
			query,
			variables,
		} => {
			// Merge stored vars with query vars
			let mut merged_vars =
				vars.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<Variables>();
			merged_vars.extend(variables);
			let cmd = Command::Query {
				txn,
				query,
				variables: merged_vars,
			};
			let req =
				cmd.into_router_request(None).expect("command should convert to router request");
			let res = send_request(req, base_url, client, headers, auth).await?;
			Ok(res)
		}
		cmd => {
			let req =
				cmd.into_router_request(None).expect("command should convert to router request");
			let res = send_request(req, base_url, client, headers, auth).await?;
			Ok(res)
		}
	}
}
