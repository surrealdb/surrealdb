//! HTTP engine

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod native;
#[cfg(target_arch = "wasm32")]
pub(crate) mod wasm;

use crate::api::conn::Command;
use crate::api::conn::DbResponse;
use crate::api::conn::RequestData;
use crate::api::conn::RouterRequest;
use crate::api::engine::remote::{deserialize, serialize};
use crate::api::err::Error;
use crate::api::Connect;
use crate::api::Result;
use crate::api::Surreal;
use crate::engine::remote::Response;
use crate::headers::AUTH_DB;
use crate::headers::AUTH_NS;
use crate::headers::DB;
use crate::headers::NS;
use crate::opt::IntoEndpoint;
use crate::sql::from_value;
use crate::sql::Value;
use futures::TryStreamExt;
use indexmap::IndexMap;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use reqwest::header::ACCEPT;
use reqwest::header::CONTENT_TYPE;
use reqwest::RequestBuilder;
use serde::Deserialize;
use serde::Serialize;
use std::marker::PhantomData;
use surrealdb_core::sql::Query;
use url::Url;

#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;
#[cfg(not(target_arch = "wasm32"))]
use tokio::fs::OpenOptions;
#[cfg(not(target_arch = "wasm32"))]
use tokio::io;
#[cfg(not(target_arch = "wasm32"))]
use tokio_util::compat::FuturesAsyncReadCompatExt;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;

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
	/// Connects to a specific database endpoint, saving the connection on the static client
	///
	/// # Examples
	///
	/// ```no_run
	/// use once_cell::sync::Lazy;
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::remote::http::Client;
	/// use surrealdb::engine::remote::http::Http;
	///
	/// static DB: Lazy<Surreal<Client>> = Lazy::new(Surreal::init);
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
			router: self.router.clone(),
			engine: PhantomData,
			address: address.into_endpoint(),
			capacity: 0,
			waiter: self.waiter.clone(),
			response_type: PhantomData,
		}
	}
}

pub(crate) fn default_headers() -> HeaderMap {
	let mut headers = HeaderMap::new();
	headers.insert(ACCEPT, HeaderValue::from_static("application/surrealdb"));
	headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/surrealdb"));
	headers
}

#[allow(dead_code)]
#[derive(Debug)]
enum Auth {
	Basic {
		user: String,
		pass: String,
		ns: Option<String>,
		db: Option<String>,
	},
	Bearer {
		token: String,
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
			}) => self.bearer_auth(token),
			None => self,
		}
	}
}

#[derive(Debug, Serialize, Deserialize)]
struct Credentials {
	user: String,
	pass: String,
	ns: Option<String>,
	db: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AuthResponse {
	code: u16,
	details: String,
	token: Option<String>,
}

type BackupSender = channel::Sender<Result<Vec<u8>>>;

#[cfg(not(target_arch = "wasm32"))]
async fn export_file(request: RequestBuilder, path: PathBuf) -> Result<Value> {
	let mut response = request
		.send()
		.await?
		.error_for_status()?
		.bytes_stream()
		.map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
		.into_async_read()
		.compat();
	let mut file =
		match OpenOptions::new().write(true).create(true).truncate(true).open(&path).await {
			Ok(path) => path,
			Err(error) => {
				return Err(Error::FileOpen {
					path,
					error,
				}
				.into());
			}
		};
	if let Err(error) = io::copy(&mut response, &mut file).await {
		return Err(Error::FileRead {
			path,
			error,
		}
		.into());
	}

	Ok(Value::None)
}

async fn export_bytes(request: RequestBuilder, bytes: BackupSender) -> Result<Value> {
	let response = request.send().await?.error_for_status()?;

	let future = async move {
		let mut response = response.bytes_stream();
		while let Ok(Some(b)) = response.try_next().await {
			if bytes.send(Ok(b.to_vec())).await.is_err() {
				break;
			}
		}
	};

	#[cfg(not(target_arch = "wasm32"))]
	tokio::spawn(future);

	#[cfg(target_arch = "wasm32")]
	spawn_local(future);

	Ok(Value::None)
}

#[cfg(not(target_arch = "wasm32"))]
async fn import(request: RequestBuilder, path: PathBuf) -> Result<Value> {
	let file = match OpenOptions::new().read(true).open(&path).await {
		Ok(path) => path,
		Err(error) => {
			return Err(Error::FileOpen {
				path,
				error,
			}
			.into());
		}
	};

	let res = request.header(ACCEPT, "application/octet-stream").body(file).send().await?;

	if res.error_for_status_ref().is_err() {
		let res = res.text().await?;

		match res.parse::<serde_json::Value>() {
			Ok(body) => {
				let error_msg = format!(
					"\n{}",
					serde_json::to_string_pretty(&body).unwrap_or_else(|_| "{}".into())
				);
				return Err(Error::Http(error_msg).into());
			}
			Err(_) => {
				return Err(Error::Http(res).into());
			}
		}
	}
	Ok(Value::None)
}

pub(crate) async fn health(request: RequestBuilder) -> Result<Value> {
	request.send().await?.error_for_status()?;
	Ok(Value::None)
}

async fn process_req(
	req: RouterRequest,
	base_url: &Url,
	client: &reqwest::Client,
	headers: &HeaderMap,
	auth: &Option<Auth>,
) -> Result<DbResponse> {
	let url = base_url.join(RPC_PATH).unwrap();
	let http_req =
		client.post(url).headers(headers.clone()).auth(auth).body(serialize(&req, false)?);
	let response = http_req.send().await?.error_for_status()?;
	let bytes = response.bytes().await?;

	let response: Response = deserialize(&mut &bytes[..], false)?;
	return DbResponse::from(response.result);
}

fn try_one(res: DbResponse, needed: bool) -> DbResponse {
	if !needed {
		return res;
	}
	match res {
		DbResponse::Other(Value::Array(arr)) if arr.len() == 1 => {
			DbResponse::Other(arr.into_iter().next().unwrap())
		}
		r => r,
	}
}

async fn router(
	RequestData {
		command,
		..
	}: RequestData,
	base_url: &Url,
	client: &reqwest::Client,
	headers: &mut HeaderMap,
	vars: &mut IndexMap<String, Value>,
	auth: &mut Option<Auth>,
) -> Result<DbResponse> {
	error!(?command, ?headers, ?vars, ?auth);
	match command {
		Command::Query {
			query,
			mut variables,
		} => {
			variables.extend(vars.clone());
			let req = Command::Query {
				query,
				variables,
			}
			.into_router_request(None)
			.expect("query should be valid request");
			process_req(req, base_url, client, headers, auth).await
		}
		ref cmd @ Command::Use {
			ref namespace,
			ref database,
		} => {
			let req = cmd
				.clone()
				.into_router_request(None)
				.expect("use should be a valid router request");
			// process request to check permissions
			let out = process_req(req, base_url, client, headers, auth).await?;
			match namespace {
				Some(ns) => match HeaderValue::try_from(ns) {
					Ok(ns) => {
						headers.insert(&NS, ns.into());
					}
					Err(_) => {
						return Err(Error::InvalidNsName(ns.to_owned()).into());
					}
				},
				None => {}
			};

			match database {
				Some(db) => match HeaderValue::try_from(db) {
					Ok(db) => {
						headers.insert(&DB, db.into());
					}
					Err(_) => {
						return Err(Error::InvalidDbName(db.to_owned()).into());
					}
				},
				None => {}
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

			let DbResponse::Other(value) =
				process_req(req, base_url, client, headers, auth).await?
			else {
				unreachable!("didn't make query")
			};

			if let Ok(Credentials {
				user,
				pass,
				ns,
				db,
			}) = from_value(credentials.into())
			{
				*auth = Some(Auth::Basic {
					user,
					pass,
					ns,
					db,
				});
			} else {
				*auth = Some(Auth::Bearer {
					token: value.to_raw_string(),
				});
			}

			Ok(DbResponse::Other(value))
		}
		Command::Authenticate {
			token,
		} => {
			let req = Command::Authenticate {
				token: token.clone(),
			}
			.into_router_request(None)
			.expect("authenticate should be a valid router request");
			process_req(req, base_url, client, headers, auth).await?;

			*auth = Some(Auth::Bearer {
				token,
			});
			Ok(DbResponse::Other(Value::None))
		}
		Command::Invalidate => {
			*auth = None;
			Ok(DbResponse::Other(Value::None))
		}
		Command::Set {
			key,
			value,
		} => {
			let query: Query = surrealdb_core::sql::parse(&format!("RETURN ${key};"))?;
			let req = Command::Query {
				query,
				variables: [(key.clone(), value)].into(),
			}
			.into_router_request(None)
			.expect("query is valid request");
			let DbResponse::Query(mut res) =
				process_req(req, base_url, client, headers, auth).await?
			else {
				unreachable!("made query request so response must be query")
			};

			let val: Value = res.take(0)?;

			vars.insert(key, val);
			Ok(DbResponse::Other(Value::None))
		}
		Command::Unset {
			key,
		} => {
			vars.shift_remove(&key);
			Ok(DbResponse::Other(Value::None))
		}
		#[cfg(target_arch = "wasm32")]
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

		#[cfg(not(target_arch = "wasm32"))]
		Command::ExportFile {
			path,
		} => {
			let req_path = base_url.join("export")?;
			let request = client
				.get(req_path)
				.headers(headers.clone())
				.auth(auth)
				.header(ACCEPT, "application/octet-stream");
			let value = export_file(request, path).await?;
			Ok(DbResponse::Other(value))
		}
		Command::ExportBytes {
			bytes,
		} => {
			let req_path = base_url.join("export")?;
			let request = client
				.get(req_path)
				.headers(headers.clone())
				.auth(auth)
				.header(ACCEPT, "application/octet-stream");
			let value = export_bytes(request, bytes).await?;
			Ok(DbResponse::Other(value))
		}
		#[cfg(not(target_arch = "wasm32"))]
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
			let value = export_file(request, path).await?;
			Ok(DbResponse::Other(value))
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
			let value = export_bytes(request, bytes).await?;
			Ok(DbResponse::Other(value))
		}
		#[cfg(not(target_arch = "wasm32"))]
		Command::ImportFile {
			path,
		} => {
			let req_path = base_url.join("import")?;
			let request = client
				.post(req_path)
				.headers(headers.clone())
				.auth(auth)
				.header(CONTENT_TYPE, "application/octet-stream");
			let value = import(request, path).await?;
			Ok(DbResponse::Other(value))
		}
		#[cfg(not(target_arch = "wasm32"))]
		Command::ImportMl {
			path,
		} => {
			let req_path = base_url.join("ml")?.join("import")?;
			let request = client
				.post(req_path)
				.headers(headers.clone())
				.auth(auth)
				.header(CONTENT_TYPE, "application/octet-stream");
			let value = import(request, path).await?;
			Ok(DbResponse::Other(value))
		}
		Command::SubscribeLive {
			..
		} => Err(Error::LiveQueriesNotSupported.into()),

		cmd => {
			let one = cmd.needs_one();
			let req = cmd
				.into_router_request(None)
				.expect("all invalid variants should have been caught");
			process_req(req, base_url, client, headers, auth).await.map(|r| try_one(r, one))
		}
	}
}
