//! HTTP engine
use std::marker::PhantomData;
#[cfg(not(target_family = "wasm"))]
use std::path::PathBuf;

use futures::TryStreamExt;
use indexmap::IndexMap;
use reqwest::RequestBuilder;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
#[cfg(not(target_family = "wasm"))]
use tokio::fs::OpenOptions;
#[cfg(not(target_family = "wasm"))]
use tokio::io;
#[cfg(not(target_family = "wasm"))]
use tokio_util::compat::FuturesAsyncReadCompatExt;
use url::Url;
#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local;

use crate::api;
use crate::api::conn::{Command, DbResponse, RequestData, RouterRequest};
use crate::api::err::Error;
use crate::api::{Connect, Result, Surreal};
use crate::core::{rpc, val};
use crate::engine::remote::Response;
use crate::headers::{AUTH_DB, AUTH_NS, DB, NS};
use crate::opt::IntoEndpoint;

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
	headers.insert(ACCEPT, HeaderValue::from_static("application/surrealdb"));
	headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/surrealdb"));
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
#[expect(dead_code)]
struct AuthResponse {
	code: u16,
	details: String,
	token: Option<String>,
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
	use crate::engine::proto::{QueryMethodResponse, Status};

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

	let res = request.header(ACCEPT, "application/surrealdb").body(file).send().await?;

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
	} else {
		let response: Vec<QueryMethodResponse> =
			crate::core::rpc::format::bincode::decode(&res.bytes().await?)
				.map_err(|x| format!("Failed to deserialize bincode payload: {x}"))
				.map_err(crate::api::Error::InvalidResponse)?;
		for res in response {
			if let Status::Err = res.status {
				return Err(Error::Query(res.result.0.as_raw_string()).into());
			}
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
) -> Result<DbResponse> {
	let url = base_url.join(RPC_PATH).unwrap();

	let body = crate::core::rpc::format::bincode::encode(&req)
		.map_err(|x| format!("Failed to serialized to bincode: {x}"))
		.map_err(crate::api::Error::UnserializableValue)?;

	let http_req = client.post(url).headers(headers.clone()).auth(auth).body(body);
	let response = http_req.send().await?.error_for_status()?;
	let bytes = response.bytes().await?;

	let response: Response = crate::core::rpc::format::bincode::decode(&bytes)
		.map_err(|x| format!("Failed to deserialize bincode payload: {x}"))
		.map_err(crate::api::Error::InvalidResponse)?;

	DbResponse::from_server_result(response.result)
}

fn flatten_dbresponse_array(res: DbResponse) -> DbResponse {
	match res {
		DbResponse::Other(val::Value::Array(array)) if array.len() == 1 => {
			let v = array.into_iter().next().unwrap();
			DbResponse::Other(v)
		}
		x => x,
	}
}

async fn router(
	req: RequestData,
	base_url: &Url,
	client: &reqwest::Client,
	headers: &mut HeaderMap,
	vars: &mut IndexMap<String, val::Value>,
	auth: &mut Option<Auth>,
) -> Result<DbResponse> {
	match req.command {
		Command::Query {
			txn,
			query,
			mut variables,
		} => {
			variables.extend(vars.clone());
			let req = Command::Query {
				txn,
				query,
				variables,
			}
			.into_router_request(None)
			.expect("query should be valid request");
			send_request(req, base_url, client, headers, auth).await
		}
		Command::Use {
			namespace,
			database,
		} => {
			let req = Command::Use {
				namespace: namespace.clone(),
				database: database.clone(),
			}
			.into_router_request(None)
			.unwrap();
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

			let DbResponse::Other(value) =
				send_request(req, base_url, client, headers, auth).await?
			else {
				return Err(Error::InternalError(
					"recieved invalid result from server".to_string(),
				)
				.into());
			};

			match api::value::from_core_value(credentials.into()) {
				Ok(Credentials {
					user,
					pass,
					ns,
					db,
				}) => {
					*auth = Some(Auth::Basic {
						user,
						pass,
						ns,
						db,
					});
				}
				_ => {
					*auth = Some(Auth::Bearer {
						token: value.to_raw_string(),
					});
				}
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
			send_request(req, base_url, client, headers, auth).await?;

			*auth = Some(Auth::Bearer {
				token,
			});
			Ok(DbResponse::Other(val::Value::None))
		}
		Command::Invalidate => {
			*auth = None;
			Ok(DbResponse::Other(val::Value::None))
		}
		Command::Set {
			key,
			value,
		} => {
			crate::core::rpc::check_protected_param(&key)?;
			vars.insert(key, value);
			Ok(DbResponse::Other(val::Value::None))
		}
		Command::Unset {
			key,
		} => {
			vars.shift_remove(&key);
			Ok(DbResponse::Other(val::Value::None))
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
			let config_value: val::Value = config.into();
			let request = client
				.post(req_path)
				.body(rpc::format::json::encode_str(config_value).map_err(anyhow::Error::msg)?)
				.headers(headers.clone())
				.auth(auth)
				.header(CONTENT_TYPE, "application/json")
				.header(ACCEPT, "application/octet-stream");
			export_file(request, path).await?;
			Ok(DbResponse::Other(val::Value::None))
		}
		Command::ExportBytes {
			bytes,
			config,
		} => {
			let req_path = base_url.join("export")?;
			let config = config.unwrap_or_default();
			let config_value: val::Value = config.into();
			let request = client
				.post(req_path)
				.body(rpc::format::json::encode_str(config_value).map_err(anyhow::Error::msg)?)
				.headers(headers.clone())
				.auth(auth)
				.header(CONTENT_TYPE, "application/json")
				.header(ACCEPT, "application/octet-stream");
			export_bytes(request, bytes).await?;
			Ok(DbResponse::Other(val::Value::None))
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
			Ok(DbResponse::Other(val::Value::None))
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
			Ok(DbResponse::Other(val::Value::None))
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
			Ok(DbResponse::Other(val::Value::None))
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
			Ok(DbResponse::Other(val::Value::None))
		}
		Command::SubscribeLive {
			..
		} => Err(Error::LiveQueriesNotSupported.into()),
		cmd => {
			let needs_flatten = cmd.needs_flatten();
			let req = cmd.into_router_request(None).unwrap();
			let mut res = send_request(req, base_url, client, headers, auth).await?;
			if needs_flatten {
				res = flatten_dbresponse_array(res);
			}
			Ok(res)
		}
	}
}
