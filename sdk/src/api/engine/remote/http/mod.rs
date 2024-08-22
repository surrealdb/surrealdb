//! HTTP engine
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
use crate::Value;
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
use surrealdb_core::sql::{
	from_value as from_core_value, statements::OutputStatement, Object as CoreObject, Param, Query,
	Statement, Value as CoreValue,
};
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

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod native;
#[cfg(target_arch = "wasm32")]
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
async fn export_file(request: RequestBuilder, path: PathBuf) -> Result<()> {
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

	#[cfg(not(target_arch = "wasm32"))]
	tokio::spawn(future);

	#[cfg(target_arch = "wasm32")]
	spawn_local(future);

	Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
async fn import(request: RequestBuilder, path: PathBuf) -> Result<()> {
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
	let http_req =
		client.post(url).headers(headers.clone()).auth(auth).body(serialize(&req, false)?);
	let response = http_req.send().await?.error_for_status()?;
	let bytes = response.bytes().await?;

	let response: Response = deserialize(&bytes, false)?;
	DbResponse::from_server_result(response.result)
}

fn flatten_dbresponse_array(res: DbResponse) -> DbResponse {
	match res {
		DbResponse::Other(CoreValue::Array(array)) if array.len() == 1 => {
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
	vars: &mut IndexMap<String, CoreValue>,
	auth: &mut Option<Auth>,
) -> Result<DbResponse> {
	match req.command {
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
					HeaderValue::try_from(&ns).map_err(|_| Error::InvalidNsName(ns.to_owned()))?;
				headers.insert(&NS, value);
			};
			if let Some(db) = database {
				let value =
					HeaderValue::try_from(&db).map_err(|_| Error::InvalidDbName(db.to_owned()))?;
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

			if let Ok(Credentials {
				user,
				pass,
				ns,
				db,
			}) = from_core_value(credentials.into())
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
			send_request(req, base_url, client, headers, auth).await?;

			*auth = Some(Auth::Bearer {
				token,
			});
			Ok(DbResponse::Other(CoreValue::None))
		}
		Command::Invalidate => {
			*auth = None;
			Ok(DbResponse::Other(CoreValue::None))
		}
		Command::Set {
			key,
			value,
		} => {
			let mut output_stmt = OutputStatement::default();
			output_stmt.what = CoreValue::Param(Param::from(key.clone()));
			let query = Query::from(Statement::Output(output_stmt));
			let mut variables = CoreObject::default();
			variables.insert(key.clone(), value);
			let req = Command::Query {
				query,
				variables,
			}
			.into_router_request(None)
			.expect("query is valid request");
			let DbResponse::Query(mut res) =
				send_request(req, base_url, client, headers, auth).await?
			else {
				return Err(Error::InternalError(
					"recieved invalid result from server".to_string(),
				)
				.into());
			};

			let val: Value = res.take(0)?;
			vars.insert(key, val.0);
			Ok(DbResponse::Other(CoreValue::None))
		}
		Command::Unset {
			key,
		} => {
			vars.shift_remove(&key);
			Ok(DbResponse::Other(CoreValue::None))
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
			export_file(request, path).await?;
			Ok(DbResponse::Other(CoreValue::None))
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
			export_bytes(request, bytes).await?;
			Ok(DbResponse::Other(CoreValue::None))
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
			export_file(request, path).await?;
			Ok(DbResponse::Other(CoreValue::None))
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
			Ok(DbResponse::Other(CoreValue::None))
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
			import(request, path).await?;
			Ok(DbResponse::Other(CoreValue::None))
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
			import(request, path).await?;
			Ok(DbResponse::Other(CoreValue::None))
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
