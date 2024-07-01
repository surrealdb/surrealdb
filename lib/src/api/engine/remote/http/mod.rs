//! HTTP engine

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod native;
#[cfg(target_arch = "wasm32")]
pub(crate) mod wasm;

use crate::api::conn::DbResponse;
use crate::api::conn::Method;
#[cfg(feature = "ml")]
#[cfg(not(target_arch = "wasm32"))]
use crate::api::conn::MlConfig;
use crate::api::conn::Param;
use crate::api::engine::create_statement;
use crate::api::engine::delete_statement;
use crate::api::engine::insert_statement;
use crate::api::engine::merge_statement;
use crate::api::engine::patch_statement;
use crate::api::engine::remote::duration_from_str;
use crate::api::engine::select_statement;
use crate::api::engine::update_statement;
use crate::api::engine::upsert_statement;
use crate::api::err::Error;
use crate::api::method::query::QueryResult;
use crate::api::Connect;
use crate::api::Response as QueryResponse;
use crate::api::Result;
use crate::api::Surreal;
use crate::dbs::Status;
use crate::headers::AUTH_DB;
use crate::headers::AUTH_NS;
use crate::headers::DB;
use crate::headers::NS;
use crate::method::Stats;
use crate::opt::IntoEndpoint;
use crate::sql::from_value;
use crate::sql::serde::deserialize;
use crate::Value;
#[cfg(not(target_arch = "wasm32"))]
use futures::TryStreamExt;
use indexmap::IndexMap;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use reqwest::header::ACCEPT;
#[cfg(not(target_arch = "wasm32"))]
use reqwest::header::CONTENT_TYPE;
use reqwest::RequestBuilder;
use serde::Deserialize;
use serde::Serialize;
use std::marker::PhantomData;
use std::mem;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;
#[cfg(not(target_arch = "wasm32"))]
use tokio::fs::OpenOptions;
#[cfg(not(target_arch = "wasm32"))]
use tokio::io;
#[cfg(not(target_arch = "wasm32"))]
use tokio_util::compat::FuturesAsyncReadCompatExt;
use url::Url;

const SQL_PATH: &str = "sql";

/// The HTTP scheme used to connect to `http://` endpoints
#[derive(Debug)]
pub struct Http;

/// The HTTPS scheme used to connect to `https://` endpoints
#[derive(Debug)]
pub struct Https;

/// An HTTP client for communicating with the server via HTTP
#[derive(Debug, Clone)]
pub struct Client {
	method: Method,
}

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

type HttpQueryResponse = (String, Status, Value);

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

async fn submit_auth(request: RequestBuilder) -> Result<Value> {
	let response = request.send().await?.error_for_status()?;
	let bytes = response.bytes().await?;
	let response: AuthResponse =
		deserialize(&bytes).map_err(|error| Error::ResponseFromBinary {
			binary: bytes.to_vec(),
			error,
		})?;
	Ok(response.token.into())
}

async fn query(request: RequestBuilder) -> Result<QueryResponse> {
	let response = request.send().await?.error_for_status()?;
	let bytes = response.bytes().await?;
	let responses = deserialize::<Vec<HttpQueryResponse>>(&bytes).map_err(|error| {
		Error::ResponseFromBinary {
			binary: bytes.to_vec(),
			error,
		}
	})?;
	let mut map = IndexMap::<usize, (Stats, QueryResult)>::with_capacity(responses.len());
	for (index, (execution_time, status, value)) in responses.into_iter().enumerate() {
		let stats = Stats {
			execution_time: duration_from_str(&execution_time),
		};
		match status {
			Status::Ok => {
				map.insert(index, (stats, Ok(value)));
			}
			Status::Err => {
				map.insert(index, (stats, Err(Error::Query(value.as_raw_string()).into())));
			}
			_ => unreachable!(),
		}
	}

	Ok(QueryResponse {
		results: map,
		..QueryResponse::new()
	})
}

async fn take(one: bool, request: RequestBuilder) -> Result<Value> {
	if let Some((_stats, result)) = query(request).await?.results.swap_remove(&0) {
		let value = result?;
		match one {
			true => match value {
				Value::Array(mut vec) => {
					if let [value] = &mut vec.0[..] {
						return Ok(mem::take(value));
					}
				}
				Value::None | Value::None => {}
				value => return Ok(value),
			},
			false => return Ok(value),
		}
	}
	match one {
		true => Ok(Value::None),
		false => Ok(Value::Array(Default::default())),
	}
}

#[cfg(not(target_arch = "wasm32"))]
type BackupSender = channel::Sender<Result<Vec<u8>>>;

#[cfg(not(target_arch = "wasm32"))]
async fn export(
	request: RequestBuilder,
	(file, sender): (Option<PathBuf>, Option<BackupSender>),
) -> Result<Value> {
	match (file, sender) {
		(Some(path), None) => {
			let mut response = request
				.send()
				.await?
				.error_for_status()?
				.bytes_stream()
				.map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
				.into_async_read()
				.compat();
			let mut file = match OpenOptions::new()
				.write(true)
				.create(true)
				.truncate(true)
				.open(&path)
				.await
			{
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
		}
		(None, Some(tx)) => {
			let mut response = request.send().await?.error_for_status()?.bytes_stream();

			tokio::spawn(async move {
				while let Ok(Some(bytes)) = response.try_next().await {
					if tx.send(Ok(bytes.to_vec())).await.is_err() {
						break;
					}
				}
			});
		}
		_ => unreachable!(),
	}

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

async fn version(request: RequestBuilder) -> Result<Value> {
	let response = request.send().await?.error_for_status()?;
	let version = response.text().await?;
	Ok(version.into())
}

pub(crate) async fn health(request: RequestBuilder) -> Result<Value> {
	request.send().await?.error_for_status()?;
	Ok(Value::None)
}

async fn router(
	(_, method, param): (i64, Method, Param),
	base_url: &Url,
	client: &reqwest::Client,
	headers: &mut HeaderMap,
	vars: &mut IndexMap<String, String>,
	auth: &mut Option<Auth>,
) -> Result<DbResponse> {
	let mut params = param.other;

	match method {
		Method::Use => {
			let path = base_url.join(SQL_PATH)?;
			let mut request = client.post(path).headers(headers.clone());
			let (ns, db) = match &mut params[..] {
				[Value::String(ns), Value::String(db)] => {
					(Some(mem::take(ns)), Some(mem::take(db)))
				}
				[Value::String(ns), Value::None] => (Some(mem::take(ns)), None),
				[Value::None, Value::String(db)] => (None, Some(mem::take(db))),
				_ => unreachable!(),
			};
			let ns = match ns {
				Some(ns) => match HeaderValue::try_from(&ns) {
					Ok(ns) => {
						request = request.header(&NS, &ns);
						Some(ns)
					}
					Err(_) => {
						return Err(Error::InvalidNsName(ns).into());
					}
				},
				None => None,
			};
			let db = match db {
				Some(db) => match HeaderValue::try_from(&db) {
					Ok(db) => {
						request = request.header(&DB, &db);
						Some(db)
					}
					Err(_) => {
						return Err(Error::InvalidDbName(db).into());
					}
				},
				None => None,
			};
			request = request.auth(auth).body("RETURN true");
			take(true, request).await?;
			if let Some(ns) = ns {
				headers.insert(&NS, ns);
			}
			if let Some(db) = db {
				headers.insert(&DB, db);
			}
			Ok(DbResponse::Other(Value::None))
		}
		Method::Signin => {
			let path = base_url.join(Method::Signin.as_str())?;
			let credentials = match &mut params[..] {
				[credentials] => credentials.to_string(),
				_ => unreachable!(),
			};
			let request = client.post(path).headers(headers.clone()).auth(auth).body(credentials);
			let value = submit_auth(request).await?;
			if let [credentials] = &mut params[..] {
				if let Ok(Credentials {
					user,
					pass,
					ns,
					db,
				}) = from_value(mem::take(credentials))
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
			}
			Ok(DbResponse::Other(value))
		}
		Method::Signup => {
			let path = base_url.join(Method::Signup.as_str())?;
			let credentials = match &mut params[..] {
				[credentials] => credentials.to_string(),
				_ => unreachable!(),
			};
			let request = client.post(path).headers(headers.clone()).auth(auth).body(credentials);
			let value = submit_auth(request).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Authenticate => {
			let path = base_url.join(SQL_PATH)?;
			let token = match &mut params[..1] {
				[Value::Strand(token)] => mem::take(&mut token.0),
				_ => unreachable!(),
			};
			let request =
				client.post(path).headers(headers.clone()).bearer_auth(&token).body("RETURN true");
			take(true, request).await?;
			*auth = Some(Auth::Bearer {
				token,
			});
			Ok(DbResponse::Other(Value::None))
		}
		Method::Invalidate => {
			*auth = None;
			Ok(DbResponse::Other(Value::None))
		}
		Method::Create => {
			let path = base_url.join(SQL_PATH)?;
			let statement = create_statement(&mut params);
			let request =
				client.post(path).headers(headers.clone()).auth(auth).body(statement.to_string());
			let value = take(true, request).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Upsert => {
			let path = base_url.join(SQL_PATH)?;
			let (one, statement) = upsert_statement(&mut params);
			let request =
				client.post(path).headers(headers.clone()).auth(auth).body(statement.to_string());
			let value = take(one, request).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Update => {
			let path = base_url.join(SQL_PATH)?;
			let (one, statement) = update_statement(&mut params);
			let request =
				client.post(path).headers(headers.clone()).auth(auth).body(statement.to_string());
			let value = take(one, request).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Insert => {
			let path = base_url.join(SQL_PATH)?;
			let (one, statement) = insert_statement(&mut params);
			let request =
				client.post(path).headers(headers.clone()).auth(auth).body(statement.to_string());
			let value = take(one, request).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Patch => {
			let path = base_url.join(SQL_PATH)?;
			let (one, statement) = patch_statement(&mut params);
			let request =
				client.post(path).headers(headers.clone()).auth(auth).body(statement.to_string());
			let value = take(one, request).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Merge => {
			let path = base_url.join(SQL_PATH)?;
			let (one, statement) = merge_statement(&mut params);
			let request =
				client.post(path).headers(headers.clone()).auth(auth).body(statement.to_string());
			let value = take(one, request).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Select => {
			let path = base_url.join(SQL_PATH)?;
			let (one, statement) = select_statement(&mut params);
			let request =
				client.post(path).headers(headers.clone()).auth(auth).body(statement.to_string());
			let value = take(one, request).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Delete => {
			let path = base_url.join(SQL_PATH)?;
			let (one, statement) = delete_statement(&mut params);
			let request =
				client.post(path).headers(headers.clone()).auth(auth).body(statement.to_string());
			let value = take(one, request).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Query => {
			let path = base_url.join(SQL_PATH)?;
			let mut request = client.post(path).headers(headers.clone()).query(&vars).auth(auth);
			match param.query {
				Some((query, bindings)) => {
					let bindings: Vec<_> =
						bindings.iter().map(|(key, value)| (key, value.to_string())).collect();
					request = request.query(&bindings).body(query.to_string());
				}
				None => unreachable!(),
			}
			let values = query(request).await?;
			Ok(DbResponse::Query(values))
		}
		#[cfg(target_arch = "wasm32")]
		Method::Export | Method::Import => unreachable!(),
		#[cfg(not(target_arch = "wasm32"))]
		Method::Export => {
			let path = match param.ml_config {
				#[cfg(feature = "ml")]
				Some(MlConfig::Export {
					name,
					version,
				}) => base_url.join(&format!("ml/export/{name}/{version}"))?,
				_ => base_url.join(Method::Export.as_str())?,
			};
			let request = client
				.get(path)
				.headers(headers.clone())
				.auth(auth)
				.header(ACCEPT, "application/octet-stream");
			let value = export(request, (param.file, param.bytes_sender)).await?;
			Ok(DbResponse::Other(value))
		}
		#[cfg(not(target_arch = "wasm32"))]
		Method::Import => {
			let path = match param.ml_config {
				#[cfg(feature = "ml")]
				Some(MlConfig::Import) => base_url.join("ml/import")?,
				_ => base_url.join(Method::Import.as_str())?,
			};
			let file = param.file.expect("file to import from");
			let request = client
				.post(path)
				.headers(headers.clone())
				.auth(auth)
				.header(CONTENT_TYPE, "application/octet-stream");
			let value = import(request, file).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Health => {
			let path = base_url.join(Method::Health.as_str())?;
			let request = client.get(path);
			let value = health(request).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Version => {
			let path = base_url.join(method.as_str())?;
			let request = client.get(path);
			let value = version(request).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Set => {
			let path = base_url.join(SQL_PATH)?;
			let (key, value) = match &mut params[..2] {
				[Value::Strand(key), value] => (mem::take(&mut key.0), value.to_string()),
				_ => unreachable!(),
			};
			let request = client
				.post(path)
				.headers(headers.clone())
				.auth(auth)
				.query(&[(key.as_str(), value.as_str())])
				.body(format!("RETURN ${key}"));
			take(true, request).await?;
			vars.insert(key, value);
			Ok(DbResponse::Other(Value::None))
		}
		Method::Unset => {
			if let [Value::Strand(key)] = &params[..1] {
				vars.swap_remove(&key.0);
			}
			Ok(DbResponse::Other(Value::None))
		}
		Method::Live => {
			let path = base_url.join(SQL_PATH)?;
			let table = match &params[..] {
				[table] => table.to_string(),
				_ => unreachable!(),
			};
			let request = client
				.post(path)
				.headers(headers.clone())
				.auth(auth)
				.query(&[("table", table)])
				.body("LIVE SELECT * FROM type::table($table)");
			let value = take(true, request).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Kill => {
			let path = base_url.join(SQL_PATH)?;
			let id = match &params[..] {
				[id] => id.to_string(),
				_ => unreachable!(),
			};
			let request = client
				.post(path)
				.headers(headers.clone())
				.auth(auth)
				.query(&[("id", id)])
				.body("KILL type::string($id)");
			let value = take(true, request).await?;
			Ok(DbResponse::Other(value))
		}
	}
}
