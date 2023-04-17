//! HTTP engine

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod native;
#[cfg(target_arch = "wasm32")]
pub(crate) mod wasm;

use crate::api::conn::DbResponse;
use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::engine::create_statement;
use crate::api::engine::delete_statement;
use crate::api::engine::merge_statement;
use crate::api::engine::patch_statement;
use crate::api::engine::remote::Status;
use crate::api::engine::select_statement;
use crate::api::engine::update_statement;
use crate::api::err::Error;
use crate::api::method::query::QueryResult;
use crate::api::opt::from_value;
use crate::api::Connect;
use crate::api::Response as QueryResponse;
use crate::api::Result;
use crate::api::Surreal;
use crate::opt::IntoEndpoint;
use crate::sql::Array;
use crate::sql::Strand;
use crate::sql::Value;
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
use tokio::io::AsyncReadExt;
#[cfg(not(target_arch = "wasm32"))]
use tokio::io::AsyncWrite;
#[cfg(not(target_arch = "wasm32"))]
use tokio_util::compat::FuturesAsyncReadCompatExt;
use url::Url;

const SQL_PATH: &str = "sql";
const LOG: &str = "surrealdb::engine::remote::http";

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
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::remote::http::Client;
	/// use surrealdb::engine::remote::http::Http;
	///
	/// static DB: Surreal<Client> = Surreal::init();
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// DB.connect::<Http>("localhost:8000").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn connect<P>(
		&'static self,
		address: impl IntoEndpoint<P, Client = Client>,
	) -> Connect<Client, ()> {
		Connect {
			router: Some(&self.router),
			address: address.into_endpoint(),
			capacity: 0,
			client: PhantomData,
			response_type: PhantomData,
		}
	}
}

pub(crate) fn default_headers() -> HeaderMap {
	let mut headers = HeaderMap::new();
	headers.insert(ACCEPT, HeaderValue::from_static("application/bung"));
	headers
}

#[derive(Debug)]
enum Auth {
	Basic {
		user: String,
		pass: String,
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
			}) => self.basic_auth(user, Some(pass)),
			Some(Auth::Bearer {
				token,
			}) => self.bearer_auth(token),
			None => self,
		}
	}
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct HttpQueryResponse {
	time: String,
	status: Status,
	#[serde(default)]
	result: Value,
	#[serde(default)]
	detail: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Root {
	user: String,
	pass: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct AuthResponse {
	code: u16,
	details: String,
	#[serde(default)]
	token: Option<String>,
}

async fn submit_auth(request: RequestBuilder) -> Result<Value> {
	let response = request.send().await?.error_for_status()?;
	let bytes = response.bytes().await?;
	let response: AuthResponse =
		bung::from_slice(&bytes).map_err(|error| Error::ResponseFromBinary {
			binary: bytes.to_vec(),
			error,
		})?;
	Ok(response.token.into())
}

async fn query(request: RequestBuilder) -> Result<QueryResponse> {
	info!(target: LOG, "{request:?}");
	let response = request.send().await?.error_for_status()?;
	let bytes = response.bytes().await?;
	let responses = match bung::from_slice::<Vec<HttpQueryResponse>>(&bytes) {
		Ok(responses) => responses,
		Err(_) => {
			let vec =
				bung::from_slice::<Vec<(String, Status, String)>>(&bytes).map_err(|error| {
					Error::ResponseFromBinary {
						binary: bytes.to_vec(),
						error,
					}
				})?;
			let mut responses = Vec::with_capacity(vec.len());
			for (time, status, data) in vec {
				let (result, detail) = match status {
					Status::Ok => (Value::from(data), String::new()),
					Status::Err => (Value::None, data),
				};
				responses.push(HttpQueryResponse {
					time,
					status,
					result,
					detail,
				});
			}
			responses
		}
	};
	let mut map = IndexMap::<usize, QueryResult>::with_capacity(responses.len());
	for (index, response) in responses.into_iter().enumerate() {
		match response.status {
			Status::Ok => {
				match response.result {
					Value::Array(Array(array)) => map.insert(index, Ok(array)),
					Value::None | Value::Null => map.insert(index, Ok(vec![])),
					value => map.insert(index, Ok(vec![value])),
				};
			}
			Status::Err => {
				map.insert(index, Err(Error::Query(response.detail).into()));
			}
		}
	}

	Ok(QueryResponse(map))
}

async fn take(one: bool, request: RequestBuilder) -> Result<Value> {
	if let Some(result) = query(request).await?.0.remove(&0) {
		let mut vec = result?;
		match one {
			true => match vec.pop() {
				Some(Value::Array(Array(mut vec))) => {
					if let [value] = &mut vec[..] {
						return Ok(mem::take(value));
					}
				}
				Some(Value::None | Value::Null) | None => {}
				Some(value) => {
					return Ok(value);
				}
			},
			false => {
				return Ok(Value::Array(Array(vec)));
			}
		}
	}
	match one {
		true => Ok(Value::None),
		false => Ok(Value::Array(Array(vec![]))),
	}
}

#[cfg(not(target_arch = "wasm32"))]
async fn export(request: RequestBuilder, path: PathBuf) -> Result<Value> {
	let mut response = request
		.send()
		.await?
		.error_for_status()?
		.bytes_stream()
		.map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
		.into_async_read()
		.compat();
	let mut writer: Box<dyn AsyncWrite + Unpin + Send> = match path.to_str().unwrap() {
		"-" => Box::new(io::stdout()),
		_ => {
			let file = match OpenOptions::new()
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
			Box::new(file)
		}
	};

	if let Err(error) = io::copy(&mut response, &mut writer).await {
		return Err(Error::FileRead {
			path,
			error,
		}
		.into());
	}
	Ok(Value::None)
}

#[cfg(not(target_arch = "wasm32"))]
async fn import(request: RequestBuilder, path: PathBuf) -> Result<Value> {
	let mut file = match OpenOptions::new().read(true).open(&path).await {
		Ok(path) => path,
		Err(error) => {
			return Err(Error::FileOpen {
				path,
				error,
			}
			.into());
		}
	};
	let mut contents = vec![];
	if let Err(error) = file.read_to_end(&mut contents).await {
		return Err(Error::FileRead {
			path,
			error,
		}
		.into());
	}
	request
		.header(ACCEPT, "application/octet-stream")
		// ideally we should pass `file` directly into the body
		// but currently that results in
		// "HTTP status client error (405 Method Not Allowed) for url"
		.body(contents)
		.send()
		.await?
		.error_for_status()?;
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
	let mut params = match param.query {
		Some((query, bindings)) => {
			vec![query.to_string().into(), bindings.into()]
		}
		None => param.other,
	};

	match method {
		Method::Use => {
			let path = base_url.join(SQL_PATH)?;
			let (ns, db) = match &mut params[..] {
				[Value::Strand(Strand(ns)), Value::Strand(Strand(db))] => {
					(mem::take(ns), mem::take(db))
				}
				_ => unreachable!(),
			};
			let ns = match HeaderValue::try_from(&ns) {
				Ok(ns) => ns,
				Err(_) => {
					return Err(Error::InvalidNsName(ns).into());
				}
			};
			let db = match HeaderValue::try_from(&db) {
				Ok(db) => db,
				Err(_) => {
					return Err(Error::InvalidDbName(db).into());
				}
			};
			let request = client
				.post(path)
				.headers(headers.clone())
				.header("NS", &ns)
				.header("DB", &db)
				.auth(auth)
				.body("RETURN true");
			take(true, request).await?;
			headers.insert("NS", ns);
			headers.insert("DB", db);
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
				if let Ok(Root {
					user,
					pass,
				}) = from_value(mem::take(credentials))
				{
					*auth = Some(Auth::Basic {
						user,
						pass,
					});
				} else {
					*auth = Some(Auth::Bearer {
						token: value.to_strand().as_string(),
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
				[Value::Strand(Strand(token))] => mem::take(token),
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
		Method::Update => {
			let path = base_url.join(SQL_PATH)?;
			let (one, statement) = update_statement(&mut params);
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
			match &mut params[..] {
				[Value::Strand(Strand(statements))] => {
					request = request.body(mem::take(statements));
				}
				[Value::Strand(Strand(statements)), Value::Object(bindings)] => {
					let bindings: Vec<_> =
						bindings.iter().map(|(key, value)| (key, value.to_string())).collect();
					request = request.query(&bindings).body(mem::take(statements));
				}
				_ => unreachable!(),
			}
			let values = query(request).await?;
			Ok(DbResponse::Query(values))
		}
		#[cfg(target_arch = "wasm32")]
		Method::Export | Method::Import => unreachable!(),
		#[cfg(not(target_arch = "wasm32"))]
		Method::Export => {
			let path = base_url.join(Method::Export.as_str())?;
			let file = param.file.expect("file to export into");
			let request = client
				.get(path)
				.headers(headers.clone())
				.auth(auth)
				.header(ACCEPT, "application/octet-stream");
			let value = export(request, file).await?;
			Ok(DbResponse::Other(value))
		}
		#[cfg(not(target_arch = "wasm32"))]
		Method::Import => {
			let path = base_url.join(Method::Import.as_str())?;
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
				[Value::Strand(Strand(key)), value] => (mem::take(key), value.to_string()),
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
			if let [Value::Strand(Strand(key))] = &params[..1] {
				vars.remove(key);
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
