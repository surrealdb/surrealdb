//! HTTP engine

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod native;
#[cfg(target_arch = "wasm32")]
pub(crate) mod wasm;

use crate::api::conn::Command;
use crate::api::conn::DbResponse;
use crate::api::conn::RequestData;
use crate::api::engine::remote::duration_from_str;
use crate::api::err::Error;
use crate::api::method::query::QueryResult;
use crate::api::Connect;
use crate::api::Response as QueryResponse;
use crate::api::Result;
use crate::api::Surreal;
use crate::dbs::Status;
use crate::engine::value_to_values;
use crate::headers::AUTH_DB;
use crate::headers::AUTH_NS;
use crate::headers::DB;
use crate::headers::NS;
use crate::method::Stats;
use crate::opt::IntoEndpoint;
use crate::sql::from_value;
use crate::sql::serde::deserialize;
use crate::sql::Value;
use futures::TryStreamExt;
use indexmap::IndexMap;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use reqwest::header::ACCEPT;
use reqwest::RequestBuilder;
use serde::Deserialize;
use serde::Serialize;
use std::marker::PhantomData;
use std::mem;
use surrealdb_core::sql::statements::CreateStatement;
use surrealdb_core::sql::statements::DeleteStatement;
use surrealdb_core::sql::statements::InsertStatement;
use surrealdb_core::sql::statements::SelectStatement;
use surrealdb_core::sql::statements::UpdateStatement;
use surrealdb_core::sql::statements::UpsertStatement;
use surrealdb_core::sql::Data;
use surrealdb_core::sql::Field;
use surrealdb_core::sql::Output;
use url::Url;

#[cfg(not(target_arch = "wasm32"))]
use reqwest::header::CONTENT_TYPE;
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;
use surrealdb_core::sql::Function;
#[cfg(not(target_arch = "wasm32"))]
use tokio::fs::OpenOptions;
#[cfg(not(target_arch = "wasm32"))]
use tokio::io;
#[cfg(not(target_arch = "wasm32"))]
use tokio_util::compat::FuturesAsyncReadCompatExt;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::spawn_local;

const SQL_PATH: &str = "sql";

/// The HTTP scheme used to connect to `http://` endpoints
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
				Value::None | Value::Null => {}
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
	RequestData {
		command,
		..
	}: RequestData,
	base_url: &Url,
	client: &reqwest::Client,
	headers: &mut HeaderMap,
	vars: &mut IndexMap<String, String>,
	auth: &mut Option<Auth>,
) -> Result<DbResponse> {
	match command {
		Command::Use {
			namespace,
			database,
		} => {
			let path = base_url.join(SQL_PATH)?;
			let mut request = client.post(path).headers(headers.clone());
			let ns = match namespace {
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
			let db = match database {
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
		Command::Signin {
			credentials,
		} => {
			let path = base_url.join("signin")?;
			let request =
				client.post(path).headers(headers.clone()).auth(auth).body(credentials.to_string());
			let value = submit_auth(request).await?;
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
		Command::Signup {
			credentials,
		} => {
			let path = base_url.join("signup")?;
			let request =
				client.post(path).headers(headers.clone()).auth(auth).body(credentials.to_string());
			let value = submit_auth(request).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Authenticate {
			token,
		} => {
			let path = base_url.join(SQL_PATH)?;
			let request =
				client.post(path).headers(headers.clone()).bearer_auth(&token).body("RETURN true");
			take(true, request).await?;
			*auth = Some(Auth::Bearer {
				token,
			});
			Ok(DbResponse::Other(Value::None))
		}
		Command::Invalidate => {
			*auth = None;
			Ok(DbResponse::Other(Value::None))
		}
		Command::Create {
			what,
			data,
		} => {
			let path = base_url.join(SQL_PATH)?;
			let statement = {
				let mut stmt = CreateStatement::default();
				stmt.what = value_to_values(what);
				stmt.data = data.map(Data::ContentExpression);
				stmt.output = Some(Output::After);
				stmt
			};
			let request =
				client.post(path).headers(headers.clone()).auth(auth).body(statement.to_string());
			let value = take(true, request).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Upsert {
			what,
			data,
		} => {
			let path = base_url.join(SQL_PATH)?;
			let one = what.is_thing();
			let statement = {
				let mut stmt = UpsertStatement::default();
				stmt.what = value_to_values(what);
				stmt.data = data.map(Data::ContentExpression);
				stmt.output = Some(Output::After);
				stmt
			};
			let request =
				client.post(path).headers(headers.clone()).auth(auth).body(statement.to_string());
			let value = take(one, request).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Update {
			what,
			data,
		} => {
			let path = base_url.join(SQL_PATH)?;
			let one = what.is_thing();
			let statement = {
				let mut stmt = UpdateStatement::default();
				stmt.what = value_to_values(what);
				stmt.data = data.map(Data::ContentExpression);
				stmt.output = Some(Output::After);
				stmt
			};
			let request =
				client.post(path).headers(headers.clone()).auth(auth).body(statement.to_string());
			let value = take(one, request).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Insert {
			what,
			data,
		} => {
			let path = base_url.join(SQL_PATH)?;
			let one = !data.is_array();
			let statement = {
				let mut stmt = InsertStatement::default();
				stmt.into = what;
				stmt.data = Data::SingleExpression(data);
				stmt.output = Some(Output::After);
				stmt
			};
			let request =
				client.post(path).headers(headers.clone()).auth(auth).body(statement.to_string());
			let value = take(one, request).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Patch {
			what,
			data,
		} => {
			let path = base_url.join(SQL_PATH)?;
			let one = what.is_thing();
			let statement = {
				let mut stmt = UpdateStatement::default();
				stmt.what = value_to_values(what);
				stmt.data = data.map(Data::PatchExpression);
				stmt.output = Some(Output::After);
				stmt
			};
			let request =
				client.post(path).headers(headers.clone()).auth(auth).body(statement.to_string());
			let value = take(one, request).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Merge {
			what,
			data,
		} => {
			let path = base_url.join(SQL_PATH)?;
			let one = what.is_thing();
			let statement = {
				let mut stmt = UpdateStatement::default();
				stmt.what = value_to_values(what);
				stmt.data = data.map(Data::MergeExpression);
				stmt.output = Some(Output::After);
				stmt
			};
			let request =
				client.post(path).headers(headers.clone()).auth(auth).body(statement.to_string());
			let value = take(one, request).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Select {
			what,
		} => {
			let path = base_url.join(SQL_PATH)?;
			let one = what.is_thing();
			let statement = {
				let mut stmt = SelectStatement::default();
				stmt.what = value_to_values(what);
				stmt.expr.0 = vec![Field::All];
				stmt
			};
			let request =
				client.post(path).headers(headers.clone()).auth(auth).body(statement.to_string());
			let value = take(one, request).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Delete {
			what,
		} => {
			let one = what.is_thing();
			let path = base_url.join(SQL_PATH)?;
			let (one, statement) = {
				let mut stmt = DeleteStatement::default();
				stmt.what = value_to_values(what);
				stmt.output = Some(Output::Before);
				(one, stmt)
			};
			let request =
				client.post(path).headers(headers.clone()).auth(auth).body(statement.to_string());
			let value = take(one, request).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Query {
			query: q,
			variables,
		} => {
			let path = base_url.join(SQL_PATH)?;
			let mut request = client.post(path).headers(headers.clone()).query(&vars).auth(auth);
			let bindings: Vec<_> =
				variables.iter().map(|(key, value)| (key, value.to_string())).collect();
			request = request.query(&bindings).body(q.to_string());
			let values = query(request).await?;
			Ok(DbResponse::Query(values))
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
		Command::Health => {
			let path = base_url.join("health")?;
			let request = client.get(path);
			let value = health(request).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Version => {
			let path = base_url.join("version")?;
			let request = client.get(path);
			let value = version(request).await?;
			Ok(DbResponse::Other(value))
		}
		Command::Set {
			key,
			value,
		} => {
			let path = base_url.join(SQL_PATH)?;
			let value = value.to_string();
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
		Command::Unset {
			key,
		} => {
			vars.shift_remove(&key);
			Ok(DbResponse::Other(Value::None))
		}
		Command::SubscribeLive {
			..
		} => Err(Error::LiveQueriesNotSupported.into()),
		Command::Kill {
			uuid,
		} => {
			let path = base_url.join(SQL_PATH)?;
			let request = client
				.post(path)
				.headers(headers.clone())
				.auth(auth)
				.query(&[("id", uuid)])
				.body("KILL type::string($id)");
			let value = take(true, request).await?;
			Ok(DbResponse::Other(value))
		}
		Method::Run => {
			let path = base_url.join(SQL_PATH)?;
			let (fn_name, _fn_version, fn_params) = match &mut params[..] {
				[Value::Strand(n), Value::Strand(v), Value::Array(p)] => (n, Some(v), p),
				[Value::Strand(n), Value::None, Value::Array(p)] => (n, None, p),
				_ => unreachable!(),
			};
			let args: Vec<(String, Value)> = fn_params
				.iter()
				.enumerate()
				.map(|(i, v)| (format!("p{i}"), v.to_owned()))
				.collect();
			// let arg_str =
			// 	(0..args.len()).map(|i| format!("$p{i}")).collect::<Vec<String>>().join(", ");
			// let statement = match fn_version {
			// 	Some(v) => format!("{fn_name}::<{v}>({arg_str})"),
			// 	None => format!("{fn_name}({arg_str})"),
			// };

			let func: Value = match &fn_name[0..4] {
				"fn::" => {
					Function::Custom(fn_name.chars().skip(4).collect(), fn_params.0.clone()).into()
				}
				// should return error, but can't on wasm
				#[cfg(feature = "ml")]
				"ml::" => {
					let mut tmp = Model::default();

					tmp.name = fn_name.chars().skip(4).collect();
					tmp.args = mem::take(fn_params).0;
					tmp.version = mem::take(
						_fn_version
							.ok_or(Error::Query("ML functions must have a version".to_string()))?,
					)
					.0;
					tmp
				}
				.into(),
				_ => Function::Normal(mem::take(fn_name).0, mem::take(fn_params).0).into(),
			};
			let statement = func.to_string();

			println!("statement: {statement}");
			let request =
				client.post(path).headers(headers.clone()).auth(auth).query(&args).body(statement);
			// TODO: unwrap for debugging
			let value = take(true, request).await.unwrap();
			Ok(DbResponse::Other(value))
		}
	}
}
