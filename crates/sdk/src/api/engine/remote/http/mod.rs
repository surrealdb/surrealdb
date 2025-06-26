//! HTTP engine
use crate::api::Connect;
use crate::api::Result;
use crate::api::Surreal;
use crate::api::conn::{Command, Request};
use crate::api::engine::remote::{deserialize_flatbuffers, serialize_flatbuffers};
use crate::api::err::Error;
use crate::dbs::ResponseData;

use crate::headers::AUTH_DB;
use crate::headers::AUTH_NS;
use crate::headers::DB;
use crate::headers::NS;
use crate::opt::IntoEndpoint;
use anyhow::Context;
use futures::TryStreamExt;
use indexmap::IndexMap;
use reqwest::RequestBuilder;
use reqwest::header::ACCEPT;
use reqwest::header::CONTENT_TYPE;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use serde::Deserialize;
use serde::Serialize;
use std::marker::PhantomData;
use surrealdb_core::dbs::QueryResult;
use surrealdb_core::expr::{Object, Value, from_value as from_core_value};
use surrealdb_core::iam::access;
use surrealdb_core::protocol::ToFlatbuffers;
use surrealdb_core::protocol::flatbuffers::surreal_db::protocol::rpc as rpc_fb;
use surrealdb_core::sql::Statement;
use surrealdb_core::sql::statements::OutputStatement;
use surrealdb_core::sql::{Param, Query, SqlValue};
use url::Url;

#[cfg(not(target_family = "wasm"))]
use std::path::PathBuf;
#[cfg(not(target_family = "wasm"))]
use tokio::fs::OpenOptions;
#[cfg(not(target_family = "wasm"))]
use tokio::io;
#[cfg(not(target_family = "wasm"))]
use tokio_util::compat::FuturesAsyncReadCompatExt;
#[cfg(target_family = "wasm")]
use wasm_bindgen_futures::spawn_local;

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
	/// Connects to a specific database endpoint, saving the connection on the static client
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
		let bytes = res.bytes().await?;
		let response = deserialize_flatbuffers::<rpc_fb::Response<'_>>(&bytes)?;

		let query_results = response
			.data_as_results()
			.ok_or_else(|| Error::InternalError("No results in response".to_string()))?;
		let results = query_results
			.results()
			.ok_or_else(|| Error::InternalError("No results in response".to_string()))?;
		if results.is_empty() {
			return Err(Error::InternalError("No results in response".to_string()).into());
		}

		for query_result in results {
			if let Some(err) = query_result.result_as_error() {
				let code = err.code();
				let message = err.message().unwrap_or("Unknown error").to_string();
				return Err(Error::Query(format!("({code}): {message}")).into());
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
	req: impl for<'a> ToFlatbuffers<Output<'a> = ::flatbuffers::WIPOffset<rpc_fb::Request<'a>>>,
	base_url: &Url,
	client: &reqwest::Client,
	headers: &HeaderMap,
	auth: &Option<Auth>,
) -> Result<ResponseData> {
	let url = base_url.join(RPC_PATH).unwrap();
	let http_req =
		client.post(url).headers(headers.clone()).auth(auth).body(serialize_flatbuffers(&req)?);
	let response = http_req.send().await?.error_for_status()?;
	let bytes = response.bytes().await?;

	let response = deserialize_flatbuffers::<rpc_fb::Response<'_>>(&bytes)?;
	todo!("STU: FIX THIS");
	// QueryResultData::from_server_result(response.results)
}

// fn flatten_response_array(res: QueryResultData) -> QueryResultData {
// 	match res {
// 		QueryResultData::Results(results) if results.len() == 1 => {
// 			let query_result = results.into_iter().take(1).next().expect("There must be one result in the vec");
// 			match query_result.result
// 			let v = array.into_iter().next().unwrap();
// 			QueryResultData::new_from_value(v)
// 		}
// 		x => x,
// 	}
// }

async fn router(
	req: Request,
	base_url: &Url,
	client: &reqwest::Client,
	headers: &mut HeaderMap,
	vars: &mut IndexMap<String, Value>,
	auth: &mut Option<Auth>,
) -> Result<ResponseData> {
	match req.command {
		Command::Query {
			txn,
			query,
			mut variables,
		} => {
			variables.extend(vars.clone());
			let req = Request::new(Command::Query {
				txn,
				query,
				variables,
			});

			send_request(req, base_url, client, headers, auth).await
		}
		Command::Use {
			namespace,
			database,
		} => {
			let req = Request::new(Command::Use {
				namespace: namespace.clone(),
				database: database.clone(),
			});
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
		Command::Signin(params) => {
			todo!("STU: FIX THIS");
			// let req = Request::new(Command::Signin(params.clone()));

			// let QueryResultData::new_from_value(value) =
			// 	send_request(req, base_url, client, headers, auth).await?
			// else {
			// 	return Err(Error::InternalError(
			// 		"recieved invalid result from server".to_string(),
			// 	)
			// 	.into());
			// };

			// let Some(access) = params.access else {
			// 	return Err(
			// 		Error::InvalidRequest(format!("missing access in signin command")).into()
			// 	);
			// };

			// match access.inner {
			// 	Some(AccessInnerProto::RootUser(RootUserCredentials {
			// 		username,
			// 		password,
			// 	})) => {
			// 		*auth = Some(Auth::Basic {
			// 			ns: None,
			// 			db: None,
			// 			user: username,
			// 			pass: password,
			// 		});
			// 	}
			// 	Some(AccessInnerProto::Namespace(NamespaceAccessCredentials {
			// 		namespace,
			// 		access,
			// 		key,
			// 	})) => {
			// 		*auth = Some(Auth::Bearer {
			// 			token: value.to_raw_string(),
			// 		});
			// 	}
			// 	Some(AccessInnerProto::Database(DatabaseAccessCredentials {
			// 		namespace,
			// 		database,
			// 		access,
			// 		key,
			// 		refresh,
			// 	})) => {
			// 		*auth = Some(Auth::Bearer {
			// 			token: value.to_raw_string(),
			// 		})
			// 	}
			// 	Some(AccessInnerProto::NamespaceUser(NamespaceUserCredentials {
			// 		namespace,
			// 		username,
			// 		password,
			// 	})) => {
			// 		*auth = Some(Auth::Basic {
			// 			ns: Some(namespace),
			// 			db: None,
			// 			user: username,
			// 			pass: password,
			// 		});
			// 	}
			// 	Some(AccessInnerProto::DatabaseUser(DatabaseUserCredentials {
			// 		namespace,
			// 		database,
			// 		username,
			// 		password,
			// 	})) => {
			// 		*auth = Some(Auth::Basic {
			// 			ns: Some(namespace),
			// 			db: Some(database),
			// 			user: username,
			// 			pass: password,
			// 		});
			// 	}
			// 	None => {
			// 		*auth = Some(Auth::Bearer {
			// 			token: value.to_raw_string(),
			// 		});
			// 	}
			// }

			// Ok(QueryResultData::new_from_value(value))
		}
		Command::Authenticate {
			token,
		} => {
			let req = Request::new(Command::Authenticate {
				token: token.clone(),
			});
			send_request(req, base_url, client, headers, auth).await?;

			*auth = Some(Auth::Bearer {
				token,
			});
			Ok(ResponseData::new_from_value(Value::None))
		}
		Command::Invalidate => {
			*auth = None;
			Ok(ResponseData::new_from_value(Value::None))
		}
		Command::Set {
			key,
			value,
		} => {
			let req = Request::new(Command::Set {
				key: key.clone(),
				value,
			});
			let ResponseData::Results(mut res) =
				send_request(req, base_url, client, headers, auth).await?
			else {
				return Err(Error::InternalError(
					"recieved invalid result from server".to_string(),
				)
				.into());
			};

			let result: QueryResult =
				res.into_iter().next().context("Expected one item in result")?;
			let value = result.result?;
			vars.insert(key, value);
			Ok(ResponseData::new_from_value(Value::None))
		}
		Command::Unset {
			key,
		} => {
			vars.shift_remove(&key);
			Ok(ResponseData::new_from_value(Value::None))
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
			let config_value: Value = config.into();
			let request = client
				.post(req_path)
				.body(config_value.into_json().to_string())
				.headers(headers.clone())
				.auth(auth)
				.header(CONTENT_TYPE, "application/json")
				.header(ACCEPT, "application/octet-stream");
			export_file(request, path).await?;
			Ok(ResponseData::new_from_value(Value::None))
		}
		Command::ExportBytes {
			bytes,
			config,
		} => {
			let req_path = base_url.join("export")?;
			let config = config.unwrap_or_default();
			let config_value: Value = config.into();
			let request = client
				.post(req_path)
				.body(config_value.into_json().to_string())
				.headers(headers.clone())
				.auth(auth)
				.header(CONTENT_TYPE, "application/json")
				.header(ACCEPT, "application/octet-stream");
			export_bytes(request, bytes).await?;
			Ok(ResponseData::new_from_value(Value::None))
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
			Ok(ResponseData::new_from_value(Value::None))
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
			Ok(ResponseData::new_from_value(Value::None))
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
			Ok(ResponseData::new_from_value(Value::None))
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
			Ok(ResponseData::new_from_value(Value::None))
		}
		Command::SubscribeLive {
			..
		} => Err(Error::LiveQueriesNotSupported.into()),
		cmd => {
			// let needs_flatten = cmd.needs_flatten();
			let req = Request::new(cmd);
			let mut res = send_request(req, base_url, client, headers, auth).await?;
			// if needs_flatten {
			// 	res = flatten_response_array(res);
			// }
			Ok(res)
		}
	}
}
