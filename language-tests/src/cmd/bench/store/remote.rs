#![cfg(feature = "bench-remote-store")]

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use futures::{SinkExt, StreamExt as _};
use surrealdb_types::{Object, SurrealValue, Value};
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::http::Uri;
use tokio_tungstenite::tungstenite::http::header::{
	CONNECTION, HOST, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL, SEC_WEBSOCKET_VERSION,
};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::cli::Backend;
use crate::cmd::bench::stats::MeasurementData;
use crate::cmd::bench::store::{BenchDataStore, StoreConfig};

pub struct RemoteStore {
	cmd: Option<mpsc::Sender<Cmd>>,
	task_handle: Option<JoinHandle<()>>,
	auth: AuthParams,
}

struct AuthParams {
	user: String,
	pass: String,
	ns: String,
	db: String,
}

#[derive(SurrealValue)]
pub struct Request {
	id: u64,
	method: String,
	params: Vec<Value>,
}

#[derive(SurrealValue, Debug)]
pub struct Response {
	id: u64,
	result: Option<Value>,
	error: Option<Object>,
}

struct Cmd {
	method: String,
	params: Vec<Value>,
	res: oneshot::Sender<Result<Value>>,
}

impl RemoteStore {
	pub async fn new(url: &str, cfg: &StoreConfig<'_>) -> Result<Self> {
		let url = if !url.ends_with("/rpc") {
			format!("{url}/rpc")
		} else {
			url.to_string()
		};
		let url: Uri = url.parse().context("Invalid store-url")?;
		let host = url.host().unwrap_or("");

		let request = tokio_tungstenite::tungstenite::handshake::client::Request::builder()
			.header(SEC_WEBSOCKET_PROTOCOL, "flatbuffers")
			.header("Upgrade", "websocket")
			.header(SEC_WEBSOCKET_VERSION, "13")
			.header(SEC_WEBSOCKET_KEY, generate_key())
			.header(CONNECTION, "upgrade")
			.header(HOST, host)
			.uri(url)
			.body(())
			.context("Failed to create remote datastore connection request")?;

		let (ws, _) = tokio::time::timeout(Duration::from_secs(5), connect_async(request))
			.await
			.context("Connection request to remote datastore, timedout")?
			.context("Could not connect to remote datastore")?;

		let (send, recv) = mpsc::channel(32);

		// Run the websocket loop in a seperate task
		// This is to keep the connection alive by responding to pings even when we are running a
		// benchmark.
		//
		// TODO: Maybe just create a connection for every request?
		let ws_task = tokio::spawn(Self::ws_task(recv, ws));

		let this = RemoteStore {
			cmd: Some(send),
			task_handle: Some(ws_task),
			auth: AuthParams {
				user: cfg.user.clone(),
				pass: cfg.password.clone(),
				ns: cfg.ns.clone(),
				db: cfg.db.clone(),
			},
		};

		this.login().await.context("Failed to login to remote datastore")?;

		Ok(this)
	}

	async fn cmd(&self, method: &str, params: Vec<Value>) -> Result<Value> {
		let (send, recv) = oneshot::channel();
		let Some(channel) = self.cmd.as_ref() else {
			bail!("Datastore was already closed")
		};

		channel
			.send(Cmd {
				method: method.to_string(),
				params,
				res: send,
			})
			.await
			.unwrap();

		recv.await.unwrap()
	}

	/// Run a command, transparently re-authenticating and retrying once if the
	/// session expired. A full bench run can outlive the remote session's TTL,
	/// at which point the next request would otherwise fail outright.
	async fn cmd_authed(&self, method: &str, params: Vec<Value>) -> Result<Value> {
		match self.cmd(method, params.clone()).await {
			Err(e) if is_session_expired(&e) => {
				self.login().await.context("Re-authentication after session expiry failed")?;
				self.cmd(method, params).await
			}
			other => other,
		}
	}

	async fn login(&self) -> Result<()> {
		let mut params = Object::new();
		params.insert("user", self.auth.user.clone().into_value());
		params.insert("pass", self.auth.pass.clone().into_value());
		params.insert("ns", self.auth.ns.clone().into_value());
		params.insert("db", self.auth.db.clone().into_value());

		self.cmd("signin", vec![params.into_value()]).await.context("Login failed")?;
		self.cmd("use", vec![self.auth.ns.clone().into_value(), self.auth.db.clone().into_value()])
			.await
			.context("Could not use the right namespace/database")?;

		Ok(())
	}

	async fn ws_task(
		mut cmd: mpsc::Receiver<Cmd>,
		stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
	) {
		let mut stream = Some(stream);
		let mut req_id = 0;

		let mut pending = HashMap::<u64, oneshot::Sender<Result<Value>>>::new();

		enum Msg {
			Ws(Result<Message, WsError>),
			Cmd(Cmd),
		}

		fn ws_error(e: WsError) {
			print!("Warning, remote datastore connection returned an error: {e}")
		}

		loop {
			let msg = if let Some(s) = stream.as_mut() {
				select! {
					res = s.next() => {
						let Some(res) = res else {
							stream = None;
							continue;
						};
						Msg::Ws(res)
					}
					res = cmd.recv() => {
						let Some(res) = res else {
							break
						};
						Msg::Cmd(res)
					}
				}
			} else {
				let Some(cmd) = cmd.recv().await else {
					break;
				};
				Msg::Cmd(cmd)
			};

			match msg {
				Msg::Ws(Err(e)) => {
					ws_error(e);
				}
				Msg::Ws(Ok(x)) => match x {
					Message::Text(_) => {
						println!(
							"Warning, got a text messages from the remote datastore when only using binary messages"
						);
					}
					Message::Binary(bytes) => {
						let res = match surrealdb_types::decode::<Value>(&bytes) {
							Ok(x) => x,
							Err(e) => {
								println!("Warning, remote datastore returned invalid format: {e}");
								continue;
							}
						};

						let resp = match Response::from_value(res) {
							Err(e) => {
								println!("Got invalid response: {e}");
								continue;
							}
							Ok(x) => x,
						};

						let Some(channel) = pending.remove(&resp.id) else {
							continue;
						};

						if let Some(mut e) = resp.error {
							let _ = channel.send(Err(anyhow!(
								e.remove("message")
									.and_then(|x| x.into_string().ok())
									.unwrap_or_default()
							)));
						} else if let Some(r) = resp.result {
							let _ = channel.send(Ok(r));
						} else {
							let _ = channel.send(Err(anyhow!("Got no response")));
						}
					}
					Message::Ping(bytes) => {
						if let Some(s) = stream.as_mut()
							&& let Err(e) = s.send(Message::Pong(bytes)).await
						{
							ws_error(e);
						}
					}
					Message::Pong(_) => {}
					Message::Close(_) => {
						println!("Warning, remote datastore connection closed early");
						stream = None;
					}
					Message::Frame(_) => unreachable!(),
				},
				Msg::Cmd(cmd) => {
					if let Some(s) = stream.as_mut() {
						let id = req_id;
						let req = Request {
							id,
							method: cmd.method.to_string(),
							params: cmd.params,
						};
						req_id += 1;

						let bytes = surrealdb_types::encode(&req.into_value()).unwrap();
						if let Err(e) = s.send(Message::Binary(bytes.into())).await {
							ws_error(e);
						}

						pending.insert(id, cmd.res);
					} else {
						let _ = cmd.res.send(Err(anyhow!("Datastore connection lost")));
					}
				}
			}
		}

		if let Some(mut s) = stream
			&& let Err(e) = s.close(None).await
		{
			println!("Warning, couldn't close remote datastore connection correctly: {e}");
		}
	}
}

#[derive(SurrealValue, Debug)]
struct QueryResult {
	result: Value,
	status: String,
}

fn is_session_expired(e: &anyhow::Error) -> bool {
	e.to_string().to_lowercase().contains("session has expired")
}

impl BenchDataStore for RemoteStore {
	async fn add(&mut self, run: super::BenchMarkRun) -> Result<()> {
		let mut params = Object::new();
		params.insert("path", run.path.into_value());
		params.insert("backend", run.backend.into_value());
		// `commit` and `rows` ride alongside the statistics rather than inside
		// them: `MeasurementData` is the sample maths and stays that way.
		params.insert("value", super::measurement_content(run.measurement, run.commit, run.rows));

		let res = self
			.cmd_authed(
				"query",
				vec![
					"CREATE measurement:[$path,$backend,time::now()] CONTENT $value".into_value(),
					params.into_value(),
				],
			)
			.await?
			.into_array()?;

		let mut res = res.into_vec();
		if res.len() != 1 {
			bail!("Got invalid result when fetching measurement")
		}
		let res = QueryResult::from_value(res.pop().unwrap())?;
		if res.status == "ERR" {
			bail!("Got error trying to fetch latest measurement: {}", res.result.into_string()?)
		}

		Ok(())
	}

	async fn fetch_latest<'a>(
		&'a mut self,
		path: &'a str,
		backend: Backend,
	) -> Result<Option<super::Baseline>> {
		let mut params = Object::new();
		params.insert("path", path.into_value());
		params.insert("backend", backend.into_value());

		let res = self
			.cmd_authed(
				"query",
				vec!["fn::last_measurement($path,$backend)".into_value(), params.into_value()],
			)
			.await?
			.into_array()?;

		let mut res = res.into_vec();
		if res.len() != 1 {
			bail!("Got invalid result when fetching measurement")
		}
		let res = QueryResult::from_value(res.pop().unwrap())?;
		if res.status == "ERR" {
			bail!("Got error trying to fetch latest measurement: {}", res.result.into_string()?)
		}
		let (commit, datetime, rows) = super::baseline_meta(&res.result);
		let measurement = Option::<MeasurementData>::from_value(res.result)
			.context("Failed to deserialize latest measurement")?;
		Ok(measurement.map(|measurement| super::Baseline {
			measurement,
			commit,
			datetime,
			rows,
		}))
	}

	async fn close(&mut self) -> Result<()> {
		self.cmd = None;
		self.task_handle.take().expect("Store closed twice").await?;
		Ok(())
	}
}
