use anyhow::{Context, Result, bail};
use core::fmt;
use futures::{SinkExt, StreamExt};
use revision::revisioned;
use std::{future::Future, path::Path, process::Stdio, time::Duration};
use surrealdb_core::{
	dbs::{self, Status},
	val::Value,
};
use tokio::{
	io::AsyncReadExt,
	net::TcpStream,
	process::{Child, Command},
	select,
};
use tokio_tungstenite::{
	MaybeTlsStream, WebSocketStream, connect_async,
	tungstenite::{
		Message,
		handshake::client::generate_key,
		http::header::{
			CONNECTION, HOST, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL, SEC_WEBSOCKET_VERSION,
		},
	},
};

use crate::{
	cli::{DsVersion, UpgradeBackend},
	tests::report::TestTaskResult,
};

use super::{
	Config,
	protocol::{ProxyObject, ProxyValue},
};

#[revisioned(revision = 1)]
#[derive(Debug)]
pub struct Failure {
	code: i64,
	pub message: String,
}

#[revisioned(revision = 1)]
#[derive(Debug)]
pub enum Data {
	Other(Value),
	Query(Vec<dbs::QueryMethodResponse>),
	Live(dbs::Notification),
}

#[revisioned(revision = 1)]
#[derive(Debug)]
pub struct Response {
	id: Option<Value>,
	pub result: Result<Data, Failure>,
}

pub struct ProcessOutput {
	pub stdout: String,
	pub stderr: String,
}

impl fmt::Display for ProcessOutput {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "  > process stdout:\n{}\n  > process stderr:\n{}\n", self.stdout, self.stderr)
	}
}

pub struct SurrealProcess {
	process: Child,
	port: u16,
}

impl SurrealProcess {
	pub async fn new(
		config: &Config,
		version: &DsVersion,
		tmp_dir: &str,
		port: u16,
	) -> Result<SurrealProcess, anyhow::Error> {
		let mut command = match version {
			DsVersion::Path(x) => {
				let path = Path::new(x).join("target").join("debug").join("surreal");
				Command::new(path)
			}
			DsVersion::Version(x) => {
				let path = Path::new(".binary_cache").join(format!("surreal-v{x}"));
				Command::new(path)
			}
		};

		let endpoint = match config.backend {
			UpgradeBackend::RocksDb => format!("rocksdb://{tmp_dir}/ds"),
			UpgradeBackend::SurrealKv => format!("surrealkv://{tmp_dir}/ds"),
			UpgradeBackend::Foundation => format!("fdb://{tmp_dir}/ds"),
		};

		let bind = format!("127.0.0.1:{port}");
		let common_args =
			["start", "--bind", &bind, "--username", "root", "--password", "root", &endpoint];

		command
			.args(common_args)
			// Set the worker threads to 1 so the different processes interver less with eachother
			.env("SURREAL_RUNTIME_WORKER_THREADS", "1")
			.stdin(Stdio::null())
			.stdout(Stdio::piped())
			.stderr(Stdio::piped())
			.kill_on_drop(true);

		let process = command.spawn().context("Failed to spawn process")?;

		Ok(SurrealProcess {
			process,
			port,
		})
	}

	#[cfg(any(unix, target_os = "macos"))]
	pub async fn stop(&self) -> anyhow::Result<()> {
		use anyhow::bail;

		let pid = match self.process.id() {
			None => return Ok(()),
			Some(x) => x,
		};
		let res = Command::new("kill").arg("-15").arg(pid.to_string()).output().await?;
		if !res.status.success() {
			bail!(
				"Kill command for datastore process failed.\n    STDOUT:{}\n    STDERR: {}",
				String::from_utf8_lossy(&res.stdout),
				String::from_utf8_lossy(&res.stderr)
			);
		}
		Ok(())
	}

	#[cfg(windows)]
	pub async fn stop(&self) -> anyhow::Result<()> {
		use anyhow::bail;

		let pid = match self.process.id() {
			None => return Ok(()),
			Some(x) => x,
		};
		let res = Command::new("taskkill").arg("/pid").arg(pid.to_string()).output().await?;
		if !res.status.success() {
			bail!(
				"Kill command for datastore process failed.\n    STDOUT:{}\n    STDERR: {}",
				String::from_utf8_lossy(&res.stdout),
				String::from_utf8_lossy(&res.stderr)
			);
		}
		Ok(())
	}

	pub async fn quit(mut self) -> anyhow::Result<()> {
		self.stop().await?;

		if tokio::time::timeout(Duration::from_secs(5), self.process.wait()).await.is_err() {
			self.process.kill().await?;
		}
		Ok(())
	}

	async fn retrieve_output(proc: &mut Child) -> Option<ProcessOutput> {
		let mut buffer = Vec::new();

		proc.stdout.take()?.read_to_end(&mut buffer).await.unwrap();
		let stdout = String::from_utf8_lossy(&buffer).into_owned();

		buffer.clear();
		proc.stderr.take().unwrap().read_to_end(&mut buffer).await.unwrap();
		let stderr = String::from_utf8_lossy(&buffer).into_owned();

		Some(ProcessOutput {
			stdout,
			stderr,
		})
	}

	pub async fn quit_with_output(mut self) -> anyhow::Result<Option<ProcessOutput>> {
		self.stop().await?;

		if tokio::time::timeout(Duration::from_secs(5), self.process.wait()).await.is_err() {
			self.process.kill().await?;
		}

		let output = Self::retrieve_output(&mut self.process).await;

		Ok(output)
	}

	pub async fn open_connection(&mut self) -> anyhow::Result<SurrealConnection> {
		let port = self.port;
		self.assert_running_while(async {
			let request = tokio_tungstenite::tungstenite::handshake::client::Request::builder()
				.uri(format!("ws://127.0.0.1:{}/rpc", port))
				.header(SEC_WEBSOCKET_PROTOCOL, "revision")
				.header("Upgrade", "websocket")
				.header(SEC_WEBSOCKET_VERSION, "13")
				.header(SEC_WEBSOCKET_KEY, generate_key())
				.header(CONNECTION, "upgrade")
				.header(HOST, format!("127.0.0.1:{}", port))
				.body(())
				.context("failed to create request")?;

			let mut wait_duration = Duration::from_millis(5);
			let mut retries = 0;

			let socket = loop {
				tokio::time::sleep(wait_duration).await;
				match connect_async(request.clone()).await.context("Failed to connect to socket.") {
					Ok((x, _)) => break x,
					Err(e) => {
						if retries < 10 {
							wait_duration *= 2;
							retries += 1;
						} else {
							return Err(e);
						}
					}
				}
			};

			Ok(SurrealConnection {
				id: 0,
				socket,
			})
		})
		.await?
	}

	/// Ensures that the process is still running while the future is executing.
	///
	/// Will return an error if the process quit during the execution of the future.
	pub async fn assert_running_while<R, Fut: Future<Output = R>>(&mut self, f: Fut) -> Result<R> {
		select! {
			// Biased because if the process quit we want to know it first.
			biased;

			_ = self.process.wait() => {
				if let Some(output) = Self::retrieve_output(&mut self.process).await{
					bail!("Surrealdb process quit early.\n{}",output);
				}else{
					bail!("Surrealdb process quit early")
				}
			}
			res = f => {
				Ok(res)
			}
		}
	}
}

pub struct SurrealConnection {
	id: i64,
	socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl SurrealConnection {
	pub async fn request(&mut self, mut object: ProxyObject) -> anyhow::Result<Response> {
		let id = self.id;
		self.id += 1;

		object.insert("id".to_owned(), ProxyValue::from(id));

		let message =
			revision::to_vec(&ProxyValue::from(object)).context("Failed to serialize message")?;
		self.socket.send(Message::Binary(message)).await.context("Failed to send query message")?;

		loop {
			let Some(message) = self.socket.next().await else {
				bail!("Websocket connection closed early")
			};
			let message = message.context("Surrealdb connection error")?;

			let data = match message {
				Message::Ping(x) => {
					self.socket.send(Message::Pong(x)).await?;
					continue;
				}
				Message::Text(_) => {
					bail!(
						"Recieved a text message from the database, expecting only binary messages"
					)
				}
				Message::Binary(x) => x,
				Message::Pong(_) => continue,
				// Documentation says we don't get this message.
				Message::Frame(_) => unreachable!(),
				Message::Close(_) => {
					bail!("Websocket connection to database closed early")
				}
			};

			let response = revision::from_slice::<Response>(&data)
				.context("Failed to deserialize response")?;

			if response.result.is_err() {
				let Err(e) = response.result else {
					unreachable!()
				};
				bail!("Response returned a failure: {}", e.message);
			}

			if response.id != Some(Value::from(id)) {
				continue;
			}

			return Ok(response);
		}
	}

	pub async fn query(&mut self, query: &str) -> anyhow::Result<TestTaskResult> {
		let mut request_obj = ProxyObject::default();
		request_obj.insert("method".to_owned(), ProxyValue::from("query"));
		request_obj.insert("params".to_owned(), ProxyValue::from(vec![ProxyValue::from(query)]));

		let response = match self.request(request_obj).await {
			Ok(x) => x,
			Err(e) => return Ok(TestTaskResult::RunningError(e)),
		};

		match response.result {
			Ok(Data::Query(e)) => {
				let results = e
					.into_iter()
					.map(|x| {
						if let Status::Ok = x.status {
							Ok(Ok(x.result))
						} else {
							let Value::Strand(x) = x.result else {
								bail!("Value of result with error status was not a string");
							};
							Ok(Err(x.to_string()))
						}
					})
					.collect::<Result<Vec<Result<Value, String>>, anyhow::Error>>()?;

				Ok(TestTaskResult::Results(results))
			}
			Ok(_) => bail!("Got invalid response type"),
			Err(e) => Ok(TestTaskResult::RunningError(anyhow::Error::msg(e.message))),
		}
	}
}
