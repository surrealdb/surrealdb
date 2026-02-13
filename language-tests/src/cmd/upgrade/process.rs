use core::fmt;
use std::future::Future;
use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures::{SinkExt, StreamExt};
use surrealdb_core::rpc::{DbResponse, DbResult};
use surrealdb_types::{Error as TypesError, ErrorKind as TypesErrorKind};
use surrealdb_types::Value;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::select;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::client::generate_key;
use tokio_tungstenite::tungstenite::http::header::{
	CONNECTION, HOST, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL, SEC_WEBSOCKET_VERSION,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use super::Config;
use super::protocol::{ProxyObject, ProxyValue};
use crate::cli::{DsVersion, UpgradeBackend};
use crate::tests::report::TestTaskResult;

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
				.header(SEC_WEBSOCKET_PROTOCOL, surrealdb_core::api::format::FLATBUFFERS)
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
	pub async fn request(&mut self, mut object: ProxyObject) -> Result<DbResponse, TypesError> {
		let id = self.id;
		self.id += 1;

		object.insert("id".to_owned(), ProxyValue::from(id));

		// Convert ProxyObject to Value
		let value = object.to_value();

		let message = surrealdb_core::rpc::format::flatbuffers::encode(&value)
			.map_err(|e| TypesError::new(TypesErrorKind::Serialization, e.to_string()))?;
		self.socket.send(Message::Binary(message.into())).await.map_err(|e| {
			TypesError::new(TypesErrorKind::Internal, format!("Failed to send query message: {}", e))
		})?;

		loop {
			let Some(message) = self.socket.next().await else {
				return Err(TypesError::new(
					TypesErrorKind::Internal,
					"Websocket connection closed early".to_string(),
				));
			};
			let message = message.map_err(|e| {
				TypesError::new(TypesErrorKind::Internal, format!("Surrealdb connection error: {}", e))
			})?;

			let data = match message {
				Message::Ping(x) => {
					self.socket.send(Message::Pong(x)).await.map_err(|e| {
						TypesError::new(TypesErrorKind::Internal, format!("Failed to send pong: {}", e))
					})?;
					continue;
				}
				Message::Text(_) => {
					return Err(TypesError::new(
						TypesErrorKind::Internal,
						"Received a text message from the database, expecting only binary messages"
							.to_string(),
					));
				}
				Message::Binary(x) => x,
				Message::Pong(_) => continue,
				// Documentation says we don't get this message.
				Message::Frame(_) => unreachable!(),
				Message::Close(_) => {
					return Err(TypesError::new(
						TypesErrorKind::Internal,
						"Websocket connection to database closed early".to_string(),
					));
				}
			};

			let response: DbResponse = surrealdb_core::rpc::format::flatbuffers::decode(&data)
				.map_err(|e| {
					TypesError::new(
						TypesErrorKind::Serialization,
						format!("Failed to deserialize response: {}", e),
					)
				})?;

			if response.result.is_err() {
				let Err(e) = response.result else {
					unreachable!()
				};
				return Err(TypesError::new(
					TypesErrorKind::Internal,
					format!("Response returned a failure: {}", e.message),
				));
			}

			if response.id != Some(Value::Number(surrealdb_types::Number::Int(id))) {
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
			Err(e) => return Ok(TestTaskResult::RunningError(e.into())),
		};

		match response.result {
			Ok(DbResult::Query(e)) => {
				let results = e
					.into_iter()
					.map(|x| match x.result {
						Ok(value) => Ok(Ok(value)),
						Err(e) => Ok(Err(e.message.clone())),
					})
					.collect::<Result<Vec<Result<Value, String>>, anyhow::Error>>()?;

				Ok(TestTaskResult::Results(results))
			}
			Ok(_) => bail!("Got invalid response type"),
			Err(e) => Ok(TestTaskResult::RunningError(anyhow::Error::msg(e.message.clone()))),
		}
	}
}
