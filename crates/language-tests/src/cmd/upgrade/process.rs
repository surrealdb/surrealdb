use std::{path::Path, process::Stdio, time::Duration};

use anyhow::{bail, Context};
use futures::{SinkExt, StreamExt};
use revision::{revisioned, Revisioned as _};
use surrealdb_core::{
	dbs::{self, Status},
	sql::{Object, Value},
};
use tokio::{
	io::AsyncReadExt,
	process::{Child, Command},
};
use tokio_tungstenite::{
	connect_async,
	tungstenite::{
		handshake::client::generate_key,
		http::header::{
			CONNECTION, HOST, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_PROTOCOL, SEC_WEBSOCKET_VERSION,
		},
		Message,
	},
};

use crate::{
	cli::{DsVersion, UpgradeBackend},
	tests::report::TestTaskResult,
};

use super::Config;

#[revisioned(revision = 1)]
#[derive(Debug)]
struct Failure {
	code: i64,
	message: String,
}

#[revisioned(revision = 1)]
#[derive(Debug)]
enum Data {
	Other(Value),
	Query(Vec<dbs::QueryMethodResponse>),
	Live(dbs::Notification),
}

#[revisioned(revision = 1)]
#[derive(Debug)]
struct Response {
	id: Option<Value>,
	result: Result<Data, Failure>,
}

pub struct ProcessOutput {
	pub stdout: String,
	pub stderr: String,
}

pub struct SurrealProcess {
	docker_name: Option<String>,
	process: Child,
	port: u16,
}

impl SurrealProcess {
	/// Prepare the command to run for a version
	pub async fn prepare(config: &Config, version: &DsVersion) -> Result<(), anyhow::Error> {
		let mut cmd = match version {
			DsVersion::Current => {
				let mut cmd = Command::new("cargo");
				cmd.arg("build").current_dir(&config.surreal_path);
				cmd
			}
			DsVersion::Version(version) => {
				let version = format!("surrealdb/surrealdb:v{version}");
				let mut cmd = Command::new(&config.docker_command);
				cmd.args(["pull", &version]);
				cmd
			}
			DsVersion::Latest => {
				let version = "surrealdb/surrealdb:latest".to_string();
				let mut cmd = Command::new(&config.docker_command);
				cmd.args(["pull", &version]);
				cmd
			}
		};

		cmd.spawn()
			.context("Failed to spawn prepare command")?
			.wait()
			.await
			.context("Failed to wait on prepare command")?;

		Ok(())
	}

	fn docker_command(
		config: &Config,
		image_name: &str,
		port: u16,
		name: &str,
		path: Option<&str>,
	) -> Command {
		let mut cmd = Command::new(&config.docker_command);
		cmd.args([
			"container",
			"run",
			"--rm",
			"--pull",
			"never",
			"--quiet",
			"--read-only",
			"--cpus",
			"1",
			"--publish",
			&format!("127.0.0.1:{port}:8000"),
			"--name",
			name,
		]);

		let endpoint = if let Some(path) = path {
			cmd.args(["--volume", &format!("{path}:/import_data")]);

			match config.backend {
				UpgradeBackend::RocksDb => "rocksdb:///import_data/ds".to_string(),
				UpgradeBackend::SurrealKv => "surrealkv:///import_data/ds".to_string(),
				UpgradeBackend::Foundation => "fdb:///import_data/ds".to_string(),
			}
		} else {
			match config.backend {
				UpgradeBackend::RocksDb => "rocksdb:///tmp/ds".to_string(),
				UpgradeBackend::SurrealKv => "surrealkv:///tmp/ds".to_string(),
				UpgradeBackend::Foundation => "fdb:///tmp/ds".to_string(),
			}
		};

		cmd.args([
			&image_name,
			"start",
			"--username",
			"root",
			"--password",
			"root",
			"--unauthenticated",
			&endpoint,
		])
		.stdin(Stdio::null())
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.kill_on_drop(true);
		return cmd;
	}

	fn current_command(config: &Config, tmp_dir: &str, port: u16) -> Command {
		let endpoint = match config.backend {
			UpgradeBackend::RocksDb => format!("rocksdb://{tmp_dir}/ds"),
			UpgradeBackend::SurrealKv => format!("surrealkv://{tmp_dir}/ds"),
			UpgradeBackend::Foundation => format!("fdb://{tmp_dir}/ds"),
		};
		let bind = format!("127.0.0.1:{port}");
		let common_args = [
			"start",
			"--bind",
			&bind,
			"--username",
			"root",
			"--password",
			"root",
			"--unauthenticated",
			&endpoint,
		];
		let mut res = Command::new("./surreal");
		res.args(common_args)
			.current_dir(Path::new(&config.surreal_path).join("target").join("debug"))
			.stdin(Stdio::null())
			.stdout(Stdio::piped())
			.stderr(Stdio::piped())
			.kill_on_drop(true);
		return res;
	}

	pub async fn new(
		config: &Config,
		version: &DsVersion,
		tmp_dir: &str,
		port: u16,
		mount: bool,
	) -> Result<SurrealProcess, anyhow::Error> {
		let (docker_name, mut cmd) = match version {
			DsVersion::Current => (None, Self::current_command(config, tmp_dir, port)),
			DsVersion::Version(version) => {
				//Just use the port to get a unique name
				let name = format!("surrealdb_{port}");
				let cmd = Self::docker_command(
					config,
					&format!("surrealdb/surrealdb:v{version}"),
					port,
					&name,
					mount.then_some(tmp_dir),
				);
				(Some(name), cmd)
			}
			DsVersion::Latest => {
				//Just use the port to get a unique name
				let name = format!("surrealdb_{port}");
				let cmd = Self::docker_command(
					config,
					&"surrealdb/surrealdb:latest",
					port,
					&name,
					mount.then_some(tmp_dir),
				);
				(Some(name), cmd)
			}
		};

		let process = cmd.spawn().context("Failed to spawn process")?;

		Ok(SurrealProcess {
			process,
			docker_name,
			port,
		})
	}

	#[cfg(any(unix, target_os = "macos"))]
	pub async fn stop(&self) -> anyhow::Result<()> {
		use anyhow::bail;

		let pid = self
			.process
			.id()
			.ok_or_else(|| anyhow::anyhow!("Failed to get process ID for child rprocesses"))?;
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

		let pid = process
			.id()
			.ok_or_else(|| anyhow::anyhow!("Failed to get process ID for child rprocesses"))?;
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

		if let Err(_) = tokio::time::timeout(Duration::from_secs(5), self.process.wait()).await {
			self.process.kill().await?;
		}
		Ok(())
	}

	pub async fn quit_with_output(mut self) -> anyhow::Result<ProcessOutput> {
		self.stop().await?;

		if let Err(_) = tokio::time::timeout(Duration::from_secs(5), self.process.wait()).await {
			self.process.kill().await?;
		}

		let mut buffer = Vec::new();

		self.process.stdout.unwrap().read_to_end(&mut buffer).await.unwrap();
		let stdout = String::from_utf8_lossy(&buffer).into_owned();

		buffer.clear();
		self.process.stderr.unwrap().read_to_end(&mut buffer).await.unwrap();
		let stderr = String::from_utf8_lossy(&buffer).into_owned();

		Ok(ProcessOutput {
			stdout,
			stderr,
		})
	}

	pub async fn retrieve_data(&self, config: &Config, path: &str) -> anyhow::Result<()> {
		let Some(name) = self.docker_name.as_ref() else {
			return Ok(());
		};

		Command::new(&config.docker_command)
			.args(["container", "cp", &format!("{name}:/tmp/ds"), path])
			.stdout(Stdio::null())
			.stderr(Stdio::null())
			.spawn()
			.context("Failed to spawn command to copy data from container")?
			.wait()
			.await
			.context("Failed to finish command to copy data from container")?;

		Ok(())
	}

	pub async fn send_request(&self, query: &str) -> anyhow::Result<TestTaskResult> {
		// setup the connection
		let request = tokio_tungstenite::tungstenite::handshake::client::Request::builder()
			.uri(format!("ws://127.0.0.1:{}/rpc", self.port))
			.header(SEC_WEBSOCKET_PROTOCOL, "revision")
			.header("Upgrade", "websocket")
			.header(SEC_WEBSOCKET_VERSION, "13")
			.header(SEC_WEBSOCKET_KEY, generate_key())
			.header(CONNECTION, "upgrade")
			.header(HOST, format!("127.0.0.1:{}", self.port))
			.body(())
			.context("failed to create request")?;

		let mut wait_duration = Duration::from_millis(100);
		let mut retries = 0;

		let mut socket = loop {
			match connect_async(request.clone()).await.context("Failed to connect to socket.") {
				Ok((x, _)) => break x,
				Err(e) => {
					if retries < 8 {
						tokio::time::sleep(wait_duration).await;
						wait_duration *= 2;
						retries += 1;
					} else {
						return Err(e);
					}
				}
			}
		};

		// send the query request
		let mut request_obj = Object::default();
		request_obj.insert("id".to_owned(), Value::from(0xCAFECAFEi64));
		request_obj.insert("method".to_owned(), Value::from("use"));
		request_obj.insert(
			"params".to_owned(),
			Value::from(vec![Value::from("test"), Value::from("test")]),
		);

		let mut message_data = Vec::new();
		Value::from(request_obj)
			.serialize_revisioned(&mut message_data)
			.context("Failed to serialize message")?;

		socket.send(Message::Binary(message_data)).await.context("Failed to send query message")?;

		// send the query request
		let mut request_obj = Object::default();
		request_obj.insert("id".to_owned(), Value::from(0xCAFEBEEFi64));
		request_obj.insert("method".to_owned(), Value::from("query"));
		request_obj.insert("params".to_owned(), Value::from(vec![Value::from(query)]));

		let mut message_data = Vec::new();
		Value::from(request_obj)
			.serialize_revisioned(&mut message_data)
			.context("Failed to serialize message")?;

		socket.send(Message::Binary(message_data)).await.context("Failed to send query message")?;

		// wait for a response.
		loop {
			let Some(message) = socket.next().await else {
				return Ok(TestTaskResult::RunningError(anyhow::Error::msg(
					"Websocket connection to database closed early",
				)));
			};
			let message = message.context("Failed to parse message")?;
			let data =
				match message {
					Message::Ping(x) => {
						socket.send(Message::Pong(x)).await?;
						continue;
					}
					Message::Text(_) => {
						bail!("Recieved a text message from the database, expecting only binary messages")
					}
					Message::Binary(x) => x,
					Message::Pong(_) => continue,
					// Documentation says we don't get this message.
					Message::Frame(_) => unreachable!(),
					Message::Close(_) => {
						return Ok(TestTaskResult::RunningError(anyhow::Error::msg(
							"Websocket connection to database closed early",
						)));
					}
				};

			let response = revision::from_slice::<Response>(&data)
				.context("Failed to deserialize response")?;

			if response.result.is_err() {
				let Err(e) = response.result else {
					unreachable!()
				};
				return Ok(TestTaskResult::RunningError(anyhow::Error::msg(format!(
					"Response returned a failure: {}",
					e.message
				))));
			}

			if response.id != Some(Value::from(0xCAFEBEEFi64)) {
				continue;
			}

			return match response.result {
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
			};
		}
	}
}
