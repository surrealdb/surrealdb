#![allow(dead_code)]

pub mod error;

use crate::common::error::TestError;
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;
use std::{env, fs};
use tokio::net::TcpStream;
use tokio::time;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error, info};

pub const USER: &str = "root";
pub const PASS: &str = "root";

/// Child is a (maybe running) CLI process. It can be killed by dropping it
pub struct Child {
	inner: Option<std::process::Child>,
	stdout_path: String,
	stderr_path: String,
}

impl Child {
	/// Send some thing to the child's stdin
	pub fn input(mut self, input: &str) -> Self {
		let stdin = self.inner.as_mut().unwrap().stdin.as_mut().unwrap();
		use std::io::Write;
		stdin.write_all(input.as_bytes()).unwrap();
		self
	}

	pub fn kill(mut self) -> Self {
		self.inner.as_mut().unwrap().kill().unwrap();
		self
	}

	pub fn send_signal(&self, signal: nix::sys::signal::Signal) -> nix::Result<()> {
		nix::sys::signal::kill(
			nix::unistd::Pid::from_raw(self.inner.as_ref().unwrap().id() as i32),
			signal,
		)
	}

	pub fn status(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
		self.inner.as_mut().unwrap().try_wait()
	}

	pub fn stdout(&self) -> String {
		std::fs::read_to_string(&self.stdout_path).expect("Failed to read the stdout file")
	}

	pub fn stderr(&self) -> String {
		std::fs::read_to_string(&self.stderr_path).expect("Failed to read the stderr file")
	}

	/// Read the child's stdout concatenated with its stderr. Returns Ok if the child
	/// returns successfully, Err otherwise.
	pub fn output(mut self) -> Result<String, String> {
		let status = self.inner.take().unwrap().wait().unwrap();

		let mut buf = self.stdout();
		buf.push_str(&self.stderr());

		// Cleanup files after reading them
		std::fs::remove_file(self.stdout_path.as_str()).unwrap();
		std::fs::remove_file(self.stderr_path.as_str()).unwrap();

		if status.success() {
			Ok(buf)
		} else {
			Err(buf)
		}
	}
}

impl Drop for Child {
	fn drop(&mut self) {
		if let Some(inner) = self.inner.as_mut() {
			let _ = inner.kill();
		}
	}
}

pub fn run_internal<P: AsRef<Path>>(args: &str, current_dir: Option<P>) -> Child {
	let mut path = std::env::current_exe().unwrap();
	assert!(path.pop());
	if path.ends_with("deps") {
		assert!(path.pop());
	}

	// Note: Cargo automatically builds this binary for integration tests.
	path.push(format!("{}{}", env!("CARGO_PKG_NAME"), std::env::consts::EXE_SUFFIX));

	let mut cmd = Command::new(path);
	if let Some(dir) = current_dir {
		cmd.current_dir(&dir);
	}

	// Use local files instead of pipes to avoid deadlocks. See https://github.com/rust-lang/rust/issues/45572
	let stdout_path = tmp_file("server-stdout.log");
	let stderr_path = tmp_file("server-stderr.log");
	debug!("Redirecting output. args=`{args}` stdout={stdout_path} stderr={stderr_path})");
	let stdout = Stdio::from(File::create(&stdout_path).unwrap());
	let stderr = Stdio::from(File::create(&stderr_path).unwrap());

	cmd.env_clear();
	cmd.stdin(Stdio::piped());
	cmd.stdout(stdout);
	cmd.stderr(stderr);
	cmd.args(args.split_ascii_whitespace());

	Child {
		inner: Some(cmd.spawn().unwrap()),
		stdout_path,
		stderr_path,
	}
}

/// Run the CLI with the given args
pub fn run(args: &str) -> Child {
	run_internal::<String>(args, None)
}

/// Run the CLI with the given args inside a temporary directory
pub fn run_in_dir<P: AsRef<Path>>(args: &str, current_dir: P) -> Child {
	run_internal(args, Some(current_dir))
}

pub fn tmp_file(name: &str) -> String {
	let path = Path::new(env!("OUT_DIR")).join(format!("{}-{}", rand::random::<u32>(), name));
	path.to_string_lossy().into_owned()
}

pub struct StartServerArguments {
	pub auth: bool,
	pub tls: bool,
	pub wait_is_ready: bool,
	pub tick_interval: time::Duration,
	pub args: String,
}

impl Default for StartServerArguments {
	fn default() -> Self {
		Self {
			auth: true,
			tls: false,
			wait_is_ready: true,
			tick_interval: time::Duration::new(1, 0),
			args: "--allow-all".to_string(),
		}
	}
}

pub async fn start_server_without_auth() -> Result<(String, Child), Box<dyn Error>> {
	start_server(StartServerArguments {
		auth: false,
		..Default::default()
	})
	.await
}

pub async fn start_server_with_defaults() -> Result<(String, Child), Box<dyn Error>> {
	start_server(StartServerArguments::default()).await
}

pub async fn start_server(
	StartServerArguments {
		auth,
		tls,
		wait_is_ready,
		tick_interval,
		args,
	}: StartServerArguments,
) -> Result<(String, Child), Box<dyn Error>> {
	let mut rng = thread_rng();

	let port: u16 = rng.gen_range(13000..14000);
	let addr = format!("127.0.0.1:{port}");

	let mut extra_args = args.clone();
	if tls {
		// Test the crt/key args but the keys are self signed so don't actually connect.
		let crt_path = tmp_file("crt.crt");
		let key_path = tmp_file("key.pem");

		let cert = rcgen::generate_simple_self_signed(Vec::new()).unwrap();
		fs::write(&crt_path, cert.serialize_pem().unwrap()).unwrap();
		fs::write(&key_path, cert.serialize_private_key_pem().into_bytes()).unwrap();

		extra_args.push_str(format!(" --web-crt {crt_path} --web-key {key_path}").as_str());
	}

	if auth {
		extra_args.push_str(" --auth");
	}

	if !tick_interval.is_zero() {
		let sec = tick_interval.as_secs();
		extra_args.push_str(format!(" --tick-interval {sec}s").as_str());
	}

	let start_args = format!("start --bind {addr} memory --no-banner --log trace --user {USER} --pass {PASS} {extra_args}");

	info!("starting server with args: {start_args}");

	// Configure where the logs go when running the test
	let server = run_internal::<String>(&start_args, None);

	if !wait_is_ready {
		return Ok((addr, server));
	}

	// Wait 5 seconds for the server to start
	let mut interval = time::interval(time::Duration::from_millis(1000));
	info!("Waiting for server to start...");
	for _i in 0..10 {
		interval.tick().await;

		if run(&format!("isready --conn http://{addr}")).output().is_ok() {
			info!("Server ready!");
			return Ok((addr, server));
		}
	}

	let server_out = server.kill().output().err().unwrap();
	error!("server output: {server_out}");
	Err("server failed to start".into())
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub async fn connect_ws(addr: &str) -> Result<WsStream, Box<dyn Error>> {
	let url = format!("ws://{}/rpc", addr);
	let (ws_stream, _) = connect_async(url).await?;
	Ok(ws_stream)
}

pub async fn ws_send_msg(socket: &mut WsStream, msg_req: String) -> Result<(), Box<dyn Error>> {
	let now = time::Instant::now();
	debug!("Sending message: {msg_req}");
	tokio::select! {
		_ = time::sleep(time::Duration::from_millis(500)) => {
			return Err("timeout after 500ms waiting for the request to be sent".into());
		}
		res = socket.send(Message::Text(msg_req)) => {
			debug!("Message sent in {:?}", now.elapsed());
			if let Err(err) = res {
				return Err(format!("Error sending the message: {}", err).into());
			}
		}
	}

	Ok(())
}

pub async fn ws_recv_msg(socket: &mut WsStream) -> Result<serde_json::Value, Box<dyn Error>> {
	ws_recv_msg_with_fmt(socket, Format::Json).await
}

/// When testing Live Queries, we may receive multiple messages unordered.
/// This method captures all the expected messages before the given timeout. The result can be inspected later on to find the desired message.
pub async fn ws_recv_all_msgs(
	socket: &mut WsStream,
	expected: usize,
	timeout: Duration,
) -> Result<Vec<serde_json::Value>, Box<dyn Error>> {
	let mut res = Vec::new();
	let deadline = time::Instant::now() + timeout;
	loop {
		tokio::select! {
			_ = time::sleep_until(deadline) => {
				debug!("Waited for {:?} and received {} messages", timeout, res.len());
				if res.len() != expected {
					return Err(format!("Expected {} messages but got {} after {:?}: {:?}", expected, res.len(), timeout, res).into());
				}
			}
			msg = ws_recv_msg(socket) => {
				res.push(msg?);
			}
		}
		if res.len() == expected {
			return Ok(res);
		}
	}
}

pub async fn ws_send_msg_and_wait_response(
	socket: &mut WsStream,
	msg_req: String,
) -> Result<serde_json::Value, Box<dyn Error>> {
	ws_send_msg(socket, msg_req).await?;
	ws_recv_msg_with_fmt(socket, Format::Json).await
}

pub enum Format {
	Json,
	Cbor,
	Pack,
}

pub async fn ws_recv_msg_with_fmt(
	socket: &mut WsStream,
	format: Format,
) -> Result<serde_json::Value, Box<dyn Error>> {
	let now = time::Instant::now();
	debug!("Waiting for response...");
	// Parse and return response
	let mut f = socket.try_filter(|msg| match format {
		Format::Json => futures_util::future::ready(msg.is_text()),
		Format::Pack | Format::Cbor => futures_util::future::ready(msg.is_binary()),
	});

	tokio::select! {
		_ = time::sleep(time::Duration::from_millis(5000)) => {
			Err(Box::new(TestError::NetworkError {message: "timeout after 5s waiting for the response".to_string()}))
		}
		res = f.select_next_some() => {
			debug!("Response received in {:?}", now.elapsed());
			match format {
				Format::Json => Ok(serde_json::from_str(&res?.to_string())?),
				Format::Cbor => Ok(serde_cbor::from_slice(&res?.into_data())?),
				Format::Pack => Ok(serde_pack::from_slice(&res?.into_data())?),
			}
		}
	}
}

#[derive(Serialize, Deserialize)]
struct SigninParams<'a> {
	user: &'a str,
	pass: &'a str,
	#[serde(skip_serializing_if = "Option::is_none")]
	ns: Option<&'a str>,
	#[serde(skip_serializing_if = "Option::is_none")]
	db: Option<&'a str>,
	#[serde(skip_serializing_if = "Option::is_none")]
	sc: Option<&'a str>,
}
#[derive(Serialize, Deserialize)]
struct UseParams<'a> {
	#[serde(skip_serializing_if = "Option::is_none")]
	ns: Option<&'a str>,
	#[serde(skip_serializing_if = "Option::is_none")]
	db: Option<&'a str>,
}

pub async fn ws_signin(
	socket: &mut WsStream,
	user: &str,
	pass: &str,
	ns: Option<&str>,
	db: Option<&str>,
	sc: Option<&str>,
) -> Result<String, Box<dyn Error>> {
	let request_id = uuid::Uuid::new_v4().to_string().replace('-', "");
	let json = json!({
		"id": request_id,
		"method": "signin",
		"params": [
			SigninParams { user, pass, ns, db, sc }
		],
	});

	ws_send_msg(socket, serde_json::to_string(&json).unwrap()).await?;
	let msg = ws_recv_msg(socket).await?;
	debug!("ws_query result json={json:?} msg={msg:?}");

	match msg.as_object() {
		Some(obj) if obj.keys().all(|k| ["id", "error"].contains(&k.as_str())) => {
			Err(format!("unexpected error from query request: {:?}", obj.get("error")).into())
		}
		Some(obj) if obj.keys().all(|k| ["id", "result"].contains(&k.as_str())) => Ok(obj
			.get("result")
			.ok_or(TestError::AssertionError {
				message: format!("expected a result from the received object, got this instead: {:?}", obj),
			})?
			.as_str()
			.ok_or(TestError::AssertionError {
				message: format!("expected the result object to be a string for the received ws message, got this instead: {:?}", obj.get("result")).to_string(),
			})?
			.to_owned()),
		_ => {
			error!("{:?}", msg.as_object().unwrap().keys().collect::<Vec<_>>());
			Err(format!("unexpected response: {:?}", msg).into())
		}
	}
}

pub async fn ws_query(
	socket: &mut WsStream,
	query: &str,
) -> Result<Vec<serde_json::Value>, Box<dyn Error>> {
	let json = json!({
		"id": "1",
		"method": "query",
		"params": [query],
	});

	ws_send_msg(socket, serde_json::to_string(&json).unwrap()).await?;
	let msg = ws_recv_msg(socket).await?;
	debug!("ws_query result json={json:?} msg={msg:?}");

	match msg.as_object() {
		Some(obj) if obj.keys().all(|k| ["id", "error"].contains(&k.as_str())) => {
			Err(format!("unexpected error from query request: {:?}", obj.get("error")).into())
		}
		Some(obj) if obj.keys().all(|k| ["id", "result"].contains(&k.as_str())) => Ok(obj
			.get("result")
			.ok_or(TestError::AssertionError {
				message: format!("expected a result from the received object, got this instead: {:?}", obj),
			})?
			.as_array()
			.ok_or(TestError::AssertionError {
				message: format!("expected the result object to be an array for the received ws message, got this instead: {:?}", obj.get("result")).to_string(),
			})?
			.to_owned()),
		_ => {
			error!("{:?}", msg.as_object().unwrap().keys().collect::<Vec<_>>());
			Err(format!("unexpected response: {:?}", msg).into())
		}
	}
}

pub async fn ws_use(
	socket: &mut WsStream,
	ns: Option<&str>,
	db: Option<&str>,
) -> Result<serde_json::Value, Box<dyn Error>> {
	let json = json!({
		"id": "1",
		"method": "use",
		"params": [
			ns, db
		],
	});

	ws_send_msg(socket, serde_json::to_string(&json).unwrap()).await?;
	let msg = ws_recv_msg(socket).await?;
	debug!("ws_query result json={json:?} msg={msg:?}");

	match msg.as_object() {
		Some(obj) if obj.keys().all(|k| ["id", "error"].contains(&k.as_str())) => {
			Err(format!("unexpected error from query request: {:?}", obj.get("error")).into())
		}
		Some(obj) if obj.keys().all(|k| ["id", "result"].contains(&k.as_str())) => Ok(obj
			.get("result")
			.ok_or(TestError::AssertionError {
				message: format!(
					"expected a result from the received object, got this instead: {:?}",
					obj
				),
			})?
			.to_owned()),
		_ => {
			error!("{:?}", msg.as_object().unwrap().keys().collect::<Vec<_>>());
			Err(format!("unexpected response: {:?}", msg).into())
		}
	}
}

/// Check if the given message is a successful notification from LQ.
pub fn ws_msg_is_notification(msg: &serde_json::Value) -> bool {
	// Example of LQ notification:
	//
	// Object {"result": Object {"action": String("CREATE"), "id": String("04460f07-b0e1-4339-92db-049a94aeec10"), "result": Object {"id": String("table_FD40A9A361884C56B5908A934164884A:⟨an-id-goes-here⟩"), "name": String("ok")}}}
	msg.is_object()
		&& msg["result"].is_object()
		&& msg["result"]
			.as_object()
			.unwrap()
			.keys()
			.all(|k| ["id", "action", "result"].contains(&k.as_str()))
}

/// Check if the given message is a notification from LQ and comes from the given LQ ID.
pub fn ws_msg_is_notification_from_lq(msg: &serde_json::Value, id: &str) -> bool {
	ws_msg_is_notification(msg)
		&& msg["result"].as_object().unwrap().get("id").unwrap().as_str() == Some(id)
}
