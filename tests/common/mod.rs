#![allow(dead_code)]
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use tokio::net::TcpStream;
use tokio::time;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

pub const USER: &str = "root";
pub const PASS: &str = "root";

/// Child is a (maybe running) CLI process. It can be killed by dropping it
pub struct Child {
	inner: Option<std::process::Child>,
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

	/// Read the child's stdout concatenated with its stderr. Returns Ok if the child
	/// returns successfully, Err otherwise.
	pub fn output(mut self) -> Result<String, String> {
		let output = self.inner.take().unwrap().wait_with_output().unwrap();

		let mut buf = String::from_utf8(output.stdout).unwrap();
		buf.push_str(&String::from_utf8(output.stderr).unwrap());

		if output.status.success() {
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
	cmd.env_clear();
	cmd.stdin(Stdio::piped());
	cmd.stdout(Stdio::piped());
	cmd.stderr(Stdio::piped());
	cmd.args(args.split_ascii_whitespace());
	Child {
		inner: Some(cmd.spawn().unwrap()),
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
	let path = Path::new(env!("OUT_DIR")).join(name);
	path.to_string_lossy().into_owned()
}

pub async fn start_server(
	auth: bool,
	tls: bool,
	wait_is_ready: bool,
) -> Result<(String, Child), Box<dyn Error>> {
	let mut rng = thread_rng();

	let port: u16 = rng.gen_range(13000..14000);
	let addr = format!("127.0.0.1:{port}");

	let mut extra_args = String::default();
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

	let start_args = format!("start --bind {addr} memory --no-banner --log info --user {USER} --pass {PASS} {extra_args}");

	println!("starting server with args: {start_args}");

	let server = run(&start_args);

	if !wait_is_ready {
		return Ok((addr, server));
	}

	// Wait 5 seconds for the server to start
	let mut interval = time::interval(time::Duration::from_millis(500));
	println!("Waiting for server to start...");
	for _i in 0..10 {
		interval.tick().await;

		if run(&format!("isready --conn http://{addr}")).output().is_ok() {
			println!("Server ready!");
			return Ok((addr, server));
		}
	}

	let server_out = server.kill().output().err().unwrap();
	println!("server output: {server_out}");
	Err("server failed to start".into())
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub async fn connect_ws(addr: &str) -> Result<WsStream, Box<dyn Error>> {
	let url = format!("ws://{}/rpc", addr);
	let (ws_stream, _) = connect_async(url).await?;
	Ok(ws_stream)
}

pub async fn ws_send_msg(
	socket: &mut WsStream,
	msg: Message,
) -> Result<serde_json::Value, Box<dyn Error>> {
	socket.send(msg).await?;

	// Parse and return response
	let mut f = socket.try_filter(|msg| futures_util::future::ready(msg.is_text()));
	let msg = f.select_next_some().await?;
	Ok(serde_json::from_str(&msg.to_string())?)
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
	let json = json!({
		"id": "1",
		"method": "signin",
		"params": [
			SigninParams { user, pass, ns, db, sc }
		],
	});

	if let Err(err) = socket.send(Message::Text(serde_json::to_string(&json).unwrap())).await {
		panic!("Error sending the message: {}", err);
	}

	let mut f = socket.try_filter(|msg| futures_util::future::ready(msg.is_text()));
	let msg = f.select_next_some().await?;
	let msg: serde_json::Value = serde_json::from_str(&msg.to_string()).unwrap();
	Ok(msg["result"]["token"].as_str().unwrap_or_default().to_owned())
}

pub async fn ws_query(
	socket: &mut WsStream,
	query: &str,
) -> Result<Vec<serde_json::Value>, Box<dyn Error>> {
	let req = socket.send(Message::Text(
		serde_json::to_string(&json!({
			"id": "1",
			"method": "query",
			"params": [query],
		}))
		.unwrap(),
	));

	tokio::select! {
		_ = time::sleep(time::Duration::from_millis(100)) => {
			return Err("timeout waiting for the request to be sent".into());
		}
		res = req => {
			if let Err(err) = res {
				return Err(format!("Error sending the message: {}", err).into());
			}
		}
	}

	let mut f = socket.try_filter(|msg| futures_util::future::ready(msg.is_text()));

	let msg: serde_json::Value = tokio::select! {
		_ = time::sleep(time::Duration::from_millis(1000)) => {
			return Err("timeout waiting for the response".into());
		}
		msg = f.select_next_some() => {
			serde_json::from_str(&msg?.to_string())?
		}
	};

	match msg.as_object() {
		Some(obj) if obj.get("error").is_some() => {
			Err(format!("unexpected error from query request: {:?}", obj.get("error")).into())
		}
		Some(obj) if obj.get("result").is_some() => {
			Ok(obj.get("result").unwrap().as_array().unwrap().to_owned())
		}
		_ => return Err(format!("unexpected response: {:?}", msg).into()),
	}
}

pub async fn ws_use(
	socket: &mut WsStream,
	ns: Option<&str>,
	db: Option<&str>,
) -> Result<serde_json::Value, Box<dyn Error>> {
	if let Err(err) = socket
		.send(Message::Text(
			serde_json::to_string(&json!({
				"id": "1",
				"method": "use",
				"params": [
					ns, db
				],
			}))
			.unwrap(),
		))
		.await
	{
		panic!("Error sending the message: {}", err);
	}

	let mut f = socket.try_filter(|msg| futures_util::future::ready(msg.is_text()));
	let msg = f.select_next_some().await?;
	let msg: serde_json::Value = serde_json::from_str(&msg.to_string())?;
	match msg.as_object() {
		Some(obj) if obj.get("error").is_some() => {
			Err(format!("unexpected error from request: {:?}", obj.get("error")).into())
		}
		Some(obj) if obj.get("result").is_some() => Ok(obj.get("result").unwrap().to_owned()),
		_ => return Err(format!("unexpected response: {:?}", msg).into()),
	}
}

// pub async fn ws_query(socket: &mut WsStream, query: &str) -> Result<Value, Box<dyn Error>> {
// 	let req_data: HashMap<String, Value> = HashMap::from([
// 		("id".to_owned(), Value::from("1")),
// 		("method".to_owned(), Value::from("query")),
// 		("params".to_owned(),  Value::from(vec![query.to_owned()])),
// 	]);

//     if let Err(err) = socket.send(Message::Text(to_value(req_data).unwrap().as_string())).await {
//         panic!("Error sending the message: {}", err);
//     }

// 	let mut f = socket.try_filter(|msg| {
// 		println!("msg: {:#?}", msg);
// 		futures_util::future::ready(msg.is_text())
// 	});
// 	let msg = f.select_next_some().await?;
// 	surrealdb::sql::json(&msg.into_text()?)
// }
