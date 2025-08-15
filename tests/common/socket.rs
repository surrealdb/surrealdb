use std::collections::HashMap;
use std::error::Error;
use std::result::Result as StdResult;
use std::time::Duration;

use futures::channel::oneshot::channel;
use futures_util::{SinkExt, TryStreamExt};
use http::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time;
use tokio_stream::StreamExt;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tracing::{debug, error};

use super::format::Format;
use crate::common::error::TestError;

type Result<T> = StdResult<T, Box<dyn Error>>;
type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Serialize, Deserialize)]
struct UseParams<'a> {
	#[serde(skip_serializing_if = "Option::is_none")]
	ns: Option<&'a str>,
	#[serde(skip_serializing_if = "Option::is_none")]
	db: Option<&'a str>,
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
	ac: Option<&'a str>,
}

enum SocketMsg {
	SendAwait {
		method: String,
		args: serde_json::Value,
		channel: oneshot::Sender<serde_json::Value>,
	},
	Send {
		method: String,
		args: serde_json::Value,
	},
	Close {
		channel: oneshot::Sender<()>,
	},
}

pub struct Socket {
	sender: mpsc::Sender<SocketMsg>,
	other_messages: mpsc::Receiver<serde_json::Value>,
}

// pub struct Socket(pub WebSocketStream<MaybeTlsStream<TcpStream>>);

impl Socket {
	/// Close the connection with the WebSocket server
	pub async fn close(&mut self) -> Result<()> {
		let (send, recv) = oneshot::channel();
		self.sender
			.send(SocketMsg::Close {
				channel: send,
			})
			.await?;
		if (recv.await).is_err() {
			return Err("Ws task stoped unexpectedly".to_string().into());
		}
		Ok(())
	}

	/// Connect to a WebSocket server using a specific format
	pub async fn connect(addr: &str, format: Option<Format>, msg_format: Format) -> Result<Self> {
		let url = format!("ws://{addr}/rpc");
		let mut req = url.into_client_request().unwrap();
		if let Some(v) = format.map(|v| v.to_string()) {
			req.headers_mut().insert("Sec-WebSocket-Protocol", v.parse().unwrap());
		}
		let (stream, _) = connect_async(req).await?;
		let (send, recv) = mpsc::channel(16);
		let (send_other, recv_other) = mpsc::channel(16);

		tokio::spawn(async move {
			if let Err(e) = Self::ws_task(recv, stream, send_other, msg_format).await {
				eprintln!("error in websocket task: {e}")
			}
		});

		Ok(Self {
			sender: send,
			other_messages: recv_other,
		})
	}

	/// Connect to a WebSocket server using a specific format with custom
	/// headers
	pub async fn connect_with_headers(
		addr: &str,
		format: Option<Format>,
		msg_format: Format,
		headers: HeaderMap<HeaderValue>,
	) -> Result<Self> {
		let url = format!("ws://{addr}/rpc");
		let mut req = url.into_client_request().unwrap();
		if let Some(v) = format.map(|v| v.to_string()) {
			req.headers_mut().insert("Sec-WebSocket-Protocol", v.parse().unwrap());
		}
		for (key, value) in headers.into_iter() {
			if let Some(key) = key {
				req.headers_mut().append(key, value);
			}
		}
		let (stream, _) = connect_async(req).await?;
		let (send, recv) = mpsc::channel(16);
		let (send_other, recv_other) = mpsc::channel(16);

		tokio::spawn(async move {
			if let Err(e) = Self::ws_task(recv, stream, send_other, msg_format).await {
				eprintln!("error in websocket task: {e}")
			}
		});

		Ok(Self {
			sender: send,
			other_messages: recv_other,
		})
	}

	fn to_msg(format: Format, message: &serde_json::Value) -> Result<Message> {
		match format {
			Format::Json => Ok(Message::Text(serde_json::to_string(message)?)),
			Format::Cbor => {
				// For tests we need to convert the serde_json::Value
				// to a SurrealQL value, so that record ids, uuids,
				// datetimes, and durations are stored properly.
				// First of all we convert the JSON type to a string.
				let json = message.to_string();
				// Then we parse the JSON in to SurrealQL.
				let surrealql = surrealdb_core::syn::value_legacy_strand(&json)?;
				// Then we convert the SurrealQL in to CBOR.
				let cbor = surrealdb_core::rpc::format::cbor::encode(surrealql)?;
				// THen output the message.
				Ok(Message::Binary(cbor))
			}
		}
	}

	fn from_msg(format: Format, msg: Message) -> Result<Option<serde_json::Value>> {
		match msg {
			Message::Text(msg) => {
				debug!("Response {msg:?}");
				match format {
					Format::Json => {
						let msg = serde_json::from_str(&msg)?;
						debug!("Received message: {msg}");
						Ok(Some(msg))
					}
					_ => Err("Expected to receive a binary message".to_string().into()),
				}
			}
			Message::Binary(msg) => {
				debug!("Response {msg:?}");
				match format {
					Format::Cbor => {
						// For tests we need to convert the binary data to
						// a serde_json::Value so that test assertions work.
						// First of all we deserialize the CBOR data.
						// Then we convert it to a SurrealQL Value.
						let msg = surrealdb_core::rpc::format::cbor::decode(msg.as_slice())?;
						// Then we convert the SurrealQL to JSON.
						let msg = msg.into_json_value().unwrap();
						// Then output the response.
						debug!("Received message: {msg:?}");
						Ok(Some(msg))
					}
					_ => Err("Expected to receive a text message".to_string().into()),
				}
			}
			Message::Close(_) => Err("Socket closed unexpectedly".to_string().into()),
			_ => Ok(None),
		}
	}

	async fn send_msg(
		stream: &mut WsStream,
		id: u64,
		format: Format,
		method: &str,
		args: serde_json::Value,
	) -> Result<()> {
		let msg = json!({
			"id": id,
			"method": method,
			"params": args,
		});

		let msg = Self::to_msg(format, &msg)?;

		match tokio::time::timeout(Duration::from_millis(500), stream.send(msg)).await {
			Ok(Ok(_)) => Ok(()),
			Ok(Err(e)) => Err(format!("error sending message: {e}").into()),
			Err(_) => Err("sending message timed-out".to_string().into()),
		}
	}

	async fn ws_task(
		mut recv: Receiver<SocketMsg>,
		mut stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
		other: Sender<serde_json::Value>,
		format: Format,
	) -> Result<()> {
		let mut next_id: u64 = 0;

		let mut awaiting = HashMap::new();

		loop {
			tokio::select! {
				msg = recv.recv() => {
					let Some(msg) = msg else {
						return Ok(());
					};
					match msg{
						SocketMsg::SendAwait { method, args, channel } => {
							let id = next_id;
							next_id += 1;
							awaiting.insert(id,channel);
							Self::send_msg(&mut stream,id,format,&method, args).await?;
						},
						SocketMsg::Send { method, args } => {
							let id = next_id;
							next_id += 1;
							Self::send_msg(&mut stream,id,format,&method, args).await?;
						},
						SocketMsg::Close{ channel } => {
							stream.close(None).await?;
							let _ = channel.send(());
							return Ok(());
						}
					}
				}
				res = stream.next() => {
					let Some(res) = res else {
						return Ok(());
					};
					let res = res?;
					let Some(res) = Self::from_msg(format,res)? else {
						continue;
					};

					// does the response have an id.
					match res.get("id").and_then(|x| x.as_u64()).and_then(|x| awaiting.remove(&x)){ Some(sender) => {
						let _ = sender.send(res);
					} _ => if (other.send(res).await).is_err(){
						 return Err("main thread quit unexpectedly".to_string().into())
					 }}
				}
			}
		}
	}

	/// Send a text or binary message and receive a reponse from the WebSocket
	/// server
	pub async fn send_request(
		&self,
		method: &str,
		params: serde_json::Value,
	) -> Result<serde_json::Value> {
		let (send, recv) = oneshot::channel();
		if (self
			.sender
			.send(SocketMsg::SendAwait {
				method: method.to_string(),
				args: params,
				channel: send,
			})
			.await)
			.is_err()
		{
			return Err("websocket task quit unexpectedly".to_string().into());
		}

		match recv.await {
			Ok(x) => Ok(x),
			Err(_) => Err("websocket task dropped request unexpectedly".to_string().into()),
		}
	}

	/// When testing Live Queries, we may receive multiple messages unordered.
	/// This method captures all the expected messages before the given timeout.
	/// The result can be inspected later on to find the desired message.
	pub async fn receive_other_message(&mut self) -> Result<serde_json::Value> {
		match self.other_messages.recv().await {
			Some(x) => Ok(x),
			None => Err("websocket task quit unexpectedly".to_string().into()),
		}
	}

	pub async fn receive_all_other_messages(
		&mut self,
		amount: usize,
		timeout: Duration,
	) -> Result<Vec<serde_json::Value>> {
		tokio::time::timeout(timeout, async {
			let mut res = Vec::with_capacity(amount);
			for _ in 0..amount {
				res.push(self.receive_other_message().await?)
			}
			Ok(res)
		})
		.await?
	}

	/// Send a USE message to the server and check the response
	pub async fn send_message_use(
		&mut self,
		ns: Option<&str>,
		db: Option<&str>,
	) -> Result<serde_json::Value> {
		// Send message and receive response
		let msg = self.send_request("use", json!([ns, db])).await?;
		// Check response message structure
		match msg.as_object() {
			Some(obj) if obj.keys().all(|k| ["id", "error"].contains(&k.as_str())) => {
				Err(format!("unexpected error from query request: {:?}", obj.get("error")).into())
			}
			Some(obj) if obj.keys().all(|k| ["id", "result"].contains(&k.as_str())) => Ok(obj
				.get("result")
				.ok_or(TestError::AssertionError {
					message: format!(
						"expected a result from the received object, got this instead: {obj:?}"
					),
				})?
				.to_owned()),
			_ => {
				error!("{:?}", msg.as_object().unwrap().keys().collect::<Vec<_>>());
				Err(format!("unexpected response: {msg:?}").into())
			}
		}
	}

	/// Send a generic query message to the server and check the response
	pub async fn send_message_query(&mut self, query: &str) -> Result<Vec<serde_json::Value>> {
		// Send message and receive response
		let msg = self.send_request("query", json!([query])).await?;
		// Check response message structure
		match msg.as_object() {
			Some(obj) if obj.keys().all(|k| ["id", "error"].contains(&k.as_str())) => {
				Err(format!("unexpected error from query request: {:?}", obj.get("error")).into())
			}
			Some(obj) if obj.keys().all(|k| ["id", "result"].contains(&k.as_str())) => Ok(obj
				.get("result")
				.ok_or(TestError::AssertionError {
					message: format!("expected a result from the received object, got this instead: {obj:?}"),
				})?
				.as_array()
				.ok_or(TestError::AssertionError {
					message: format!("expected the result object to be an array for the received ws message, got this instead: {:?}", obj.get("result")).to_string(),
				})?
				.to_owned()),
			_ => {
				error!("{:?}", msg.as_object().unwrap().keys().collect::<Vec<_>>());
				Err(format!("unexpected response: {msg:?}").into())
			}
		}
	}

	/// Send a signin authentication query message to the server and check the
	/// response
	pub async fn send_message_signin(
		&mut self,
		user: &str,
		pass: &str,
		ns: Option<&str>,
		db: Option<&str>,
		ac: Option<&str>,
	) -> Result<String> {
		// Send message and receive response
		let msg = self
			.send_request(
				"signin",
				json!([SigninParams {
					user,
					pass,
					ns,
					db,
					ac
				}]),
			)
			.await?;
		// Check response message structure
		match msg.as_object() {
			Some(obj) if obj.keys().all(|k| ["id", "error"].contains(&k.as_str())) => {
				Err(format!("unexpected error from query request: {:?}", obj.get("error")).into())
			}
			Some(obj) if obj.keys().all(|k| ["id", "result"].contains(&k.as_str())) => Ok(obj
				.get("result")
				.ok_or(TestError::AssertionError {
					message: format!("expected a result from the received object, got this instead: {obj:?}"),
				})?
				.as_str()
				.ok_or(TestError::AssertionError {
					message: format!("expected the result object to be a string for the received ws message, got this instead: {:?}", obj.get("result")).to_string(),
				})?
				.to_owned()),
			_ => {
				error!("{:?}", msg.as_object().unwrap().keys().collect::<Vec<_>>());
				Err(format!("unexpected response: {msg:?}").into())
			}
		}
	}

	pub async fn send_message_run(
		&mut self,
		fn_name: &str,
		version: Option<&str>,
		args: Vec<serde_json::Value>,
	) -> Result<serde_json::Value> {
		// Send message and receive response
		let msg = self.send_request("run", json!([fn_name, version, args])).await?;
		// Check response message structure
		match msg.as_object() {
			Some(obj) if obj.keys().all(|k| ["id", "error"].contains(&k.as_str())) => {
				Err(format!("unexpected error from query request: {:?}", obj.get("error")).into())
			}
			Some(obj) if obj.keys().all(|k| ["id", "result"].contains(&k.as_str())) => Ok(obj
				.get("result")
				.ok_or(TestError::AssertionError {
					message: format!(
						"expected a result from the received object, got this instead: {obj:?}"
					),
				})?
				.to_owned()),
			_ => {
				error!("{:?}", msg.as_object().unwrap().keys().collect::<Vec<_>>());
				Err(format!("unexpected response: {msg:?}").into())
			}
		}
	}

	pub async fn send_message_relate(
		&mut self,
		from: serde_json::Value,
		kind: serde_json::Value,
		with: serde_json::Value,
		content: Option<serde_json::Value>,
	) -> Result<serde_json::Value> {
		// Send message and receive response
		let msg = if let Some(content) = content {
			self.send_request("relate", json!([from, kind, with, content])).await?
		} else {
			self.send_request("relate", json!([from, kind, with])).await?
		};
		// Check response message structure
		match msg.as_object() {
			Some(obj) if obj.keys().all(|k| ["id", "error"].contains(&k.as_str())) => {
				Err(format!("unexpected error from query request: {:?}", obj.get("error")).into())
			}
			Some(obj) if obj.keys().all(|k| ["id", "result"].contains(&k.as_str())) => Ok(obj
				.get("result")
				.ok_or(TestError::AssertionError {
					message: format!(
						"expected a result from the received object, got this instead: {obj:?}"
					),
				})?
				.to_owned()),
			_ => {
				error!("{:?}", msg.as_object().unwrap().keys().collect::<Vec<_>>());
				Err(format!("unexpected response: {msg:?}").into())
			}
		}
	}
}
