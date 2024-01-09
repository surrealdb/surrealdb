use super::format::Format;
use crate::common::error::TestError;
use futures_util::{SinkExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::error::Error;
use std::time::Duration;
use surrealdb::sql::Value;
use tokio::net::TcpStream;
use tokio::time;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error};

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
	sc: Option<&'a str>,
}

pub struct Socket {
	pub stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

// pub struct Socket(pub WebSocketStream<MaybeTlsStream<TcpStream>>);

impl Socket {
	/// Close the connection with the WebSocket server
	pub async fn close(&mut self) -> Result<(), Box<dyn Error>> {
		Ok(self.stream.close(None).await?)
	}

	/// Connect to a WebSocket server using a specific format
	pub async fn connect(addr: &str, format: Option<Format>) -> Result<Self, Box<dyn Error>> {
		let url = format!("ws://{}/rpc", addr);
		let mut req = url.into_client_request().unwrap();
		if let Some(v) = format.map(|v| v.to_string()) {
			req.headers_mut().insert("Sec-WebSocket-Protocol", v.parse().unwrap());
		}
		let (stream, _) = connect_async(req).await?;
		Ok(Self {
			stream,
		})
	}

	/// Send a text or binary message to the WebSocket server
	pub async fn send_message(
		&mut self,
		format: Format,
		message: serde_json::Value,
	) -> Result<(), Box<dyn Error>> {
		let now = time::Instant::now();
		debug!("Sending message: {message}");
		// Format the message
		let msg = match format {
			Format::Json => Message::Text(serde_json::to_string(&message)?),
			Format::Cbor => {
				pub mod try_from_impls {
					include!("../../src/rpc/format/cbor/convert.rs");
				}
				// For tests we need to convert the serde_json::Value
				// to a SurrealQL value, so that record ids, uuids,
				// datetimes, and durations are stored properly.
				// First of all we convert the JSON type to a string.
				let json = message.to_string();
				// Then we parse the JSON in to SurrealQL.
				let surrealql = surrealdb::sql::value(&json)?;
				// Then we convert the SurrealQL in to CBOR.
				let cbor = try_from_impls::Cbor::try_from(surrealql)?;
				// Then serialize the CBOR as binary data.
				let mut output = Vec::new();
				ciborium::into_writer(&cbor.0, &mut output).unwrap();
				// THen output the message.
				Message::Binary(output)
			}
			Format::Pack => {
				pub mod try_from_impls {
					include!("../../src/rpc/format/msgpack/convert.rs");
				}
				// For tests we need to convert the serde_json::Value
				// to a SurrealQL value, so that record ids, uuids,
				// datetimes, and durations are stored properly.
				// First of all we convert the JSON type to a string.
				let json = message.to_string();
				// Then we parse the JSON in to SurrealQL.
				let surrealql = surrealdb::sql::value(&json)?;
				// Then we convert the SurrealQL in to MessagePack.
				let pack = try_from_impls::Pack::try_from(surrealql)?;
				// Then serialize the MessagePack as binary data.
				let mut output = Vec::new();
				rmpv::encode::write_value(&mut output, &pack.0).unwrap();
				// THen output the message.
				Message::Binary(output)
			}
		};
		// Send the message
		tokio::select! {
			_ = time::sleep(time::Duration::from_millis(500)) => {
				return Err("timeout after 500ms waiting for the request to be sent".into());
			}
			res = self.stream.send(msg) => {
				debug!("Message sent in {:?}", now.elapsed());
					if let Err(err) = res {
						return Err(format!("Error sending the message: {}", err).into());
					}
			}
		}
		Ok(())
	}

	/// Receive a text or binary message from the WebSocket server
	pub async fn receive_message(
		&mut self,
		format: Format,
	) -> Result<serde_json::Value, Box<dyn Error>> {
		let now = time::Instant::now();
		debug!("Receiving response...");
		loop {
			tokio::select! {
				_ = time::sleep(time::Duration::from_millis(5000)) => {
					return Err(Box::new(TestError::NetworkError {message: "timeout after 5s waiting for the response".to_string()}))
				}
				res = self.stream.try_next() => {
					match res {
						Ok(res) => match res {
							Some(Message::Text(msg)) => {
								debug!("Response {msg:?} received in {:?}", now.elapsed());
								match format {
									Format::Json => {
										let msg = serde_json::from_str(&msg)?;
										debug!("Received message: {msg}");
										return Ok(msg);
									},
									_ => {
										return Err("Expected to receive a binary message".to_string().into());
									}
								}
							},
							Some(Message::Binary(msg)) => {
								debug!("Response {msg:?} received in {:?}", now.elapsed());
								match format {
									Format::Cbor => {
										pub mod try_from_impls {
											include!("../../src/rpc/format/cbor/convert.rs");
										}
										// For tests we need to convert the binary data to
										// a serde_json::Value so that test assertions work.
										// First of all we deserialize the CBOR data.
										let msg: ciborium::Value = ciborium::from_reader(&mut msg.as_slice())?;
										// Then we convert it to a SurrealQL Value.
										let msg: Value = try_from_impls::Cbor(msg).try_into()?;
										// Then we convert the SurrealQL to JSON.
										let msg = msg.into_json();
										// Then output the response.
										debug!("Received message: {msg:?}");
										return Ok(msg);
									},
									Format::Pack => {
										pub mod try_from_impls {
											include!("../../src/rpc/format/msgpack/convert.rs");
										}
										// For tests we need to convert the binary data to
										// a serde_json::Value so that test assertions work.
										// First of all we deserialize the MessagePack data.
										let msg: rmpv::Value = rmpv::decode::read_value(&mut msg.as_slice())?;
										// Then we convert it to a SurrealQL Value.
										let msg: Value = try_from_impls::Pack(msg).try_into()?;
										// Then we convert the SurrealQL to JSON.
										let msg = msg.into_json();
										// Then output the response.
										debug!("Received message: {msg:?}");
										return Ok(msg);
									},
									_ => {
										return Err("Expected to receive a text message".to_string().into());
									}
								}
							},
							Some(_) => {
								continue;
							}
							None => {
								return Err("Expected to receive a message".to_string().into());
							}
						},
						Err(err) => {
							return Err(format!("Error receiving the message: {}", err).into());
						}
					}
				}
			}
		}
	}

	/// Send a text or binary message and receive a reponse from the WebSocket server
	pub async fn send_and_receive_message(
		&mut self,
		format: Format,
		message: serde_json::Value,
	) -> Result<serde_json::Value, Box<dyn Error>> {
		self.send_message(format, message).await?;
		self.receive_message(format).await
	}

	/// When testing Live Queries, we may receive multiple messages unordered.
	/// This method captures all the expected messages before the given timeout. The result can be inspected later on to find the desired message.
	pub async fn receive_all_messages(
		&mut self,
		format: Format,
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
				msg = self.receive_message(format) => {
					res.push(msg?);
				}
			}
			if res.len() == expected {
				return Ok(res);
			}
		}
	}

	/// Send a USE message to the server and check the response
	pub async fn send_message_use(
		&mut self,
		format: Format,
		ns: Option<&str>,
		db: Option<&str>,
	) -> Result<serde_json::Value, Box<dyn Error>> {
		// Generate an ID
		let id = uuid::Uuid::new_v4().to_string();
		// Construct message
		let msg = json!({
			"id": id,
			"method": "use",
			"params": [
				ns, db
			],
		});
		// Send message and receive response
		let msg = self.send_and_receive_message(format, msg).await?;
		// Check response message structure
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

	/// Send a generic query message to the server and check the response
	pub async fn send_message_query(
		&mut self,
		format: Format,
		query: &str,
	) -> Result<Vec<serde_json::Value>, Box<dyn Error>> {
		// Generate an ID
		let id = uuid::Uuid::new_v4().to_string();
		// Construct message
		let msg = json!({
			"id": id,
			"method": "query",
			"params": [query],
		});
		// Send message and receive response
		let msg = self.send_and_receive_message(format, msg).await?;
		// Check response message structure
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

	/// Send a signin authentication query message to the server and check the response
	pub async fn send_message_signin(
		&mut self,
		format: Format,
		user: &str,
		pass: &str,
		ns: Option<&str>,
		db: Option<&str>,
		sc: Option<&str>,
	) -> Result<String, Box<dyn Error>> {
		// Generate an ID
		let id = uuid::Uuid::new_v4().to_string();
		// Construct message
		let msg = json!({
			"id": id,
			"method": "signin",
			"params": [
				SigninParams { user, pass, ns, db, sc }
			],
		});
		// Send message and receive response
		let msg = self.send_and_receive_message(format, msg).await?;
		// Check response message structure
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
}
