//! The RPC WebSocket: the upgrade of `/rpc` and the socket it yields.
//!
//! [`RpcUpgrade`] is axum's [`WebSocketUpgrade`] with the connection kept back: axum validates
//! the handshake, chooses the subprotocol and writes the `101` response exactly as it does for
//! any other route, and the upgraded connection is driven by a [`RpcSocket`] rather than by
//! axum's own socket.
//!
//! [`RpcSocket`] reads and writes axum [`Message`]s, as axum's socket does, but writes a data
//! message longer than [`FRAGMENT`] as a sequence of frames of at most that size (RFC 6455
//! §5.4), sliced from the message without copying it. A client reassembles them into the same
//! payload. The fragmentation bounds what the WebSocket's write buffer holds at once: a write the
//! socket takes only part of moves the rest of that buffer to its front, so a message buffered
//! whole is moved once per partial write, a cost that grows with the square of its size.

use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use axum::extract::FromRequestParts;
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::response::Response;
use bytes::Bytes;
use futures::{Sink, Stream};
use http::HeaderValue;
use http::request::Parts;
use hyper::upgrade::{OnUpgrade, Upgraded};
use hyper_util::rt::TokioIo;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::frame::coding::{CloseCode, Data, OpCode};
use tokio_tungstenite::tungstenite::protocol::frame::{Frame, Utf8Bytes};
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Role, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error as WsError, Message as WsMessage};

/// The largest frame a data message is written in: a longer message is fragmented.
pub(crate) const FRAGMENT: usize = 64 * 1024;

/// A WebSocket upgrade of `/rpc`: axum's extractor, with the upgraded connection kept for a
/// [`RpcSocket`].
pub(crate) struct RpcUpgrade {
	axum: WebSocketUpgrade,
	connection: OnUpgrade,
}

impl<S: Send + Sync> FromRequestParts<S> for RpcUpgrade {
	type Rejection = <WebSocketUpgrade as FromRequestParts<S>>::Rejection;

	async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
		// axum is given an upgrade that never completes in place of the connection's, so the
		// handshake is judged, and refused, exactly as axum judges it.
		let connection = parts.extensions.remove::<OnUpgrade>();
		if connection.is_some() {
			parts.extensions.insert(hyper::upgrade::on(http::Request::new(())));
		}
		let axum = WebSocketUpgrade::from_request_parts(parts, state).await?;
		// axum refuses a request with no upgrade to take, so one was taken here.
		let connection = connection.expect("axum accepted an upgrade the request does not carry");
		Ok(Self {
			axum,
			connection,
		})
	}
}

impl RpcUpgrade {
	/// Offer `protocols`, in order of preference, as [`WebSocketUpgrade::protocols`] does.
	pub(crate) fn protocols<I>(mut self, protocols: I) -> Self
	where
		I: IntoIterator,
		I::Item: Into<std::borrow::Cow<'static, str>>,
	{
		self.axum = self.axum.protocols(protocols);
		self
	}

	/// The `101` response, after which the connection is run as a [`RpcSocket`] with `config`
	/// by `callback`; `on_failed` is told when the connection cannot be upgraded.
	pub(crate) fn on_upgrade<C, Fut>(
		self,
		config: WebSocketConfig,
		on_failed: impl FnOnce(hyper::Error) + Send + 'static,
		callback: C,
	) -> Response
	where
		C: FnOnce(RpcSocket) -> Fut + Send + 'static,
		Fut: Future<Output = ()> + Send + 'static,
	{
		let protocol = self.axum.selected_protocol().cloned();
		let connection = self.connection;
		tokio::spawn(async move {
			let upgraded = match connection.await {
				Ok(upgraded) => upgraded,
				Err(err) => return on_failed(err),
			};
			let stream = WebSocketStream::from_raw_socket(
				TokioIo::new(upgraded),
				Role::Server,
				Some(config),
			)
			.await;
			callback(RpcSocket::new(stream, protocol)).await;
		});
		// axum's own upgrade is of the stand-in, which fails at once and is ignored.
		self.axum.on_failed_upgrade(|_| {}).on_upgrade(|_| async {})
	}
}

/// An upgraded `/rpc` connection, reading and writing axum [`Message`]s.
pub(crate) struct RpcSocket {
	inner: WebSocketStream<TokioIo<Upgraded>>,
	protocol: Option<HeaderValue>,
	/// Messages and frames accepted but not yet handed to `inner`, in order.
	queued: VecDeque<WsMessage>,
}

impl RpcSocket {
	fn new(inner: WebSocketStream<TokioIo<Upgraded>>, protocol: Option<HeaderValue>) -> Self {
		Self {
			inner,
			protocol,
			queued: VecDeque::new(),
		}
	}

	/// The subprotocol the upgrade chose.
	pub(crate) fn protocol(&self) -> Option<&HeaderValue> {
		self.protocol.as_ref()
	}

	/// Hand the queued messages to `inner`, as far as it takes them.
	fn poll_queued(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
		while let Some(message) = self.queued.pop_front() {
			if Pin::new(&mut self.inner).poll_ready(cx)?.is_pending() {
				self.queued.push_front(message);
				return Poll::Pending;
			}
			Pin::new(&mut self.inner).start_send(message)?;
		}
		Poll::Ready(Ok(()))
	}
}

/// `message` as the messages or frames that carry it: one message, or the frames of a data
/// message longer than [`FRAGMENT`].
fn outgoing(message: Message, into: &mut VecDeque<WsMessage>) -> Result<(), WsError> {
	let (data, opcode) = match message {
		Message::Binary(data) => (data, Data::Binary),
		Message::Text(text) => (Bytes::from(text), Data::Text),
		Message::Ping(data) => {
			into.push_back(WsMessage::Ping(data));
			return Ok(());
		}
		Message::Pong(data) => {
			into.push_back(WsMessage::Pong(data));
			return Ok(());
		}
		Message::Close(close) => {
			let close = match close {
				Some(close) => Some(CloseFrame {
					code: CloseCode::from(close.code),
					reason: utf8(Bytes::from(close.reason))?,
				}),
				None => None,
			};
			into.push_back(WsMessage::Close(close));
			return Ok(());
		}
	};
	if data.len() <= FRAGMENT {
		into.push_back(match opcode {
			Data::Text => WsMessage::Text(utf8(data)?),
			Data::Binary | Data::Continue | Data::Reserved(_) => WsMessage::Binary(data),
		});
		return Ok(());
	}
	let mut at = 0;
	while at < data.len() {
		let end = (at + FRAGMENT).min(data.len());
		let opcode = match at {
			0 => OpCode::Data(opcode),
			_ => OpCode::Data(Data::Continue),
		};
		into.push_back(WsMessage::Frame(Frame::message(
			data.slice(at..end),
			opcode,
			end == data.len(),
		)));
		at = end;
	}
	Ok(())
}

/// Text that axum has already checked, as tungstenite's.
fn utf8(bytes: Bytes) -> Result<Utf8Bytes, WsError> {
	Utf8Bytes::try_from(bytes).map_err(|_| WsError::Utf8(String::from("invalid UTF-8")))
}

/// `message` as axum reads it; `None` for a raw frame, which a read never yields.
fn incoming(message: WsMessage) -> Option<Message> {
	Some(match message {
		WsMessage::Text(text) => Message::Text(
			axum::extract::ws::Utf8Bytes::try_from(Bytes::from(text))
				.expect("tungstenite yields only valid UTF-8"),
		),
		WsMessage::Binary(data) => Message::Binary(data),
		WsMessage::Ping(data) => Message::Ping(data),
		WsMessage::Pong(data) => Message::Pong(data),
		WsMessage::Close(Some(close)) => Message::Close(Some(axum::extract::ws::CloseFrame {
			code: close.code.into(),
			reason: axum::extract::ws::Utf8Bytes::try_from(Bytes::from(close.reason))
				.expect("tungstenite yields only valid UTF-8"),
		})),
		WsMessage::Close(None) => Message::Close(None),
		WsMessage::Frame(_) => return None,
	})
}

impl Stream for RpcSocket {
	type Item = Result<Message, axum::Error>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		loop {
			match ready!(Pin::new(&mut self.inner).poll_next(cx)) {
				Some(Ok(message)) => {
					if let Some(message) = incoming(message) {
						return Poll::Ready(Some(Ok(message)));
					}
				}
				Some(Err(err)) => return Poll::Ready(Some(Err(axum::Error::new(err)))),
				None => return Poll::Ready(None),
			}
		}
	}
}

impl Sink<Message> for RpcSocket {
	type Error = axum::Error;

	fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		ready!(self.poll_queued(cx)).map_err(axum::Error::new)?;
		Pin::new(&mut self.inner).poll_ready(cx).map_err(axum::Error::new)
	}

	fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
		debug_assert!(self.queued.is_empty(), "`poll_ready` hands every queued frame on");
		outgoing(item, &mut self.queued).map_err(axum::Error::new)?;
		// The first message or frame goes to the socket now, as axum's socket sends a message
		// in `start_send`; the rest follow as the socket takes them.
		let first = self.queued.pop_front().expect("a message is at least one frame");
		Pin::new(&mut self.inner).start_send(first).map_err(axum::Error::new)
	}

	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		ready!(self.poll_queued(cx)).map_err(axum::Error::new)?;
		Pin::new(&mut self.inner).poll_flush(cx).map_err(axum::Error::new)
	}

	fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		ready!(self.poll_queued(cx)).map_err(axum::Error::new)?;
		Pin::new(&mut self.inner).poll_close(cx).map_err(axum::Error::new)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn frames(message: Message) -> Vec<WsMessage> {
		let mut out = VecDeque::new();
		outgoing(message, &mut out).unwrap();
		out.into()
	}

	/// The payload a client reassembles from `frames`, and each frame's opcode and finality.
	fn reassemble(frames: &[WsMessage]) -> (Vec<u8>, Vec<(OpCode, bool)>) {
		let mut payload = Vec::new();
		let mut heads = Vec::new();
		for frame in frames {
			let WsMessage::Frame(frame) = frame else {
				panic!("a fragmented message is all frames");
			};
			assert!(frame.payload().len() <= FRAGMENT);
			payload.extend_from_slice(frame.payload());
			heads.push((frame.header().opcode, frame.header().is_final));
		}
		(payload, heads)
	}

	#[test]
	fn a_message_of_one_fragment_is_sent_whole() {
		let data = Bytes::from(vec![7u8; FRAGMENT]);
		assert_eq!(frames(Message::Binary(data.clone())), vec![WsMessage::Binary(data)]);
		assert_eq!(frames(Message::Text("hi".into())), vec![WsMessage::Text("hi".into())]);
		assert_eq!(frames(Message::Ping(Bytes::new())), vec![WsMessage::Ping(Bytes::new())]);
	}

	#[test]
	fn a_longer_binary_message_is_fragmented_in_order() {
		let data: Vec<u8> = (0..FRAGMENT * 2 + 5).map(|i| i as u8).collect();
		let sent = frames(Message::Binary(Bytes::from(data.clone())));
		let (payload, heads) = reassemble(&sent);
		assert_eq!(payload, data);
		assert_eq!(
			heads,
			vec![
				(OpCode::Data(Data::Binary), false),
				(OpCode::Data(Data::Continue), false),
				(OpCode::Data(Data::Continue), true),
			]
		);
	}

	#[test]
	fn a_longer_text_message_is_fragmented_mid_character() {
		// Two-byte characters, so a fragment boundary falls inside one.
		let text = "é".repeat(FRAGMENT / 2 + 1);
		let sent = frames(Message::Text(text.as_str().into()));
		let (payload, heads) = reassemble(&sent);
		assert_eq!(payload, text.as_bytes());
		assert_eq!(
			heads,
			vec![(OpCode::Data(Data::Text), false), (OpCode::Data(Data::Continue), true)]
		);
	}

	#[test]
	fn a_close_keeps_its_code_and_reason() {
		let sent = frames(Message::Close(Some(axum::extract::ws::CloseFrame {
			code: 4000,
			reason: "bye".into(),
		})));
		assert_eq!(
			sent,
			vec![WsMessage::Close(Some(CloseFrame {
				code: CloseCode::from(4000),
				reason: "bye".into(),
			}))]
		);
	}
}
