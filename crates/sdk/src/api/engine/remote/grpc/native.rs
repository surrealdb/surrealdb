// #[cfg(any(feature = "native-tls", feature = "rustls"))]
// use crate::api::opt::Tls;
// use crate::api::opt::Endpoint;
// use crate::api::Result;
// use futures::stream::{SplitSink, SplitStream};
// use tokio::net::TcpStream;
// use tokio_tungstenite::Connector;
// use tokio_tungstenite::MaybeTlsStream;
// use tokio_tungstenite::WebSocketStream;
// use tokio_tungstenite::tungstenite::Message;

// // pub(crate) const MAX_MESSAGE_SIZE: usize = 64 << 20; // 64 MiB
// // pub(crate) const MAX_FRAME_SIZE: usize = 16 << 20; // 16 MiB
// // pub(crate) const WRITE_BUFFER_SIZE: usize = 128000; // tungstenite default
// // pub(crate) const MAX_WRITE_BUFFER_SIZE: usize = WRITE_BUFFER_SIZE + MAX_MESSAGE_SIZE; // Recommended max according to tungstenite docs
// // pub(crate) const NAGLE_ALG: bool = false;

// // type MessageSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
// // type MessageStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

// #[cfg(any(feature = "native-tls", feature = "rustls"))]
// impl From<Tls> for Connector {
// 	fn from(tls: Tls) -> Self {
// 		match tls {
// 			#[cfg(feature = "native-tls")]
// 			Tls::Native(config) => Self::NativeTls(config),
// 			#[cfg(feature = "rustls")]
// 			Tls::Rust(config) => Self::Rustls(std::sync::Arc::new(config)),
// 		}
// 	}
// }

// pub(crate) async fn connect(
// 	endpoint: &Endpoint,
// 	config: Option<WebSocketConfig>,
// 	#[cfg_attr(not(any(feature = "native-tls", feature = "rustls")), expect(unused_variables))]
// 	maybe_connector: Option<Connector>,
// ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
// 	let mut request = (&endpoint.url).into_client_request()?;

// 	request
// 		.headers_mut()
// 		.insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static(super::REVISION_HEADER));

// 	#[cfg(any(feature = "native-tls", feature = "rustls"))]
// 	let (socket, _) = tokio_tungstenite::connect_async_tls_with_config(
// 		request,
// 		config,
// 		NAGLE_ALG,
// 		maybe_connector,
// 	)
// 	.await?;

// 	#[cfg(not(any(feature = "native-tls", feature = "rustls")))]
// 	let (socket, _) = tokio_tungstenite::connect_async_with_config(request, config, NAGLE_ALG).await?;

// 	Ok(socket)
// }
