use crate::api::Result;
use crate::api::opt::Endpoint;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::api::opt::Tls;
use futures::stream::{SplitSink, SplitStream};
use tokio::net::TcpStream;
use tokio_tungstenite::Connector;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

pub(crate) const MAX_MESSAGE_SIZE: usize = 64 << 20; // 64 MiB
pub(crate) const MAX_FRAME_SIZE: usize = 16 << 20; // 16 MiB
pub(crate) const WRITE_BUFFER_SIZE: usize = 128000; // tungstenite default
pub(crate) const MAX_WRITE_BUFFER_SIZE: usize = WRITE_BUFFER_SIZE + MAX_MESSAGE_SIZE; // Recommended max according to tungstenite docs
pub(crate) const NAGLE_ALG: bool = false;

type MessageSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type MessageStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

#[cfg(any(feature = "native-tls", feature = "rustls"))]
impl From<Tls> for Connector {
	fn from(tls: Tls) -> Self {
		match tls {
			#[cfg(feature = "native-tls")]
			Tls::Native(config) => Self::NativeTls(config),
			#[cfg(feature = "rustls")]
			Tls::Rust(config) => Self::Rustls(std::sync::Arc::new(config)),
		}
	}
}

pub(crate) async fn connect(
	endpoint: &Endpoint,
	config: Option<WebSocketConfig>,
	#[cfg_attr(not(any(feature = "native-tls", feature = "rustls")), expect(unused_variables))]
	maybe_connector: Option<Connector>,
) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
	let mut request = (&endpoint.url).into_client_request()?;

	request
		.headers_mut()
		.insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static(super::REVISION_HEADER));

	#[cfg(any(feature = "native-tls", feature = "rustls"))]
	let (socket, _) = tokio_tungstenite::connect_async_tls_with_config(
		request,
		config,
		NAGLE_ALG,
		maybe_connector,
	)
	.await?;

	#[cfg(not(any(feature = "native-tls", feature = "rustls")))]
	let (socket, _) = tokio_tungstenite::connect_async_with_config(request, config, NAGLE_ALG).await?;

	Ok(socket)
}

#[cfg(test)]
mod tests {
	use super::serialize_flatbuffers;
	use bincode::Options;
	use flate2::Compression;
	use flate2::write::GzEncoder;
	use rand::{Rng, thread_rng};
	use std::io::Write;
	use std::time::SystemTime;
	use surrealdb_core::expr::{Array, Value};

	// #[test_log::test]
	// fn large_vector_serialisation_bench() {
	// 	//
	// 	let timed = |func: &dyn Fn() -> Vec<u8>| {
	// 		let start = SystemTime::now();
	// 		let r = func();
	// 		(start.elapsed().unwrap(), r)
	// 	};
	// 	//
	// 	let compress = |v: &Vec<u8>| {
	// 		let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
	// 		encoder.write_all(v).unwrap();
	// 		encoder.finish().unwrap()
	// 	};
	// 	// Generate a random vector
	// 	let vector_size = if cfg!(debug_assertions) {
	// 		200_000 // Debug is slow
	// 	} else {
	// 		2_000_000 // Release is fast
	// 	};
	// 	let mut vector: Vec<i32> = Vec::new();
	// 	let mut rng = thread_rng();
	// 	for _ in 0..vector_size {
	// 		vector.push(rng.r#gen());
	// 	}
	// 	//	Store the results
	// 	let mut results = vec![];
	// 	// Calculate the reference
	// 	let ref_payload;
	// 	let ref_compressed;
	// 	//
	// 	const BINCODE_REF: &str = "Bincode Vec<i32>";
	// 	const COMPRESSED_BINCODE_REF: &str = "Compressed Bincode Vec<i32>";
	// 	{
	// 		// Bincode Vec<i32>
	// 		let (duration, payload) = timed(&|| {
	// 			let mut payload = Vec::new();
	// 			bincode::options()
	// 				.with_fixint_encoding()
	// 				.serialize_into(&mut payload, &vector)
	// 				.unwrap();
	// 			payload
	// 		});
	// 		ref_payload = payload.len() as f32;
	// 		results.push((payload.len(), BINCODE_REF, duration, 1.0));

	// 		// Compressed bincode
	// 		let (compression_duration, payload) = timed(&|| compress(&payload));
	// 		let duration = duration + compression_duration;
	// 		ref_compressed = payload.len() as f32;
	// 		results.push((payload.len(), COMPRESSED_BINCODE_REF, duration, 1.0));
	// 	}
	// 	// Build the Value
	// 	let vector = Value::Array(Array::from(vector));
	// 	//
	// 	const BINCODE: &str = "Bincode Vec<Value>";
	// 	const COMPRESSED_BINCODE: &str = "Compressed Bincode Vec<Value>";
	// 	{
	// 		// Bincode Vec<i32>
	// 		let (duration, payload) = timed(&|| {
	// 			let mut payload = Vec::new();
	// 			bincode::options()
	// 				.with_varint_encoding()
	// 				.serialize_into(&mut payload, &vector)
	// 				.unwrap();
	// 			payload
	// 		});
	// 		results.push((payload.len(), BINCODE, duration, payload.len() as f32 / ref_payload));

	// 		// Compressed bincode
	// 		let (compression_duration, payload) = timed(&|| compress(&payload));
	// 		let duration = duration + compression_duration;
	// 		results.push((
	// 			payload.len(),
	// 			COMPRESSED_BINCODE,
	// 			duration,
	// 			payload.len() as f32 / ref_compressed,
	// 		));
	// 	}
	// 	const UNVERSIONED: &str = "Unversioned Vec<Value>";
	// 	const COMPRESSED_UNVERSIONED: &str = "Compressed Unversioned Vec<Value>";
	// 	{
	// 		// Unversioned
	// 		let (duration, payload) = timed(&|| serialize_proto(&vector).unwrap());
	// 		results.push((
	// 			payload.len(),
	// 			UNVERSIONED,
	// 			duration,
	// 			payload.len() as f32 / ref_payload,
	// 		));

	// 		// Compressed Versioned
	// 		let (compression_duration, payload) = timed(&|| compress(&payload));
	// 		let duration = duration + compression_duration;
	// 		results.push((
	// 			payload.len(),
	// 			COMPRESSED_UNVERSIONED,
	// 			duration,
	// 			payload.len() as f32 / ref_compressed,
	// 		));
	// 	}
	// 	//
	// 	const VERSIONED: &str = "Versioned Vec<Value>";
	// 	const COMPRESSED_VERSIONED: &str = "Compressed Versioned Vec<Value>";
	// 	{
	// 		// Versioned
	// 		let (duration, payload) = timed(&|| serialize_proto(&vector).unwrap());
	// 		results.push((payload.len(), VERSIONED, duration, payload.len() as f32 / ref_payload));

	// 		// Compressed Versioned
	// 		let (compression_duration, payload) = timed(&|| compress(&payload));
	// 		let duration = duration + compression_duration;
	// 		results.push((
	// 			payload.len(),
	// 			COMPRESSED_VERSIONED,
	// 			duration,
	// 			payload.len() as f32 / ref_compressed,
	// 		));
	// 	}
	// 	//
	// 	const CBOR: &str = "CBor Vec<Value>";
	// 	const COMPRESSED_CBOR: &str = "Compressed CBor Vec<Value>";
	// 	{
	// 		// CBor
	// 		let (duration, payload) = timed(&|| {
	// 			let cbor: Cbor = vector.clone().try_into().unwrap();
	// 			let mut res = Vec::new();
	// 			ciborium::into_writer(&cbor.0, &mut res).unwrap();
	// 			res
	// 		});
	// 		results.push((payload.len(), CBOR, duration, payload.len() as f32 / ref_payload));

	// 		// Compressed Cbor
	// 		let (compression_duration, payload) = timed(&|| compress(&payload));
	// 		let duration = duration + compression_duration;
	// 		results.push((
	// 			payload.len(),
	// 			COMPRESSED_CBOR,
	// 			duration,
	// 			payload.len() as f32 / ref_compressed,
	// 		));
	// 	}
	// 	// Sort the results by ascending size
	// 	results.sort_by(|(a, _, _, _), (b, _, _, _)| a.cmp(b));
	// 	for (size, name, duration, factor) in &results {
	// 		info!("{name} - Size: {size} - Duration: {duration:?} - Factor: {factor}");
	// 	}
	// 	// Check the expected sorted results
	// 	let results: Vec<&str> = results.into_iter().map(|(_, name, _, _)| name).collect();
	// 	assert_eq!(
	// 		results,
	// 		vec![
	// 			BINCODE_REF,
	// 			COMPRESSED_BINCODE_REF,
	// 			COMPRESSED_CBOR,
	// 			COMPRESSED_BINCODE,
	// 			COMPRESSED_UNVERSIONED,
	// 			CBOR,
	// 			COMPRESSED_VERSIONED,
	// 			BINCODE,
	// 			UNVERSIONED,
	// 			VERSIONED,
	// 		]
	// 	)
	// }
}
