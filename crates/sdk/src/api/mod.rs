//! Functionality for connecting to local and remote databases

use crate::Result;
use anyhow::ensure;
use anyhow::Context;
use method::BoxFuture;
use semver::BuildMetadata;
use semver::Version;
use semver::VersionReq;
use surrealdb_protocol::proto::rpc::v1::surreal_db_service_client::SurrealDbServiceClient;
use std::fmt;
use std::fmt::Debug;
use std::future::IntoFuture;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::watch;

use tonic::transport::Uri;
use hyper_util::rt::TokioIo;
use tower::service_fn;
use anyhow::bail;
use std::collections::HashSet;
use tonic::codegen::{Body, StdError};
use tokio_util::bytes::Bytes;
use tonic::body::BoxBody;
use tonic::client::GrpcService;



// impl<T> Connection for T
// where
//     T: GrpcService<BoxBody> + Clone + Send + 'static,
//     T::Error: Into<StdError>,
//     T::ResponseBody: Body<Data = tokio_util::bytes::Bytes> + Send + 'static,
//     <T::ResponseBody as Body>::Error: Into<StdError> + Send,
// {
// }

// impl Connection for tokio::io::DuplexStream {}


macro_rules! transparent_wrapper{
	(
		$(#[$m:meta])*
		$vis:vis struct $name:ident($field_vis:vis $inner:ty)
	) => {
		$(#[$m])*
		#[repr(transparent)]
		$vis struct $name($field_vis $inner);

		#[allow(dead_code)]
		impl $name{
			#[doc(hidden)]
			pub fn from_inner(inner: $inner) -> Self{
				$name(inner)
			}

			#[doc(hidden)]
			pub fn from_inner_ref(inner: &$inner) -> &Self{
				unsafe{
					std::mem::transmute::<&$inner,&$name>(inner)
				}
			}

			#[doc(hidden)]
			pub fn from_inner_mut(inner: &mut $inner) -> &mut Self{
				unsafe{
					std::mem::transmute::<&mut $inner,&mut $name>(inner)
				}
			}

			#[doc(hidden)]
			pub fn into_inner(self) -> $inner{
				self.0
			}

			#[doc(hidden)]
			pub fn into_inner_ref(&self) -> &$inner{
				&self.0
			}

			#[doc(hidden)]
			pub fn into_inner_mut(&mut self) -> &mut $inner{
				&mut self.0
			}
		}

		impl std::fmt::Display for $name{
			fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result{
				self.0.fmt(fmt)
			}
		}
		impl std::fmt::Debug for $name{
			fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result{
				self.0.fmt(fmt)
			}
		}
	};
}

macro_rules! impl_serialize_wrapper {
	($ty:ty) => {
		impl ::revision::Revisioned for $ty {
			fn revision() -> u16 {
				Value::revision()
			}

			fn serialize_revisioned<W: std::io::Write>(
				&self,
				w: &mut W,
			) -> std::result::Result<(), revision::Error> {
				self.0.serialize_revisioned(w)
			}

			fn deserialize_revisioned<R: std::io::Read>(
				r: &mut R,
			) -> std::result::Result<Self, revision::Error>
			where
				Self: Sized,
			{
				::revision::Revisioned::deserialize_revisioned(r).map(Self::from_inner)
			}
		}

		impl ::serde::Serialize for $ty {
			fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
			where
				S: ::serde::ser::Serializer,
			{
				self.0.serialize(serializer)
			}
		}

		impl<'de> ::serde::de::Deserialize<'de> for $ty {
			fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
			where
				D: ::serde::de::Deserializer<'de>,
			{
				Ok(Self::from_inner(::serde::de::Deserialize::deserialize(deserializer)?))
			}
		}
	};
}

pub mod engine;
pub mod err;
#[cfg(feature = "protocol-http")]
pub mod headers;
pub mod method;
pub mod opt;
pub mod value;

mod conn;

use self::err::Error;
use self::opt::Endpoint;
use self::opt::EndpointKind;
use self::opt::WaitFor;

pub use method::query::QueryResults;

// Channel for waiters
type Waiter = (watch::Sender<Option<WaitFor>>, watch::Receiver<Option<WaitFor>>);

const SUPPORTED_VERSIONS: (&str, &str) = (">=1.2.0, <4.0.0", "20230701.55918b7c");


// /// The future returned when creating a new SurrealDB instance
// #[derive(Debug)]
// #[must_use = "futures do nothing unless you `.await` or poll them"]
// pub struct Connect<Response> {
// 	surreal: Surreal,
// 	address: Result<Endpoint>,
// 	capacity: usize,
// 	response_type: PhantomData<Response>,
// }

// impl<R> Connect<R>
// {
// 	/// Sets the maximum capacity of the connection
// 	///
// 	/// This is used to set bounds of the channels used internally
// 	/// as well set the capacity of the `HashMap` used for routing
// 	/// responses in case of the WebSocket client.
// 	///
// 	/// Setting this capacity to `0` (the default) means that
// 	/// unbounded channels will be used. If your queries per second
// 	/// are so high that the client is running out of memory,
// 	/// it might be helpful to set this to a number that works best
// 	/// for you.
// 	///
// 	/// # Examples
// 	///
// 	/// ```no_run
// 	/// # #[tokio::main]
// 	/// # async fn main() -> surrealdb::Result<()> {
// 	/// use surrealdb::engine::remote::ws::Ws;
// 	/// use surrealdb::Surreal;
// 	///
// 	/// let db = Surreal::new::<Ws>("localhost:8000")
// 	///     .with_capacity(100_000)
// 	///     .await?;
// 	/// # Ok(())
// 	/// # }
// 	/// ```
// 	pub const fn with_capacity(mut self, capacity: usize) -> Self {
// 		self.capacity = capacity;
// 		self
// 	}
// }

// impl<Client> IntoFuture for Connect<Surreal>
// {
// 	type Output = Result<Surreal>;
// 	type IntoFuture = BoxFuture<'static, Self::Output>;

// 	fn into_future(self) -> Self::IntoFuture {
// 		Box::pin(async move {
// 			let endpoint = self.address?;
// 			let endpoint_kind = EndpointKind::from(endpoint.url.scheme());
// 			let client = Client::connect(endpoint, self.capacity).await?;
// 			if endpoint_kind.is_remote() {
// 				match client.version().await {
// 					Ok(mut version) => {
// 						// we would like to be able to connect to pre-releases too
// 						version.pre = Default::default();
// 						client.check_server_version(&version).await?;
// 					}
// 					// TODO(raphaeldarley) don't error if Method Not allowed
// 					Err(e) => return Err(e),
// 				}
// 			}
// 			// Both ends of the channel are still alive at this point
// 			client.client.waiter.0.send(Some(WaitFor::Connection)).ok();
// 			Ok(client)
// 		})
// 	}
// }

// impl<Client> IntoFuture for Connect<Client, ()>
// where
// 	Client: Connection,
// {
// 	type Output = Result<()>;
// 	type IntoFuture = BoxFuture<'static, Self::Output>;

// 	fn into_future(self) -> Self::IntoFuture {
// 		Box::pin(async move {
// 			// Avoid establishing another connection if already connected
// 			ensure!(self.surreal.client.router.get().is_none(), Error::AlreadyConnected);
// 			let endpoint = self.address?;
// 			let endpoint_kind = EndpointKind::from(endpoint.url.scheme());
// 			let client = Client::connect(endpoint, self.capacity).await?;
// 			if endpoint_kind.is_remote() {
// 				match client.version().await {
// 					Ok(mut version) => {
// 						// we would like to be able to connect to pre-releases too
// 						version.pre = Default::default();
// 						client.check_server_version(&version).await?;
// 					}
// 					// TODO(raphaeldarley) don't error if Method Not allowed
// 					Err(e) => return Err(e),
// 				}
// 			}
// 			let inner =
// 				Arc::into_inner(client.client).expect("new connection to have no references");
// 			let router = inner.router.into_inner().expect("router to be set");
// 			self.surreal.client.router.set(router).map_err(|_| Error::AlreadyConnected)?;
// 			// Both ends of the channel are still alive at this point
// 			self.surreal.client.waiter.0.send(Some(WaitFor::Connection)).ok();
// 			Ok(())
// 		})
// 	}
// }

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub(crate) enum ExtraFeatures {
	Backup,
	LiveQueries,
}


#[cfg(not(target_family = "wasm"))]
type Channel = tonic::transport::Channel;


/// A database client instance for embedded or remote databases.
///
/// See [Running SurrealDB embedded in Rust](crate#running-surrealdb-embedded-in-rust)
/// for tips on how to optimize performance for the client when working
/// with embedded instances.
#[derive(Clone)]
pub struct Surreal
{
	client: SurrealDbServiceClient<Channel>,
	endpoint: Endpoint,
}

impl Surreal
{

	/// Connects to a local or remote database endpoint
	///
	/// # Examples
	///
	/// ```no_run
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::remote::ws::{Ws, Wss};
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// // Connect to a local endpoint
	/// let db = Surreal::new::<Ws>("localhost:8000").await?;
	///
	/// // Connect to a remote endpoint
	/// let db = Surreal::new::<Wss>("cloud.surrealdb.com").await?;
	/// #
	/// # Ok(())
	/// # }
	/// ```
	pub fn new(dst: Channel, endpoint: Endpoint) -> Self
	{
		Self {
			client: SurrealDbServiceClient::new(dst),
			endpoint,
		}
	}

	pub async fn connect(endpoint: impl TryInto<Endpoint, Error = anyhow::Error>, capacity: usize) -> Result<Self> {
		let endpoint = endpoint.try_into().context("Failed to parse endpoint")?;
		let config = endpoint.config.clone();
		let mut features: HashSet<ExtraFeatures> = HashSet::new();

		let endpoint_kind = endpoint.url.scheme().parse::<EndpointKind>()?;
		match endpoint_kind {
			opt::EndpointKind::FoundationDb => {
				#[cfg(kv_fdb)]
				{
					let (client, server) = tokio::io::duplex(capacity);
					features.insert(ExtraFeatures::Backup);
					features.insert(ExtraFeatures::LiveQueries);
					tokio::spawn(engine::local::native::serve(server, endpoint.clone()));
					Ok(Surreal::new(client, endpoint))
				}

				#[cfg(not(kv_fdb))]
				bail!("Cannot connect to the `foundationdb` storage engine as it is not enabled in this build of SurrealDB".to_owned());
			}

			opt::EndpointKind::Memory => {
				#[cfg(feature = "kv-mem")]
				{
					let (client, server) = tokio::io::duplex(capacity);
					let mut client = Some(client);
					let channel = tonic::transport::Endpoint::try_from(endpoint.url.to_string())?
						.connect_with_connector(service_fn(move |_: Uri| {
							let mut client = client.take();
							async move {
								if let Some(client) = client {
									Ok(hyper_util::rt::TokioIo::new(client))
								} else {
									Err(std::io::Error::other("Client was already taken"))
								}
							}
						}))
						.await?;
					features.insert(ExtraFeatures::Backup);
					features.insert(ExtraFeatures::LiveQueries);
					tokio::spawn(engine::local::native::serve(server, endpoint.clone()));
					Ok(Surreal::new(channel, endpoint))
				}

				#[cfg(not(feature = "kv-mem"))]
				bail!("Cannot connect to the `memory` storage engine as it is not enabled in this build of SurrealDB".to_owned());
			}

			opt::EndpointKind::File | opt::EndpointKind::RocksDb => {
				#[cfg(feature = "kv-rocksdb")]
				{
					let (client, server) = tokio::io::duplex(capacity);
					features.insert(ExtraFeatures::Backup);
					features.insert(ExtraFeatures::LiveQueries);
					tokio::spawn(engine::local::native::serve(server, endpoint.clone()));
					Ok(Surreal::new(client, endpoint))
				}

				#[cfg(not(feature = "kv-rocksdb"))]
				bail!("Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned());
			}

			opt::EndpointKind::TiKv => {
				#[cfg(feature = "kv-tikv")]
				{
					let (client, server) = tokio::io::duplex(capacity);
					features.insert(ExtraFeatures::Backup);
					features.insert(ExtraFeatures::LiveQueries);
					tokio::spawn(engine::local::native::serve(server, endpoint.clone()));
					Ok(Surreal::new(client, endpoint))
				}

				#[cfg(not(feature = "kv-tikv"))]
				bail!("Cannot connect to the `tikv` storage engine as it is not enabled in this build of SurrealDB".to_owned());
			}

			opt::EndpointKind::SurrealKv | opt::EndpointKind::SurrealKvVersioned => {
				#[cfg(feature = "kv-surrealkv")]
				{
					let (client, server) = tokio::io::duplex(capacity);
					features.insert(ExtraFeatures::Backup);
					features.insert(ExtraFeatures::LiveQueries);
					tokio::spawn(engine::local::native::serve(server, endpoint.clone()));
					Ok(Surreal::new(client, endpoint))
				}

				#[cfg(not(feature = "kv-surrealkv"))]
				bail!("Cannot connect to the `surrealkv` storage engine as it is not enabled in this build of SurrealDB".to_owned());
			}

			opt::EndpointKind::Http | opt::EndpointKind::Https |
			opt::EndpointKind::Ws | opt::EndpointKind::Wss |
			opt::EndpointKind::Grpc | opt::EndpointKind::Grpcs => {
				#[cfg(feature = "protocol-ws")]
				{
					// features.insert(ExtraFeatures::LiveQueries);
					// let mut endpoint = address;
					// endpoint.url = endpoint.url.join(engine::remote::grpc::PATH)?;
					// #[cfg(any(feature = "native-tls", feature = "rustls"))]
					// let maybe_connector = endpoint.config.tls_config.clone().map(Connector::from);
					// #[cfg(not(any(feature = "native-tls", feature = "rustls")))]
					// let maybe_connector = None;

					// let config = WebSocketConfig {
					// 	max_message_size: Some(engine::remote::grpc::native::MAX_MESSAGE_SIZE),
					// 	max_frame_size: Some(engine::remote::grpc::native::MAX_FRAME_SIZE),
					// 	max_write_buffer_size: engine::remote::grpc::native::MAX_MESSAGE_SIZE,
					// 	..Default::default()
					// };
					// let socket = engine::remote::grpc::native::connect(
					// 	&endpoint,
					// 	Some(config),
					// 	maybe_connector.clone(),
					// )
					// .await?;
					// tokio::spawn(engine::remote::grpc::native::run_router(
					// 	endpoint,
					// 	maybe_connector,
					// 	capacity,
					// 	config,
					// 	socket,
					// 	route_rx,
					// ));


					let channel = tonic::transport::Endpoint::new(endpoint.url.to_string())?
						.connect()
						.await?;
					Ok(Surreal::new(channel, endpoint))
				}

				#[cfg(not(feature = "protocol-ws"))]
				bail!("Cannot connect to the `WebSocket` remote engine as it is not enabled in this build of SurrealDB".to_owned());
			}
		}
	}

	pub fn is_local(&self) -> bool {
		self.endpoint.parse_kind().unwrap().is_local()
	}

	async fn check_server_version(&self, version: &Version) -> Result<()> {
		let (versions, build_meta) = SUPPORTED_VERSIONS;
		// invalid version requirements should be caught during development
		let req = VersionReq::parse(versions).expect("valid supported versions");
		let build_meta = BuildMetadata::new(build_meta).expect("valid supported build metadata");
		let server_build = &version.build;
		ensure!(
			req.matches(version),
			Error::VersionMismatch {
				server_version: version.clone(),
				supported_versions: versions.to_owned(),
			}
		);

		ensure!(
			server_build.is_empty() || server_build >= &build_meta,
			Error::BuildMetadataMismatch {
				server_metadata: server_build.clone(),
				supported_metadata: build_meta,
			}
		);
		Ok(())
	}
}

impl Debug for Surreal
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Surreal")
			.finish()
	}
}
