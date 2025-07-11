//! Functionality for connecting to local and remote databases

use crate::Result;
use anyhow::Context;
use std::fmt;
use std::fmt::Debug;
use surrealdb_protocol::proto::rpc::v1::surreal_db_service_client::SurrealDbServiceClient;

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

use self::opt::Endpoint;
use self::opt::EndpointKind;

pub use method::query::QueryResults;

#[cfg(not(target_family = "wasm"))]
type Channel = tonic::transport::Channel;

/// A database client instance for embedded or remote databases.
///
/// See [Running SurrealDB embedded in Rust](crate#running-surrealdb-embedded-in-rust)
/// for tips on how to optimize performance for the client when working
/// with embedded instances.
#[derive(Clone)]
pub struct Surreal {
	client: SurrealDbServiceClient<Channel>,
	endpoint: Endpoint,
}

impl Surreal {
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
	pub fn new(dst: Channel, endpoint: Endpoint) -> Self {
		Self {
			client: SurrealDbServiceClient::new(dst),
			endpoint,
		}
	}

	pub async fn connect(endpoint: impl TryInto<Endpoint, Error = anyhow::Error>) -> Result<Self> {
		let endpoint = endpoint.try_into().context("Failed to parse endpoint")?;

		let endpoint_kind = endpoint.url.scheme().parse::<EndpointKind>()?;

		if endpoint_kind.is_local() {
			#[cfg(not(any(
				kv_fdb,
				feature = "kv-mem",
				feature = "kv-tikv",
				feature = "kv-rocksdb",
				feature = "kv-indxdb",
				feature = "kv-surrealkv",
			)))]
			{
				return Err(anyhow::anyhow!(
					"Local engine not supported, must enable one of the following features: kv-fdb, kv-mem, kv-tikv, kv-rocksdb, kv-indxdb, kv-surrealkv"
				));
			}

			#[cfg(any(
				kv_fdb,
				feature = "kv-mem",
				feature = "kv-tikv",
				feature = "kv-rocksdb",
				feature = "kv-indxdb",
				feature = "kv-surrealkv",
			))]
			{
				let (client, server) = tokio::io::duplex(64 * 1024);
				let mut client = Some(client);
				let channel = tonic::transport::Endpoint::try_from(endpoint.url.to_string())?
					.connect_with_connector(tower::service_fn(move |_: http::Uri| {
						let client = client.take();
						async move {
							if let Some(client) = client {
								Ok(hyper_util::rt::TokioIo::new(client))
							} else {
								Err(std::io::Error::other("Client was already taken"))
							}
						}
					}))
					.await?;

				tokio::spawn(engine::local::native::serve(server, endpoint.clone()));
				Ok(Surreal::new(channel, endpoint))
			}
		} else {
			let channel =
				tonic::transport::Endpoint::new(endpoint.url.to_string())?.connect().await?;
			Ok(Surreal::new(channel, endpoint))
		}
	}

	pub fn is_local(&self) -> bool {
		self.endpoint.parse_kind().unwrap().is_local()
	}
}

impl Debug for Surreal {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Surreal").finish()
	}
}
