#[cfg(storage)]
use std::path::PathBuf;
use std::time::Duration;

use surrealdb_iam::Level;
use surrealdb_rpc::capabilities::Capabilities as CoreCapabilities;

use crate::opt::capabilities::Capabilities;
use crate::opt::grpc::GrpcConfig;
use crate::opt::websocket::WebsocketConfig;

/// Configuration for a connection: credentials, capabilities, TLS, timeouts,
/// and the background-task intervals the local engines run on.
#[derive(Debug, Clone, Default)]
pub struct Config {
	pub(crate) ast_payload: bool,
	pub(crate) query_timeout: Option<Duration>,
	pub(crate) transaction_timeout: Option<Duration>,
	#[cfg(any(feature = "native-tls", feature = "rustls"))]
	pub(crate) tls_config: Option<super::Tls>,
	// Only used by the local engines
	// `Level::No` in this context means no authentication information was configured
	pub(crate) auth: Level,
	pub(crate) username: String,
	pub(crate) password: String,
	pub(crate) capabilities: CoreCapabilities,
	pub(crate) websocket: WebsocketConfig,
	pub(crate) grpc: GrpcConfig,
	#[cfg(storage)]
	pub(crate) temporary_directory: Option<PathBuf>,
	pub(crate) node_membership_refresh_interval: Option<Duration>,
	pub(crate) node_membership_check_interval: Option<Duration>,
	pub(crate) node_membership_cleanup_interval: Option<Duration>,
	pub(crate) changefeed_gc_interval: Option<Duration>,
}

impl Config {
	/// Create a default config that can be modified to configure a connection
	pub fn new() -> Self {
		Default::default()
	}

	/// Whether to send queries as AST
	pub fn set_ast_payload(mut self, ast_payload: bool) -> Self {
		self.ast_payload = ast_payload;
		self
	}

	/// Send queries as AST
	pub fn ast_payload(mut self) -> Self {
		self.ast_payload = true;
		self
	}

	/// Set the query timeout of the config
	pub fn query_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
		self.query_timeout = timeout.into();
		self
	}

	/// Set the transaction timeout of the config
	pub fn transaction_timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
		self.transaction_timeout = timeout.into();
		self
	}

	/// Set the default user
	#[allow(clippy::needless_pass_by_value)] // Public SDK builder: ergonomic for callers passing an owned `Root`.
	pub fn user(mut self, user: crate::opt::auth::Root) -> Self {
		self.auth = Level::Root;
		self.username = user.username;
		self.password = user.password;
		self
	}

	/// Use Rustls to configure TLS connections
	///
	/// WARNING: `rustls` is not stable yet. As we may need to upgrade this
	/// dependency from time to time to keep up with its security fixes, this
	/// method is excluded from our stability guarantee.
	#[cfg(feature = "rustls")]
	#[cfg_attr(docsrs, doc(cfg(feature = "rustls")))]
	pub fn rustls(mut self, config: rustls::ClientConfig) -> Self {
		self.tls_config = Some(super::Tls::Rust(config));
		self
	}

	/// Use native TLS to configure TLS connections
	///
	/// WARNING: `native-tls` is not stable yet. As we may need to upgrade this
	/// dependency from time to time to keep up with its security fixes, this
	/// method is excluded from our stability guarantee.
	#[cfg(feature = "native-tls")]
	#[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
	pub fn native_tls(mut self, config: native_tls::TlsConnector) -> Self {
		self.tls_config = Some(super::Tls::Native(config));
		self
	}

	/// Set the capabilities for the database
	pub fn capabilities(mut self, capabilities: Capabilities) -> Self {
		self.capabilities = capabilities.into();
		self
	}

	/// Set the WebSocket config
	pub fn websocket(mut self, websocket: WebsocketConfig) -> crate::Result<Self> {
		if websocket.max_write_buffer_size <= websocket.write_buffer_size {
			return Err(crate::Error::internal("The write buffer size is too small".to_string()));
		}
		self.websocket = websocket;
		Ok(self)
	}

	/// Set the gRPC config
	pub fn grpc(mut self, grpc: GrpcConfig) -> crate::Result<Self> {
		// Refused here rather than at connect: a size this small cannot work
		// against any server, so there is nothing about the connection left to
		// find out.
		if let Some(size) = grpc.max_message_size
			&& size < crate::opt::grpc::MIN_MESSAGE_SIZE
		{
			return Err(crate::Error::configuration(
				format!(
					"The gRPC maximum message size must be at least \
					 {} bytes, and is {size}: a file transfer is framed in fixed {} byte \
					 chunks, which a smaller limit cannot carry in either direction.",
					crate::opt::grpc::MIN_MESSAGE_SIZE,
					crate::opt::grpc::FILE_CHUNK_SIZE
				),
				None,
			));
		}
		// And likewise a size the wire cannot describe: a message carries its
		// length in a four byte prefix, so no connection can transfer one past
		// that however either peer is configured.
		//
		// Asked as "does this fit the prefix" rather than as a comparison
		// against the ceiling, because on a 32-bit target -- wasm among them --
		// the ceiling is `usize::MAX` and no comparison against it can ever be
		// true. The conversion says the same thing at every pointer width.
		if let Some(size) = grpc.max_message_size
			&& u32::try_from(size).is_err()
		{
			return Err(crate::Error::configuration(
				format!(
					"The gRPC maximum message size must be at most {} bytes, and is {size}: a \
					 message carries its length in a four byte prefix, so a longer one cannot be \
					 framed.",
					crate::opt::grpc::MAX_MESSAGE_SIZE
				),
				None,
			));
		}
		self.grpc = grpc;
		Ok(self)
	}

	#[cfg(storage)]
	pub fn temporary_directory(mut self, path: Option<PathBuf>) -> Self {
		self.temporary_directory = path;
		self
	}

	/// Set the interval at which the database should run node maintenance tasks
	pub fn node_membership_refresh_interval(
		mut self,
		interval: impl Into<Option<Duration>>,
	) -> Self {
		self.node_membership_refresh_interval = interval.into().filter(|x| !x.is_zero());
		self
	}

	/// Set the interval at which the database should run node maintenance tasks
	pub fn node_membership_check_interval(mut self, interval: impl Into<Option<Duration>>) -> Self {
		self.node_membership_check_interval = interval.into().filter(|x| !x.is_zero());
		self
	}

	/// Set the interval at which the database should run node maintenance tasks
	pub fn node_membership_cleanup_interval(
		mut self,
		interval: impl Into<Option<Duration>>,
	) -> Self {
		self.node_membership_cleanup_interval = interval.into().filter(|x| !x.is_zero());
		self
	}

	/// Set the interval at which the database should run node maintenance tasks
	pub fn changefeed_gc_interval(mut self, interval: impl Into<Option<Duration>>) -> Self {
		self.changefeed_gc_interval = interval.into().filter(|x| !x.is_zero());
		self
	}
}
