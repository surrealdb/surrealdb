/// The size an import is framed in, and an export arrives in.
pub(crate) const FILE_CHUNK_SIZE: usize = 256 << 10;

/// The smallest message size a gRPC connection can work under.
///
/// A file transfer is framed in fixed [`FILE_CHUNK_SIZE`] chunks in both
/// directions, so a client bounded below that cannot import or export at all,
/// however the server is configured. The allowance above the chunk covers the
/// fields wrapping it in its message.
///
/// Spelled out rather than derived, because this module is compiled whether or
/// not the gRPC engine is; the assertion below is what keeps the two figures in
/// step when the protocol crate is present.
pub(crate) const MIN_MESSAGE_SIZE: usize = FILE_CHUNK_SIZE + (4 << 10);

#[cfg(feature = "protocol-grpc")]
const _: () = assert!(FILE_CHUNK_SIZE == surrealdb_protocol::DEFAULT_FILE_CHUNK_SIZE);

/// The largest message size a gRPC connection can work under.
///
/// Every message is length-prefixed with four bytes, so a longer one cannot be
/// framed at all and tonic refuses to emit or accept it whatever this says.
/// Setting a size above it would have this client buffer for, and permit
/// requests up to, a message the transport can never carry.
pub(crate) const MAX_MESSAGE_SIZE: usize = u32::MAX as usize;

/// Configuration options for gRPC connections.
///
/// The one bound a caller usually needs to move is the message size. A gRPC
/// message is not streamed the way a WebSocket message is: a request and each
/// response frame are decoded whole, so both peers refuse anything past their
/// limit rather than reading it in pieces.
///
/// # Examples
///
/// ```rust
/// use surrealdb::opt::GrpcConfig;
///
/// let config = GrpcConfig::new().max_message_size(128 << 20); // 128 MiB
/// ```
#[derive(Debug, Clone, Default)]
pub struct GrpcConfig {
	/// The maximum size of a single gRPC message.
	///
	/// `None` -- the default -- takes the limit from what the server
	/// advertises during the capability handshake, which is the limit the
	/// operator configured.
	pub(crate) max_message_size: Option<usize>,
}

impl GrpcConfig {
	/// Creates a new `GrpcConfig` with default values.
	///
	/// This is equivalent to calling `GrpcConfig::default()`.
	pub fn new() -> Self {
		Default::default()
	}

	/// Sets the maximum size of a single gRPC message.
	///
	/// The two directions reconcile differently against what the server
	/// advertises, because they answer to different things.
	///
	/// A **response** is bounded by this value alone. It is the memory this
	/// client will give one message, and a server can emit more than it
	/// advertises, so raising it above the advertised figure is asking for
	/// something reachable.
	///
	/// It cannot be set below what a file transfer needs, since an import is
	/// sent and an export received in fixed chunks that a smaller limit could
	/// not carry in either direction.
	///
	/// A **request** is bounded by the smaller of this and what the server
	/// said it accepts. Nothing set here persuades a server to take more than
	/// it allows -- only raising its own `SURREAL_GRPC_MAX_MESSAGE_SIZE` does
	/// that -- so a client that raises this alone is told about the mismatch
	/// and has its oversized requests refused before they are sent, rather
	/// than part way through a body. Lowering it below the server's applies in
	/// both directions.
	///
	/// # Examples
	///
	/// ```rust
	/// use surrealdb::opt::GrpcConfig;
	///
	/// let config = GrpcConfig::new().max_message_size(128 << 20); // 128 MiB
	/// ```
	pub fn max_message_size(mut self, max_message_size: usize) -> Self {
		self.max_message_size = Some(max_message_size);
		self
	}
}

#[cfg(test)]
mod tests {
	use super::{FILE_CHUNK_SIZE, GrpcConfig, MAX_MESSAGE_SIZE, MIN_MESSAGE_SIZE};
	use crate::opt::Config;

	/// A size below what a file transfer needs cannot work against any server,
	/// so it is refused where it is set rather than at connect, or on the first
	/// export.
	#[test]
	fn a_message_size_too_small_to_transfer_a_file_is_refused() {
		for size in [1, 64 << 10, FILE_CHUNK_SIZE, MIN_MESSAGE_SIZE - 1] {
			let refused = Config::new()
				.grpc(GrpcConfig::new().max_message_size(size))
				.expect_err("below the floor");
			assert!(
				refused.is_configuration(),
				"expected a configuration error for {size}, got {refused:?}"
			);
		}
	}

	/// A message longer than four gibibytes cannot be length-prefixed, so no
	/// connection can carry one however this is set.
	///
	/// Only meaningful where a `usize` can hold such a size. On a 32-bit target
	/// the ceiling is `usize::MAX`, so there is no value to refuse and nothing
	/// for this to assert.
	#[cfg(target_pointer_width = "64")]
	#[test]
	fn a_message_size_past_the_frame_width_is_refused() {
		for size in [MAX_MESSAGE_SIZE + 1, 8 << 30] {
			let refused = Config::new()
				.grpc(GrpcConfig::new().max_message_size(size))
				.expect_err("above the frame width");
			assert!(
				refused.is_configuration(),
				"expected a configuration error for {size}, got {refused:?}"
			);
		}
	}

	/// The floor itself, the ceiling itself, and anything between, is a size a
	/// caller can hold.
	#[test]
	fn a_workable_message_size_is_accepted() {
		for size in [MIN_MESSAGE_SIZE, 128 << 20, MAX_MESSAGE_SIZE] {
			Config::new()
				.grpc(GrpcConfig::new().max_message_size(size))
				.unwrap_or_else(|error| panic!("{size} should be accepted: {error:?}"));
		}
		// Saying nothing leaves the server's advertised figure to govern.
		Config::new().grpc(GrpcConfig::new()).expect("an unset size is the default");
	}
}
