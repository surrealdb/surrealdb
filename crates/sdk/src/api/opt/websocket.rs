/// Configuration options for WebSocket connections.
///
/// This struct provides fine-grained control over WebSocket buffer sizes and message limits,
/// allowing applications to optimize WebSocket performance based on their specific requirements.
///
/// # Examples
///
/// ```rust
/// use surrealdb::opt::WebsocketConfig;
///
/// // Create a configuration with custom buffer sizes
/// let config = WebsocketConfig::new()
///     .read_buffer_size(256 * 1024)  // 256 KiB read buffer
///     .write_buffer_size(256 * 1024) // 256 KiB write buffer
///     .max_message_size(16 << 20);    // 16 MiB max message size
/// ```
///
/// # Performance Considerations
///
/// - **Read/Write Buffer Sizes**: Larger buffers can improve throughput for high-bandwidth
///   connections but consume more memory. Smaller buffers reduce memory usage but may limit
///   performance for large data transfers.
///
/// - **Message Size Limits**: Setting appropriate limits helps prevent memory exhaustion from
///   malicious or malformed clients while allowing legitimate large messages.
#[derive(Debug, Clone)]
pub struct WebsocketConfig {
	/// The size of the read buffer for incoming WebSocket data (default: 128 KiB)
	pub(crate) read_buffer_size: usize,
	/// The size of the write buffer for outgoing WebSocket data (default: 128 KiB)
	pub(crate) write_buffer_size: usize,
	/// The maximum size of the write buffer before backpressure is applied (default: unlimited)
	pub(crate) max_write_buffer_size: usize,
	/// The maximum size of a complete WebSocket message (default: 64 MiB)
	pub(crate) max_message_size: Option<usize>,
}

impl Default for WebsocketConfig {
	/// Creates a new `WebsocketConfig` with sensible defaults for most applications.
	///
	/// The default configuration provides:
	/// - 128 KiB read and write buffers
	/// - Unlimited write buffer size (no backpressure)
	/// - 64 MiB maximum message size
	///
	/// These defaults are suitable for most applications but can be customized
	/// based on specific performance and memory requirements.
	fn default() -> Self {
		Self {
			read_buffer_size: 128 * 1024,
			write_buffer_size: 128 * 1024,
			max_write_buffer_size: usize::MAX,
			max_message_size: Some(64 << 20),
		}
	}
}

impl WebsocketConfig {
	/// Creates a new `WebsocketConfig` with default values.
	///
	/// This is equivalent to calling `WebsocketConfig::default()`.
	///
	/// # Examples
	///
	/// ```rust
	/// use surrealdb::opt::WebsocketConfig;
	///
	/// let config = WebsocketConfig::new();
	/// ```
	pub fn new() -> Self {
		Default::default()
	}

	/// Sets the read buffer size for incoming WebSocket data.
	///
	/// The read buffer is used to buffer incoming data from the WebSocket connection.
	/// Larger buffers can improve performance for high-throughput connections but
	/// consume more memory per connection.
	///
	/// # Arguments
	///
	/// * `read_buffer_size` - The size of the read buffer in bytes
	///
	/// # Examples
	///
	/// ```rust
	/// use surrealdb::opt::WebsocketConfig;
	///
	/// let config = WebsocketConfig::new()
	///     .read_buffer_size(256 * 1024); // 256 KiB
	/// ```
	pub fn read_buffer_size(mut self, read_buffer_size: usize) -> Self {
		self.read_buffer_size = read_buffer_size;
		self
	}

	/// Sets the write buffer size for outgoing WebSocket data.
	///
	/// The write buffer is used to buffer outgoing data before it's sent over the
	/// WebSocket connection. Larger buffers can improve performance for high-throughput
	/// connections but consume more memory per connection.
	///
	/// # Arguments
	///
	/// * `write_buffer_size` - The size of the write buffer in bytes
	///
	/// # Examples
	///
	/// ```rust
	/// use surrealdb::opt::WebsocketConfig;
	///
	/// let config = WebsocketConfig::new()
	///     .write_buffer_size(256 * 1024); // 256 KiB
	/// ```
	pub fn write_buffer_size(mut self, write_buffer_size: usize) -> Self {
		self.write_buffer_size = write_buffer_size;
		self
	}

	/// Sets the maximum write buffer size before backpressure is applied.
	///
	/// When the write buffer reaches this size, the WebSocket connection will apply
	/// backpressure to prevent memory exhaustion. Setting this to `usize::MAX` (the default)
	/// effectively disables write buffer limits.
	///
	/// # Arguments
	///
	/// * `max_write_buffer_size` - The maximum write buffer size in bytes
	///
	/// # Examples
	///
	/// ```rust
	/// use surrealdb::opt::WebsocketConfig;
	///
	/// let config = WebsocketConfig::new()
	///     .max_write_buffer_size(1024 * 1024); // 1 MiB limit
	/// ```
	pub fn max_write_buffer_size(mut self, max_write_buffer_size: usize) -> Self {
		self.max_write_buffer_size = max_write_buffer_size;
		self
	}

	/// Sets the maximum size of a complete WebSocket message.
	///
	/// This limit applies to the total size of a WebSocket message, which may consist
	/// of multiple frames. Messages exceeding this limit will be rejected with an error.
	///
	/// # Arguments
	///
	/// * `max_message_size` - The maximum message size in bytes, or `None` to disable the limit
	///
	/// # Examples
	///
	/// ```rust
	/// use surrealdb::opt::WebsocketConfig;
	///
	/// let config = WebsocketConfig::new()
	///     .max_message_size(128 << 20); // 128 MiB
	///
	/// // Or disable the limit entirely
	/// let config = WebsocketConfig::new()
	///     .max_message_size(None);
	/// ```
	pub fn max_message_size(mut self, max_message_size: impl Into<Option<usize>>) -> Self {
		self.max_message_size = max_message_size.into();
		self
	}
}
