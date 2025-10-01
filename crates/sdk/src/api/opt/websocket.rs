#[derive(Debug, Clone)]
pub struct WebsocketConfig {
	pub(crate) read_buffer_size: usize,
	pub(crate) write_buffer_size: usize,
	pub(crate) max_write_buffer_size: usize,
	pub(crate) max_message_size: Option<usize>,
	pub(crate) max_frame_size: Option<usize>,
}

impl Default for WebsocketConfig {
	fn default() -> Self {
		Self {
			read_buffer_size: 128 * 1024,
			write_buffer_size: 128 * 1024,
			max_write_buffer_size: usize::MAX,
			max_message_size: Some(64 << 20),
			max_frame_size: Some(16 << 20),
		}
	}
}

impl WebsocketConfig {
	/// Create a new WebsocketConfig
	pub fn new() -> Self {
		Default::default()
	}

	/// Set the read buffer size
	pub fn read_buffer_size(mut self, read_buffer_size: usize) -> Self {
		self.read_buffer_size = read_buffer_size;
		self
	}

	/// Set the write buffer size
	pub fn write_buffer_size(mut self, write_buffer_size: usize) -> Self {
		self.write_buffer_size = write_buffer_size;
		self
	}

	/// Set the maximum write buffer size
	pub fn max_write_buffer_size(mut self, max_write_buffer_size: usize) -> Self {
		self.max_write_buffer_size = max_write_buffer_size;
		self
	}

	/// Set the maximum WebSocket message size
	pub fn max_message_size(mut self, max_message_size: impl Into<Option<usize>>) -> Self {
		self.max_message_size = max_message_size.into();
		self
	}

	/// Set the maximum WebSocket frame size
	pub fn max_frame_size(mut self, max_frame_size: impl Into<Option<usize>>) -> Self {
		self.max_frame_size = max_frame_size.into();
		self
	}
}
