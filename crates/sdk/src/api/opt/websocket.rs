#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WebsocketConfig {
	pub read_buffer_size: usize,
	pub write_buffer_size: usize,
	pub max_write_buffer_size: usize,
	pub max_message_size: Option<usize>,
	pub max_frame_size: Option<usize>,
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
