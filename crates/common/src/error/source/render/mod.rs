use super::Diagnostic;

mod char_buffer;
mod format;

pub use char_buffer::CharBuffer;

impl Diagnostic<'_> {
	pub fn render_string(&self) -> String {
		format::render_string(self)
	}

	pub fn render_char_buffer(&self) -> CharBuffer {
		format::render_char_buffer(self)
	}
}
