//! A location in source text.

use std::ops::Range;

/// A location in the source passed to the lexer.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub struct Span {
	/// Offset in bytes.
	pub offset: u32,
	/// The amount of bytes this location encompasses.
	pub len: u32,
}

impl Span {
	/// Create a new empty span.
	pub const fn empty() -> Self {
		Span {
			offset: 0,
			len: 0,
		}
	}

	pub fn is_empty(&self) -> bool {
		self.len == 0
	}

	pub fn from_range(r: Range<u32>) -> Self {
		let len = r.end - r.start;
		Span {
			offset: r.start,
			len,
		}
	}

	pub fn to_range(&self) -> Range<u32> {
		let start = self.offset;
		let end = start + self.len;
		start..end
	}

	/// Create a span that covers the range of both spans as well as possible
	/// space inbetween.
	pub fn covers(self, other: Span) -> Span {
		let start = self.offset.min(other.offset);
		let end = (self.offset + self.len).max(other.offset + other.len);
		let len = end - start;
		Span {
			offset: start,
			len,
		}
	}

	pub fn as_within(mut self, other: Span) -> Span {
		self.offset += other.offset;
		self
	}

	// returns a zero-length span that starts after the current span.
	pub fn after(self) -> Span {
		Span {
			offset: self.offset + self.len,
			len: 0,
		}
	}
	// returns a zero-length span that starts after the current span.
	pub fn after_offset(self) -> u32 {
		self.offset + self.len
	}

	/// Returns if the given span is the next span after this one.
	pub fn is_followed_by(&self, other: &Self) -> bool {
		let end = self.offset as usize + self.len as usize;
		other.offset as usize == end
	}

	/// Returns if this span immediately follows the given.
	pub fn follows_from(&self, other: &Self) -> bool {
		let end = other.offset as usize + other.len as usize;
		self.offset as usize == end
	}
}
