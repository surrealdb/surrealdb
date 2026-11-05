//! Source code spans for error reporting and AST source tracking.

/// A byte offset range `[start, end)` within the source string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Span {
	/// Start byte offset (inclusive).
	pub start: usize,
	/// End byte offset (exclusive).
	pub end: usize,
}

impl Span {
	/// Create a new span from byte offsets.
	#[inline]
	pub const fn new(start: usize, end: usize) -> Self {
		Self {
			start,
			end,
		}
	}

	/// Create an empty span at a single byte offset.
	#[inline]
	pub const fn empty(offset: usize) -> Self {
		Self {
			start: offset,
			end: offset,
		}
	}

	/// Merge two spans into a single span covering both.
	#[inline]
	pub fn merge(self, other: Self) -> Self {
		Self {
			start: self.start.min(other.start),
			end: self.end.max(other.end),
		}
	}

	/// Return the length of the span in bytes.
	#[inline]
	pub const fn len(self) -> usize {
		self.end.saturating_sub(self.start)
	}

	/// Return whether the span is empty (zero length).
	#[inline]
	pub const fn is_empty(self) -> bool {
		self.start >= self.end
	}
}
