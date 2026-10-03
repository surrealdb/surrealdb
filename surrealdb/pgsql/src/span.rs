//! Source span representation.

use std::fmt;
use std::ops::Range;

/// A byte offset range in source text `[start, end)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Span {
	/// Start byte offset (inclusive).
	pub start: u32,
	/// End byte offset (exclusive).
	pub end: u32,
}

impl Span {
	/// Create a new span from `start` to `end`.
	#[inline]
	pub const fn new(start: u32, end: u32) -> Self {
		Self {
			start,
			end,
		}
	}

	/// Create an empty span at `pos`.
	#[inline]
	pub const fn empty(pos: u32) -> Self {
		Self {
			start: pos,
			end: pos,
		}
	}

	/// Merge two spans to cover the span from the minimum start to the maximum end.
	#[inline]
	pub fn merge(self, other: Self) -> Self {
		Self {
			start: self.start.min(other.start),
			end: self.end.max(other.end),
		}
	}

	/// Return the length of the span in bytes.
	#[inline]
	pub const fn len(&self) -> usize {
		(self.end - self.start) as usize
	}

	/// Return `true` if the span has length zero.
	#[inline]
	pub const fn is_empty(&self) -> bool {
		self.start == self.end
	}

	/// Return this span as a standard byte `Range<usize>`.
	#[inline]
	pub const fn as_range(&self) -> Range<usize> {
		self.start as usize..self.end as usize
	}

	/// Safely slice source text using this span, returning `None` if out of bounds.
	#[inline]
	pub fn slice<'a>(&self, src: &'a str) -> Option<&'a str> {
		src.get(self.as_range())
	}
}

impl fmt::Display for Span {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}..{}", self.start, self.end)
	}
}

impl From<Range<u32>> for Span {
	#[inline]
	fn from(r: Range<u32>) -> Self {
		Self::new(r.start, r.end)
	}
}

impl From<Range<usize>> for Span {
	#[inline]
	fn from(r: Range<usize>) -> Self {
		Self::new(r.start as u32, r.end as u32)
	}
}
