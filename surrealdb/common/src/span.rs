//! Module implementing spans, types indicating a region of code.

use std::ops::{Range, RangeBounds};

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct Span {
	pub start: u32,
	pub end: u32,
}

impl Span {
	pub const MAX_LENGHT: u32 = u32::MAX;

	#[inline]
	pub const fn new(start: u32, end: u32) -> Self {
		Span {
			start,
			end,
		}
	}

	#[inline]
	pub const fn empty() -> Self {
		Span {
			start: 0,
			end: 0,
		}
	}

	#[inline]
	pub const fn is_empty(&self) -> bool {
		self.start > self.end
	}

	#[inline]
	pub fn from_range<R>(r: R) -> Self
	where
		R: RangeBounds<u32>,
	{
		let start = match r.start_bound() {
			std::ops::Bound::Included(x) => *x,
			std::ops::Bound::Excluded(x) => (*x).saturating_add(1),
			std::ops::Bound::Unbounded => u32::MAX,
		};

		let end = match r.end_bound() {
			std::ops::Bound::Excluded(x) => *x,
			std::ops::Bound::Included(x) => (*x).saturating_add(1),
			std::ops::Bound::Unbounded => u32::MAX,
		};

		Span {
			start,
			end,
		}
	}

	#[inline]
	pub fn from_usize_range<R>(r: R) -> Option<Self>
	where
		R: RangeBounds<usize>,
	{
		let start = match r.start_bound() {
			std::ops::Bound::Included(x) => u32::try_from(*x).ok()?,
			std::ops::Bound::Excluded(x) => u32::try_from(*x).ok()?.saturating_add(1),
			std::ops::Bound::Unbounded => u32::MAX,
		};

		let end = match r.end_bound() {
			std::ops::Bound::Excluded(x) => u32::try_from(*x).ok()?,
			std::ops::Bound::Included(x) => u32::try_from(*x).ok()?.saturating_add(1),
			std::ops::Bound::Unbounded => u32::MAX,
		};

		Some(Span {
			start,
			end,
		})
	}

	#[inline]
	pub fn to_range(&self) -> Range<u32> {
		self.start..self.end
	}

	/// The length of the span.
	#[inline]
	pub fn len(&self) -> u32 {
		self.end.saturating_sub(self.start)
	}

	/// Returns if a span is within the region of this span.
	#[inline]
	pub fn contains(&self, other: Span) -> bool {
		self.start <= other.start && self.end >= other.end
	}

	/// Returns a span which covers the region of both span, as well as possible uncovered space
	/// inbetween.
	#[inline]
	pub fn extend(&self, other: Span) -> Self {
		Span {
			start: self.start.min(other.start),
			end: self.end.max(other.end),
		}
	}

	/// Returns a span for the range of bytes specified by the given span as sub span with in the
	/// current span.
	///
	/// # Panic
	/// Will panic if the sub span is not fully within the current span.
	pub fn sub_span(&self, other: Span) -> Self {
		assert!(self.start + other.end <= self.end);
		Span {
			start: self.start + other.start,
			end: self.start + other.end,
		}
	}
}
