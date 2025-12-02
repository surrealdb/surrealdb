//! Module implementing spans, types indicating a region of code.

use std::ops::{RangeBounds, RangeInclusive};

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct Span {
	pub start: u32,
	pub end: u32,
}

impl Span {
	#[inline]
	pub const fn new(start: u32, end: u32) -> Self {
		Span {
			start,
			end,
		}
	}

	pub const fn empty() -> Self {
		Span {
			start: 0,
			end: 0,
		}
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
			std::ops::Bound::Included(x) => *x,
			std::ops::Bound::Excluded(x) => (*x).saturating_sub(1),
			std::ops::Bound::Unbounded => u32::MAX,
		};

		Span {
			start,
			end,
		}
	}

	pub fn to_range(&self) -> RangeInclusive<u32> {
		self.start..=self.end
	}

	/// The length of the span.
	pub fn len(&self) -> u32 {
		self.end.saturating_sub(self.start)
	}

	/// Returns if a span is within the region of this span.
	pub fn contains(&self, other: Span) -> bool {
		self.start <= other.start && self.end >= other.end
	}

	/// Returns a span which covers the region of both span, as well as possible uncovered space inbetween.
	pub fn extend(&self, other: Span) -> Self {
		Span {
			start: self.start.min(other.start),
			end: self.end.max(other.end),
		}
	}
}
