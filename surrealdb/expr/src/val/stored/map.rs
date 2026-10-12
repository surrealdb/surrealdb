//! The schema containers of the stored-value grammar: maps from field number to item, and lists
//! of items.
//!
//! A schema reads these where the generic [`ValueRef`] cannot: [`tag::MAP`] is schema-only, so the
//! generic decoder refuses it and a generic value never holds one. An [`Item`] is any one of a
//! generic value, a MAP, or a list (an ARRAY container whose elements are items).
//!
//! Every read is checked against the bytes in hand: an entry outside its container, a number that
//! is not a canonical varint in `1..=u32::MAX`, and an entry with no item are typed errors.

use super::read::{Container, ValueRef};
use super::tag::{ARRAY, MAP, width};
use super::{Error, varint};

/// One item of a schema map or list: a generic value, a MAP or a list, as its bytes.
///
/// An item is the tag byte and everything after it, up to the end of its extent.
#[derive(Clone, Copy, Debug)]
pub struct Item<'a> {
	bytes: &'a [u8],
}

impl<'a> Item<'a> {
	/// The item whose encoding is exactly `bytes`. Empty `bytes` give an item that every accessor
	/// refuses as truncated.
	#[inline(always)]
	pub const fn new(bytes: &'a [u8]) -> Self {
		Self {
			bytes,
		}
	}

	/// The item's bytes: its tag and payload.
	#[inline(always)]
	pub const fn bytes(&self) -> &'a [u8] {
		self.bytes
	}

	/// The tag byte.
	#[inline(always)]
	pub fn tag(&self) -> Result<u8, Error> {
		self.bytes.first().copied().ok_or(Error::Truncated)
	}

	/// The item as a generic value, checked as [`ValueRef::new`] checks it. Scalar reads in a
	/// schema use the strict accessors instead, which check the payload of every scalar kind.
	#[inline(always)]
	pub fn value(&self) -> Result<ValueRef<'a>, Error> {
		ValueRef::new(self.bytes)
	}

	/// The item as a map, or `None` when its tag is not a MAP tag.
	#[inline]
	pub fn map(&self) -> Result<Option<MapRef<'a>>, Error> {
		let (&t, p) = self.bytes.split_first().ok_or(Error::Truncated)?;
		match t {
			MAP..=MAP_MAX => Ok(Some(MapRef {
				c: Container::split(p, width(t, MAP))?,
			})),
			_ => Ok(None),
		}
	}

	/// The item as a list, or `None` when its tag is not an ARRAY container tag.
	#[inline]
	pub fn list(&self) -> Result<Option<ListRef<'a>>, Error> {
		let (&t, p) = self.bytes.split_first().ok_or(Error::Truncated)?;
		match t {
			ARRAY..=ARRAY_MAX => Ok(Some(ListRef {
				c: Container::split(p, width(t, ARRAY))?,
			})),
			_ => Ok(None),
		}
	}
}

/// The last MAP tag: offset width 4.
const MAP_MAX: u8 = MAP + 2;
/// The last ARRAY container tag: offset width 4.
const ARRAY_MAX: u8 = ARRAY + 2;

/// A map read in place: entries `number, item` in strictly ascending number order.
#[derive(Clone, Copy, Debug)]
pub struct MapRef<'a> {
	c: Container<'a>,
}

impl<'a> MapRef<'a> {
	/// The number of entries.
	#[inline(always)]
	pub fn len(&self) -> usize {
		self.c.n
	}

	/// Whether the map has no entries.
	#[inline(always)]
	pub fn is_empty(&self) -> bool {
		self.c.n == 0
	}

	/// Entry `i`: its number, a canonical varint in `1..=u32::MAX`, and its item, which is not
	/// empty. The order of the numbers is not checked here.
	#[inline]
	pub fn entry(&self, i: usize) -> Result<(u32, Item<'a>), Error> {
		let bytes = self.c.entry(i)?;
		let (number, used) = varint::read_u64(bytes)
			.map_err(|_| Error::Malformed("a map entry number is malformed"))?;
		let number = u32::try_from(number)
			.ok()
			.filter(|&n| n != 0)
			.ok_or(Error::Malformed("a map entry number is malformed"))?;
		match bytes.get(used..) {
			Some(item) if !item.is_empty() => Ok((number, Item::new(item))),
			_ => Err(Error::Malformed("a map entry has no item")),
		}
	}

	/// The item under `n`, found by binary search.
	///
	/// Each entry the search reads must lie between the entries already read on either side of it,
	/// so a map whose numbers are out of order where the search passes is refused rather than
	/// answered from. [`MapRef::validate`] checks every entry.
	pub fn get(&self, n: u32) -> Result<Option<Item<'a>>, Error> {
		let (mut lo, mut hi) = (0, self.c.n);
		// The numbers of the entries just below `lo` and at `hi`, once the search has read them.
		let (mut below, mut above) = (None::<u32>, None::<u32>);
		while lo < hi {
			let mid = lo + (hi - lo) / 2;
			let (k, item) = self.entry(mid)?;
			if below.is_some_and(|b| k <= b) || above.is_some_and(|a| k >= a) {
				return Err(Error::Malformed("map entry numbers are not strictly ascending"));
			}
			match k.cmp(&n) {
				std::cmp::Ordering::Equal => return Ok(Some(item)),
				std::cmp::Ordering::Less => {
					lo = mid + 1;
					below = Some(k);
				}
				std::cmp::Ordering::Greater => {
					hi = mid;
					above = Some(k);
				}
			}
		}
		Ok(None)
	}

	/// Checks every entry: a canonical number in `1..=u32::MAX`, strictly ascending, and a
	/// non-empty item. A caller that copies entries out of a map it did not write checks it here
	/// first, so a copy never carries a malformed entry.
	pub fn validate(&self) -> Result<(), Error> {
		let mut prev = None::<u32>;
		for i in 0..self.c.n {
			let (k, _) = self.entry(i)?;
			if prev.is_some_and(|p| k <= p) {
				return Err(Error::Malformed("map entry numbers are not strictly ascending"));
			}
			prev = Some(k);
		}
		Ok(())
	}
}

/// A list read in place: an ARRAY container whose elements are items.
#[derive(Clone, Copy, Debug)]
pub struct ListRef<'a> {
	c: Container<'a>,
}

impl<'a> ListRef<'a> {
	/// The number of elements.
	#[inline(always)]
	pub fn len(&self) -> usize {
		self.c.n
	}

	/// Whether the list has no elements.
	#[inline(always)]
	pub fn is_empty(&self) -> bool {
		self.c.n == 0
	}

	/// Element `i`, which is not empty. An `i` past the end is refused.
	#[inline]
	pub fn get(&self, i: usize) -> Result<Item<'a>, Error> {
		Ok(Item::new(self.c.entry(i)?))
	}
}
