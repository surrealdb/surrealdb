use core::range::RangeInclusive;
use std::borrow::{Borrow, Cow};
use std::fmt::{self, Write as _};
use std::ops::{self, Bound, Deref};

/// A key type for indexing into a key value store.
///
/// This type and it's methods, along with the [`KeyRange`], should be sufficient to create an kind
/// of key or range of keys you want to create, there should be no need to manually manipulate the
/// bytes of a key.
#[derive(Debug, Default, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct Key<'a>(Cow<'a, [u8]>);

impl fmt::Display for Key<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		for c in
			self.as_slice().iter().copied().flat_map(std::ascii::escape_default).map(|b| b as char)
		{
			f.write_char(c)?;
		}
		Ok(())
	}
}

impl<'a> Key<'a> {
	/// Returns the empty key.
	#[inline]
	pub fn empty() -> Key<'static> {
		Key((&[]).into())
	}

	/// Removes the lifetime from the key, making it static by conferting the internal buffer into
	/// an owned one.
	#[inline]
	pub fn into_static(self) -> Key<'static> {
		Key(self.0.into_owned().into())
	}

	/// Returns a key which borrows the bytes of the current key.
	#[inline]
	pub fn as_borrowed(&self) -> Key<'_> {
		Key::from(&(*self.0))
	}

	/// Returns the bytes of the current key as a slice.
	#[inline]
	pub fn as_slice(&self) -> &[u8] {
		self.0.as_ref()
	}

	/// Constructs a vector of bytes from the current key
	#[inline]
	pub fn into_vec(self) -> Vec<u8> {
		self.0.into_owned()
	}

	/// Returns the internal Cow.
	#[inline]
	pub fn into_inner(self) -> Cow<'a, [u8]> {
		self.0
	}

	/// Returns the next key immediately after the current one.
	///
	/// The immediate next key is the key which must follow immediately after the current key and
	/// there can be no key which would order between the current and the next.
	///
	/// The next key is therefore always the current key + a new zero byte appended at the end.
	#[inline]
	pub fn next(self) -> Key<'static> {
		let mut res = self.0.into_owned();
		res.push(0);
		Key::from(res)
	}

	/// Advances the key to the next key immediately after the current one.
	///
	/// The immediate next key is the key which must follow immediately after the current key and
	/// there can be no key which would order between the current and the next.
	///
	/// The next key is therefore always the current key + a new zero byte appended at the end.
	///
	/// This function is the same as [`next`] except the change is made inplace.
	#[inline]
	pub fn advance(&mut self) {
		self.0.to_mut().push(0);
	}

	/// Returns the key immediately before the current one.
	///
	/// If the key is empty there is not key before the current one and this function returns None
	///
	/// The immediate previous  key is the key which must follow immediately before the current key
	/// and there can be no key which would order between previous and the current key.
	///
	/// The previous key is therefore the current key with 1 subtracted from the last byte, or it
	/// removed if the last byte is currently 0
	#[inline]
	pub fn prev(self) -> Option<Self> {
		let mut res = self.0.into_owned();

		let last = res.last_mut()?;

		if let Some(x) = last.checked_sub(1) {
			*last = x;
		} else {
			res.pop();
		}

		Some(res.into())
	}

	/// Returns the next key that has the same length or less as the current key, or it returns that
	/// key that is greater then all keys `n, n ++ 0x0, n ++ 0x1, .., n ++ 0x1, 0x00, ..` are less
	/// then the new key.
	///
	/// This function returns None if there is no next neighbour key which is the case if the key
	/// consist of all 1 bits or is empty.
	///
	/// This differs from [`next`] in that it is possible to have keys that order in between the
	/// current and it's neighbour, for example the the neighbour of `b'a'` is `b'b'` but
	/// `b'a' < b'aa' < b'b'`
	///
	/// # Example
	/// ```
	/// use surrealdb_kvs::{Key, KeyRange};
	///
	/// // Construct a key range from a prefix.
	/// let prefix = Key::from(&[0xA, 0xB][..]); // Some key which is the prefix of the range we want.
	///
	/// // This code is equivalent to `prefix.prefix_expect()`
	/// let start = prefix.as_borrowed().next();
	/// // The key is not all 0xFF so it has a neighbour
	/// let end = prefix.next_neighbour().unwrap();
	///
	/// let range = KeyRange {
	///     start,
	///     end,
	/// };
	/// ```
	#[inline]
	pub fn next_neighbour(self) -> Option<Key<'static>> {
		let mut this = self.0.into_owned();
		while let Some(x) = this.last_mut() {
			if let Some(new) = x.checked_add(1) {
				*x = new;
				return Some(Key(this.into()));
			} else {
				this.pop();
			}
		}
		None
	}

	/// Returns the next key that has the same length or less as the current key, or it returns that
	/// key that is greater then all keys `n, n ++ 0x0, n ++ 0x1, .., n ++ 0x1, 0x00, ..` are less
	/// then the new key.
	///
	/// This differs from [`next`] in that it is possible to have keys that order in between the
	/// current and it's neighbour, for example the the neighbour of `b'a'` is `b'b'` but
	/// `b'a' < b'aa' < b'b'`
	///
	/// # Panic
	///
	/// This function panics if there is no next neighbour key which is the case if the key
	/// consist of all 1 bits or is empty.
	///
	/// # Example
	/// ```
	/// use surrealdb_kvs::{Key, KeyRange};
	///
	/// // Construct a key range from a prefix.
	/// let prefix = Key::from(&[0xA, 0xB][..]); // Some key which is the prefix of the range we want.
	///
	/// // This code is equivalent to `prefix.prefix_expect()`
	/// let start = prefix.as_borrowed().next();
	/// // The key is not all 0xFF so it has a neighbour
	/// let end = prefix.next_neighbour_expect();
	///
	/// let range = KeyRange {
	///     start,
	///     end,
	/// };
	/// ```
	#[inline]
	#[track_caller]
	pub fn next_neighbour_expect(self) -> Key<'static> {
		let Some(n) = self.next_neighbour() else {
			panic!("Key does not have a next neighbour");
		};
		n
	}

	/// Returns the range of keys, of which the current one would be the prefix
	///
	/// Returns none, if a valid range which would have the current key as it prefix cannot be
	/// constructed.
	#[inline]
	pub fn prefix(self) -> Option<KeyRange<'static>> {
		let end = self.as_borrowed().next_neighbour()?;
		let start = self.next();
		Some(KeyRange {
			start,
			end,
		})
	}

	/// Returns the range of keys, of which the current one would be the prefix
	///
	/// # Panic
	/// Panics if a valid range which would have the current key as it prefix cannot be constructed.
	/// This can be the case if the key is all `0xFF` bytes, in which case we cannot construct a key
	/// which is the exclusive end bound of the range of all keys with this key as its prefix.
	#[inline]
	#[track_caller]
	pub fn prefix_expect(self) -> KeyRange<'static> {
		let Some(range) = self.prefix() else {
			panic!("Key cannot be used as a prefix range")
		};
		range
	}

	/// Overwrites the key from slice, reusing buffers if they exist.
	#[inline]
	pub fn clone_from_slice(&mut self, slice: &[u8]) {
		if let Cow::Owned(ref mut v) = self.0 {
			v.clear();
			v.extend_from_slice(slice);
		} else {
			self.0 = Cow::Owned(slice.to_vec());
		}
	}
}

impl AsRef<[u8]> for Key<'_> {
	#[inline]
	fn as_ref(&self) -> &[u8] {
		self.0.as_ref()
	}
}

impl Borrow<[u8]> for Key<'_> {
	#[inline]
	fn borrow(&self) -> &[u8] {
		self.0.as_ref()
	}
}

impl Deref for Key<'_> {
	type Target = [u8];

	#[inline]
	fn deref(&self) -> &Self::Target {
		self.0.as_ref()
	}
}

impl<'a, T> From<T> for Key<'a>
where
	Cow<'a, [u8]>: From<T>,
{
	#[inline]
	fn from(value: T) -> Self {
		Key(value.into())
	}
}

/// A range of KVStore keys.
///
/// Inclusive over the start and exclusive over the end of the key.
#[derive(Clone, Eq, PartialEq, Debug, Default)]
pub struct KeyRange<'a> {
	pub start: Key<'a>,
	pub end: Key<'a>,
}

impl<'a> KeyRange<'a> {
	/// Create a key from specific bounds on a key.
	///
	/// This key generally returns a valid range with the exception of an unbounded end range.
	/// Such a range cannot be constructed as the last key would be the key after the key with
	/// all bytes 0xff and infinite length.
	pub fn from_bounds<T>(start: Bound<T>, end: Bound<T>) -> Option<Self>
	where
		Key<'a>: From<T>,
	{
		let end = match end {
			Bound::Included(x) => Key::from(x).next(),
			Bound::Excluded(x) => Key::from(x),
			Bound::Unbounded => return None,
		};
		let start = match start {
			Bound::Included(x) => Key::from(x),
			Bound::Excluded(x) => Key::from(x).next(),
			Bound::Unbounded => Key::empty(),
		};
		Some(KeyRange {
			start,
			end,
		})
	}

	pub fn into_static(self) -> KeyRange<'static> {
		KeyRange {
			start: self.start.into_static(),
			end: self.end.into_static(),
		}
	}

	pub fn as_borrowed<'b>(&'b self) -> KeyRange<'b> {
		KeyRange {
			start: self.start.as_borrowed(),
			end: self.end.as_borrowed(),
		}
	}

	pub fn contains(&self, key: &Key<'_>) -> bool {
		self.start <= *key && self.end > *key
	}

	/// Returns true if the range is empty, i.e. cannot contain any key.
	/// Which can be the case when the start key is larger or equal to the end key.
	pub fn is_empty(&self) -> bool {
		self.start >= self.end
	}
}

impl fmt::Display for KeyRange<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}..{}", self.start, self.end)
	}
}

impl<'a, T> From<ops::RangeInclusive<T>> for KeyRange<'a>
where
	Key<'a>: From<T>,
{
	fn from(v: ops::RangeInclusive<T>) -> Self {
		let (start, end) = v.into_inner();
		KeyRange {
			start: start.into(),
			end: Key::from(end).next(),
		}
	}
}

impl<'a, T> From<RangeInclusive<T>> for KeyRange<'a>
where
	Key<'a>: From<T>,
{
	fn from(v: RangeInclusive<T>) -> Self {
		KeyRange {
			start: v.start.into(),
			end: Key::from(v.last).next(),
		}
	}
}

impl<'a, T> From<ops::Range<T>> for KeyRange<'a>
where
	Key<'a>: From<T>,
{
	fn from(v: ops::Range<T>) -> Self {
		KeyRange {
			start: Key::from(v.start),
			end: Key::from(v.end),
		}
	}
}
