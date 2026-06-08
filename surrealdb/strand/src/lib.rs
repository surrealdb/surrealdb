//! A small-string-optimised immutable string type used throughout the value layer.
//!
//! [`Strand`] backs string-shaped keys and values — `Value::String`, `Object` keys, `TableName`,
//! `RecordIdKey::String` — and trades the mutability of `String` for three complementary storage
//! strategies selected at construction time:
//!
//! * `Inline` — short strings (≤ [`INLINE_CAP`] bytes) stored directly on the stack, with no heap
//!   allocation.
//! * `Static` — a `&'static str` wrapper for compile-time known values. Construction is `const`,
//!   clone is a pointer copy, and drop is a no-op. Ideal for long literals (table names, reserved
//!   keywords, response keys) that would otherwise allocate. Build one with [`Strand::new_static`].
//! * `Boxed` — dynamic long strings held in a `Box<str>`: one allocation per string, no atomic ops
//!   on construction or drop. Clone does `malloc + memcpy`, so reach for [`Strand::new_static`]
//!   whenever the value is known at compile time.
//!
//! ## Layout
//!
//! `Strand` uses a custom 24-byte union layout. The first 16 bytes (on 64-bit) always form a
//! valid `&str` fat pointer for `Static` and `Boxed` strings. For `Inline` strings, the string
//! data is stored inline. The 24th byte (index 23) is used as a tag:
//! * `0..=23`: The string is `Inline`, and the tag is the length.
//! * `254`: The string is `Static`.
//! * `255`: The string is `Boxed`.
//!
//! This layout allows `as_str()` to be completely branchless, significantly improving the
//! performance of equality and ordering comparisons for all variants.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::mem::ManuallyDrop;
use std::ops::Deref;

use revision::{DeserializeRevisioned, Error, Revisioned, SerializeRevisioned};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Maximum byte length of a string that can be stored inline.
pub const INLINE_CAP: usize = 23;

/// Tag for static strings.
const TAG_STATIC: u8 = 254;
/// Tag for boxed strings.
const TAG_BOXED: u8 = 255;

/// Length of the padding bytes in the heap data.
const HEAP_PAD_LEN: usize = 23 - 2 * std::mem::size_of::<usize>();

/// Heap data for boxed strings.
#[derive(Clone, Copy)]
#[repr(C)]
struct HeapData {
	ptr: *const u8,
	len: usize,
	_pad: [u8; HEAP_PAD_LEN],
	tag: u8,
}

/// Union for inline and heap data.
#[repr(C)]
union StrandData {
	inline: [u8; 24],
	heap: ManuallyDrop<HeapData>,
}

/// Immutable string with inline small-string optimisation.
///
/// See the [module docs](self) for the design rationale.
#[repr(transparent)]
pub struct Strand {
	data: StrandData,
}

unsafe impl Send for Strand {}
unsafe impl Sync for Strand {}

impl Strand {
	/// Create a new [`Strand`] from any string-like input.
	#[inline]
	pub fn new(s: impl AsRef<str>) -> Self {
		let s = s.as_ref();
		if s.len() <= INLINE_CAP {
			Self::new_inline(s)
		} else {
			Self::from(Box::from(s))
		}
	}

	#[inline]
	fn new_inline(s: &str) -> Self {
		debug_assert!(s.len() <= INLINE_CAP);
		let mut inline = [0u8; 24];
		// SAFETY: We checked the length above.
		unsafe {
			std::ptr::copy_nonoverlapping(s.as_ptr(), inline.as_mut_ptr(), s.len());
		}
		inline[23] = s.len() as u8;
		Self {
			data: StrandData {
				inline,
			},
		}
	}

	/// Wrap a `&'static str` as a [`Strand`] without allocating.
	///
	/// This never allocates or copies; the returned `Strand` holds
	/// the caller's fat pointer directly, and cloning it is a
	/// bitwise copy. Callable in `const` context, so compile-time
	/// `Strand` constants of arbitrary length are fine:
	///
	/// ```
	/// # use surrealdb_strand::Strand;
	/// const KIND: Strand = Strand::new_static("geometry<multipolygon>");
	/// ```
	#[inline(always)]
	pub const fn new_static(text: &'static str) -> Self {
		Self {
			data: StrandData {
				heap: ManuallyDrop::new(HeapData {
					ptr: text.as_ptr(),
					len: text.len(),
					_pad: [0; HEAP_PAD_LEN],
					tag: TAG_STATIC,
				}),
			},
		}
	}

	/// Format a value directly into an inline `Strand`, bypassing any heap allocation.
	///
	/// If the formatted string exceeds `INLINE_CAP` bytes, this falls back to a heap-allocated
	/// `Boxed` string. This is ideal for constructing short, dynamic strings (like keys or
	/// identifiers) where the length is known or highly likely to be small.
	pub fn from_display(d: impl Display) -> Self {
		use std::fmt::Write;
		// Custom writer for the inline buffer
		struct StrandWriter {
			inline: [u8; 24],
			len: usize,
			overflow: Option<String>,
		}
		// Implement the inline buffer writer
		impl Write for StrandWriter {
			fn write_str(&mut self, s: &str) -> std::fmt::Result {
				// Check if we have overflowed already
				if let Some(overflow) = &mut self.overflow {
					overflow.push_str(s);
					return Ok(());
				}
				// Get the string bytes
				let bytes = s.as_bytes();
				// Calculate the end length
				let end = self.len + bytes.len();
				// Check if it fits in the inline buffer
				if end <= INLINE_CAP {
					// It fits in the inline buffer
					unsafe {
						std::ptr::copy_nonoverlapping(
							bytes.as_ptr(),
							self.inline.as_mut_ptr().add(self.len),
							bytes.len(),
						);
					}
					self.len = end;
				} else {
					// It overflows! Convert what we have so far into a String, then append the new
					// string.
					let valid_utf8 =
						unsafe { std::str::from_utf8_unchecked(&self.inline[..self.len]) };
					let mut overflow = String::with_capacity(end);
					overflow.push_str(valid_utf8);
					overflow.push_str(s);
					self.overflow = Some(overflow);
				}
				Ok(())
			}
		}
		// Create a new writer
		let mut writer = StrandWriter {
			inline: [0u8; 24],
			len: 0,
			overflow: None,
		};
		// Write the displayable value into our custom writer
		write!(&mut writer, "{}", d).expect("writing to StrandWriter should never fail");
		// Check if we have an overflow
		if let Some(overflow) = writer.overflow {
			// It was too long, return a Boxed strand
			Self::from(overflow)
		} else {
			// It fit perfectly! Set the tag/length and return the inline strand
			writer.inline[23] = writer.len as u8;
			Self {
				data: StrandData {
					inline: writer.inline,
				},
			}
		}
	}

	/// Whether this string is stored inline (no heap allocation).
	#[inline]
	pub fn is_inline(&self) -> bool {
		unsafe { self.data.inline[23] <= INLINE_CAP as u8 }
	}

	/// Whether this string wraps a `&'static str` (no allocation).
	#[inline]
	pub fn is_static(&self) -> bool {
		unsafe { self.data.inline[23] == TAG_STATIC }
	}

	/// Whether this string is heap-allocated in a `Box<str>`.
	#[inline]
	pub fn is_boxed(&self) -> bool {
		unsafe { self.data.inline[23] == TAG_BOXED }
	}

	/// Access the underlying string slice.
	#[inline(always)]
	pub fn as_str(&self) -> &str {
		// SAFETY: The tag byte is strictly controlled during construction.
		// It is either the length of an inline string (0..=23), TAG_STATIC (254),
		// or TAG_BOXED (255). This allows for LLVM optimizations.
		unsafe {
			// Get the tag byte.
			let tag = self.data.inline[23];
			// Tell the compiler that tags between 24 and 253 are impossible.
			if tag > INLINE_CAP as u8 && tag != TAG_STATIC && tag != TAG_BOXED {
				std::hint::unreachable_unchecked();
			}
			// Check if the string is inline.
			let is_inline = tag <= INLINE_CAP as u8;
			// Get the length of the string.
			let len = if is_inline {
				tag as usize
			} else {
				self.data.heap.len
			};
			// Get the pointer to the string.
			let ptr = if is_inline {
				self.data.inline.as_ptr()
			} else {
				self.data.heap.ptr
			};
			// Return the string.
			std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len))
		}
	}

	/// Byte length of the string.
	#[inline]
	pub fn len(&self) -> usize {
		self.as_str().len()
	}

	/// Whether the string is empty.
	#[inline]
	pub fn is_empty(&self) -> bool {
		self.as_str().is_empty()
	}

	/// Convert into an owned `String`, copying the bytes.
	#[inline]
	pub fn into_string(self) -> String {
		self.as_str().to_owned()
	}
}

// -----------------------------------------------------------------------
// Drop
// -----------------------------------------------------------------------

impl Drop for Strand {
	#[inline]
	fn drop(&mut self) {
		// SAFETY: We only drop the inner Box<str> if the tag indicates it is Boxed.
		// The pointer and length are guaranteed to be valid because they were created
		// from a valid Box<str> in the `From<Box<str>>` implementation.
		unsafe {
			if self.data.inline[23] == TAG_BOXED {
				let ptr = self.data.heap.ptr as *mut u8;
				let len = self.data.heap.len;
				let slice = std::ptr::slice_from_raw_parts_mut(ptr, len);
				let _ = Box::from_raw(slice as *mut str);
			}
		}
	}
}

// -----------------------------------------------------------------------
// Clone
// -----------------------------------------------------------------------

impl Clone for Strand {
	#[inline]
	fn clone(&self) -> Self {
		// SAFETY: We explicitly check the tag to see if it is Boxed.
		// If it is, we perform a deep copy by allocating a new Box<str>.
		// If it is Inline or Static, we can safely perform a bitwise copy because
		// neither variant owns any heap allocations that need to be duplicated.
		unsafe {
			let tag = self.data.inline[23];
			if tag == TAG_BOXED {
				#[cold]
				#[inline(never)]
				fn cold_clone(s: &str) -> Strand {
					Strand::from(Box::from(s))
				}
				cold_clone(self.as_str())
			} else {
				// For Inline and Static, it's just a bitwise copy
				std::ptr::read(self as *const Strand)
			}
		}
	}
}

// -----------------------------------------------------------------------
// Default / Deref / AsRef / Borrow
// -----------------------------------------------------------------------

impl Default for Strand {
	#[inline]
	fn default() -> Self {
		Self {
			data: StrandData {
				inline: [0u8; 24],
			},
		}
	}
}

impl Deref for Strand {
	type Target = str;
	#[inline]
	fn deref(&self) -> &str {
		self.as_str()
	}
}

impl AsRef<str> for Strand {
	#[inline]
	fn as_ref(&self) -> &str {
		self.as_str()
	}
}

impl Borrow<str> for Strand {
	#[inline]
	fn borrow(&self) -> &str {
		self.as_str()
	}
}

// -----------------------------------------------------------------------
// Construction conversions
// -----------------------------------------------------------------------

impl From<&str> for Strand {
	#[inline]
	fn from(s: &str) -> Self {
		Self::new(s)
	}
}

impl From<String> for Strand {
	#[inline]
	fn from(s: String) -> Self {
		if s.len() <= INLINE_CAP {
			Self::new_inline(&s)
		} else {
			Self::from(s.into_boxed_str())
		}
	}
}

impl From<&String> for Strand {
	#[inline]
	fn from(s: &String) -> Self {
		Self::new(s.as_str())
	}
}

impl From<Box<str>> for Strand {
	#[inline]
	fn from(s: Box<str>) -> Self {
		if s.len() <= INLINE_CAP {
			Self::new_inline(&s)
		} else {
			let ptr = s.as_ptr();
			let len = s.len();
			std::mem::forget(s);
			Self {
				data: StrandData {
					heap: ManuallyDrop::new(HeapData {
						ptr,
						len,
						_pad: [0; HEAP_PAD_LEN],
						tag: TAG_BOXED,
					}),
				},
			}
		}
	}
}

impl From<Strand> for String {
	#[inline]
	fn from(s: Strand) -> String {
		s.as_str().to_owned()
	}
}

impl From<&Strand> for String {
	#[inline]
	fn from(s: &Strand) -> String {
		s.as_str().to_owned()
	}
}

// -----------------------------------------------------------------------
// Equality / ordering / hashing
// -----------------------------------------------------------------------

impl Eq for Strand {}

impl PartialEq for Strand {
	#[inline]
	fn eq(&self, other: &Self) -> bool {
		// SAFETY: We only compare the arrays directly if both tags are <= INLINE_CAP.
		// When an inline string is created in `new_inline`, the entire 24-byte array
		// is zero-initialized before the string data is copied into it.
		// Therefore, any unused padding bytes are guaranteed to be zero, making a
		// direct byte-for-byte comparison of the full 24-byte array safe and correct.
		unsafe {
			// Get the strand tags
			let tag_a = self.data.inline[23];
			let tag_b = other.data.inline[23];
			// Fast path: Both are Inline strings
			if tag_a <= INLINE_CAP as u8 && tag_b <= INLINE_CAP as u8 {
				// We can compare the 24-byte arrays directly
				return self.data.inline == other.data.inline;
			}
		}
		// Slow path: Types are different
		self.as_str() == other.as_str()
	}
}

impl PartialEq<str> for Strand {
	#[inline]
	fn eq(&self, other: &str) -> bool {
		self.as_str() == other
	}
}

impl PartialEq<&str> for Strand {
	#[inline]
	fn eq(&self, other: &&str) -> bool {
		self.as_str() == *other
	}
}

impl PartialEq<String> for Strand {
	#[inline]
	fn eq(&self, other: &String) -> bool {
		self.as_str() == other.as_str()
	}
}

impl Ord for Strand {
	#[inline]
	fn cmp(&self, other: &Self) -> Ordering {
		// SAFETY: We only extract slices from the inline array if both tags are <= INLINE_CAP.
		// We use the tag as the exact length of the valid string data, ensuring we don't
		// compare any unused padding bytes which could interfere with lexicographical ordering.
		unsafe {
			// Get the strand tags
			let tag_a = self.data.inline[23];
			let tag_b = other.data.inline[23];
			// Fast path: Both are Inline strings
			if tag_a <= INLINE_CAP as u8 && tag_b <= INLINE_CAP as u8 {
				// Get the lengths of the strings
				let len_a = tag_a as usize;
				let len_b = tag_b as usize;
				// For ordering, we must compare the valid bytes exactly because
				// the padding bytes might interfere with lexicographical ordering.
				let slice_a = std::slice::from_raw_parts(self.data.inline.as_ptr(), len_a);
				let slice_b = std::slice::from_raw_parts(other.data.inline.as_ptr(), len_b);
				// Compare the strings
				return slice_a.cmp(slice_b);
			}
		}
		// Slow path: Types are different
		self.as_str().cmp(other.as_str())
	}
}

impl PartialOrd for Strand {
	#[inline]
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl PartialOrd<str> for Strand {
	#[inline]
	fn partial_cmp(&self, other: &str) -> Option<Ordering> {
		self.as_str().partial_cmp(other)
	}
}

impl PartialOrd<String> for Strand {
	#[inline]
	fn partial_cmp(&self, other: &String) -> Option<Ordering> {
		self.as_str().partial_cmp(other.as_str())
	}
}

impl Hash for Strand {
	#[inline]
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.as_str().hash(state)
	}
}

impl Debug for Strand {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		Debug::fmt(self.as_str(), f)
	}
}

impl Display for Strand {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		Display::fmt(self.as_str(), f)
	}
}

// -----------------------------------------------------------------------
// serde
// -----------------------------------------------------------------------

impl Serialize for Strand {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		serializer.serialize_str(self.as_str())
	}
}

impl<'de> Deserialize<'de> for Strand {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct StrandVisitor;

		impl<'de> Visitor<'de> for StrandVisitor {
			type Value = Strand;

			fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
				f.write_str("a string")
			}

			fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
				Ok(Strand::from(v))
			}

			fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
				Ok(Strand::from(v))
			}
		}

		deserializer.deserialize_str(StrandVisitor)
	}
}

// -----------------------------------------------------------------------
// revision
// -----------------------------------------------------------------------

impl Revisioned for Strand {
	#[inline]
	fn revision() -> u16 {
		1
	}
}

impl SerializeRevisioned for Strand {
	#[inline]
	fn serialize_revisioned<W: std::io::Write>(&self, writer: &mut W) -> Result<(), Error> {
		self.as_str().serialize_revisioned(writer)
	}
}

impl DeserializeRevisioned for Strand {
	#[inline]
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, Error> {
		let len = usize::deserialize_revisioned(reader)?;
		if len == 0 {
			return Ok(Self::default());
		}
		if len <= INLINE_CAP {
			let mut inline = [0u8; 24];
			reader.read_exact(&mut inline[..len]).map_err(Error::Io)?;
			std::str::from_utf8(&inline[..len]).map_err(Error::Utf8Error)?;
			inline[23] = len as u8;
			return Ok(Strand {
				data: StrandData {
					inline,
				},
			});
		}
		let mut buf = vec![0u8; len];
		reader.read_exact(&mut buf).map_err(Error::Io)?;
		let s = String::from_utf8(buf).map_err(|e| Error::Utf8Error(e.utf8_error()))?;
		Ok(Strand::from(s.into_boxed_str()))
	}
}

impl revision::SkipRevisioned for Strand {
	#[inline]
	fn skip_revisioned<R: std::io::Read>(reader: &mut R) -> Result<(), Error> {
		<String as revision::SkipRevisioned>::skip_revisioned(reader)
	}

	#[inline]
	fn skip_revisioned_slice(reader: &mut revision::SliceReader<'_>) -> Result<(), Error> {
		<String as revision::SkipRevisioned>::skip_revisioned_slice(reader)
	}
}

impl revision::WalkRevisioned for Strand {
	type Walker<'r, R: revision::BorrowedReader + 'r> = revision::LeafWalker<'r, Strand, R>;

	#[inline]
	fn walk_revisioned<'r, R: revision::BorrowedReader>(
		reader: &'r mut R,
	) -> Result<Self::Walker<'r, R>, Error> {
		Ok(revision::LeafWalker::new(reader))
	}
}

impl revision::LengthPrefixedBytes for Strand {}

// -----------------------------------------------------------------------
// storekey
// -----------------------------------------------------------------------

impl<F> storekey::Encode<F> for Strand {
	#[inline]
	fn encode<W: std::io::Write>(
		&self,
		writer: &mut storekey::Writer<W>,
	) -> Result<(), storekey::EncodeError> {
		<str as storekey::Encode<F>>::encode(self.as_str(), writer)
	}
}

impl<'de, F> storekey::BorrowDecode<'de, F> for Strand {
	#[inline]
	fn borrow_decode(
		reader: &mut storekey::BorrowReader<'de>,
	) -> Result<Self, storekey::DecodeError> {
		let cow = reader.read_str_cow()?;
		let s: &str = &cow;
		Ok(if s.len() <= INLINE_CAP {
			Self::new_inline(s)
		} else {
			Self::from(Box::from(s))
		})
	}
}

impl<F> storekey::Decode<F> for Strand {
	#[inline]
	fn decode<R: std::io::BufRead>(
		reader: &mut storekey::Reader<R>,
	) -> Result<Self, storekey::DecodeError> {
		let bytes = reader.read_vec()?;
		let s = std::str::from_utf8(&bytes).map_err(|_| storekey::DecodeError::Utf8)?;
		Ok(if s.len() <= INLINE_CAP {
			Self::new_inline(s)
		} else {
			Self::from(Box::from(s))
		})
	}
}

// -----------------------------------------------------------------------
// arbitrary
// -----------------------------------------------------------------------

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for Strand {
	#[inline]
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		let s = <&str as arbitrary::Arbitrary<'a>>::arbitrary(u)?;
		Ok(Strand::from(s))
	}

	#[inline]
	fn size_hint(depth: usize) -> (usize, Option<usize>) {
		<&str as arbitrary::Arbitrary<'a>>::size_hint(depth)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	const SHORT: &str = "hello";
	/// 23 bytes — the inline boundary, exactly fills the inline buffer.
	const AT_CAP: &str = "abcdefghijklmnopqrstuvw";
	const LONG: &str = "this string is intentionally much longer than twenty three bytes so it must live on the heap";

	// --- layout ---------------------------------------------------------

	#[test]
	fn stack_size_is_24_bytes() {
		use std::mem::size_of;
		// Note: Strand is always 24 bytes.
		// String is 24 bytes on 64-bit
		// String is 12 bytes on 32-bit.
		assert_eq!(size_of::<Strand>(), 24);
	}

	// --- basic construction --------------------------------------------

	#[test]
	fn short_strings_are_inline() {
		let s = Strand::from(SHORT);
		assert!(s.is_inline());
		assert_eq!(s.as_str(), SHORT);
	}

	#[test]
	fn long_strings_are_boxed() {
		let s = Strand::from(LONG);
		assert!(!s.is_inline());
		assert!(!s.is_static());
		assert!(s.is_boxed());
		assert_eq!(s.as_str(), LONG);
	}

	#[test]
	fn inline_boundary() {
		assert_eq!(AT_CAP.len(), INLINE_CAP);
		assert!(Strand::from(AT_CAP).is_inline());

		let over: String = "a".repeat(INLINE_CAP + 1);
		assert!(!Strand::from(over.as_str()).is_inline());
	}

	#[test]
	fn empty_is_inline() {
		let s = Strand::from("");
		assert!(s.is_inline());
		assert!(s.is_empty());
		assert_eq!(s.as_str(), "");
	}

	#[test]
	fn default_is_empty_inline() {
		let s = Strand::default();
		assert!(s.is_inline());
		assert!(s.is_empty());
	}

	// --- clone & drop --------------------------------------------------

	#[test]
	fn inline_clone_is_independent_copy() {
		let s = Strand::from(SHORT);
		let t = s.clone();
		assert_eq!(s.as_str(), t.as_str());
		// Both are inline; drop one and the other must still be valid.
		drop(s);
		assert_eq!(t.as_str(), SHORT);
	}

	#[test]
	fn boxed_clone_is_deep_copy() {
		let s = Strand::from(LONG);
		assert!(s.is_boxed());
		let t = s.clone();
		assert_eq!(s.as_str(), t.as_str());
		// Each `Boxed` clone owns its own allocation; the byte
		// pointers must differ even though the contents are equal.
		assert_ne!(s.as_str().as_ptr(), t.as_str().as_ptr());
		// Drop the original — the clone must still be fully valid.
		drop(s);
		assert_eq!(t.as_str(), LONG);
	}

	// --- static --------------------------------------------------------

	#[test]
	fn new_static_is_static() {
		let s = Strand::new_static("foo");
		assert!(s.is_static());
		assert!(!s.is_inline());
		assert!(!s.is_boxed());
		assert_eq!(s.as_str(), "foo");
	}

	#[test]
	fn new_static_long_is_still_static() {
		// Longer than `INLINE_CAP`: the key benefit of `Static` is
		// that it skips the allocation regardless of length.
		let s = Strand::new_static(LONG);
		assert!(s.len() > INLINE_CAP);
		assert!(s.is_static());
		assert!(!s.is_boxed());
		assert_eq!(s.as_str(), LONG);
	}

	#[test]
	fn new_static_is_const() {
		// The whole point of the variant: `const` construction,
		// including for strings longer than `INLINE_CAP`.
		const SHORT_STATIC: Strand = Strand::new_static("foo");
		const LONG_STATIC: Strand =
			Strand::new_static("this literal is longer than INLINE_CAP but costs nothing");
		assert_eq!(SHORT_STATIC.as_str(), "foo");
		assert!(LONG_STATIC.as_str().len() > INLINE_CAP);
	}

	#[test]
	fn static_clone_is_pointer_copy() {
		let original = "some compile-time string";
		let s = Strand::new_static(original);
		let t = s.clone();
		// Both clones must point at the exact same backing bytes as
		// the original literal — no allocation, no copy.
		assert_eq!(s.as_str().as_ptr(), original.as_ptr());
		assert_eq!(t.as_str().as_ptr(), original.as_ptr());
	}

	// --- semantics -----------------------------------------------------

	#[test]
	fn cross_repr_equality() {
		// All three variants holding the same bytes must compare
		// equal to each other.
		let inline = Strand::from("abc");
		let stat = Strand::new_static("abc");
		let boxed = Strand::from("a".repeat(INLINE_CAP + 1));
		let boxed2 = Strand::from(boxed.as_str());
		assert!(inline.is_inline());
		assert!(stat.is_static());
		assert!(boxed.is_boxed());
		assert_eq!(inline, stat);
		assert_eq!(stat, inline);
		assert_eq!(boxed, boxed2);
	}

	#[test]
	fn ord_is_lexicographic() {
		let a = Strand::from("apple");
		let b = Strand::from("banana");
		assert!(a < b);
	}

	#[test]
	fn hashing_works_as_map_key() {
		use std::collections::HashMap;
		let mut m = HashMap::new();
		m.insert(Strand::from("k"), 1);
		assert_eq!(m.get("k"), Some(&1));
	}

	#[test]
	fn roundtrip_revisioned() {
		let s = Strand::from("round trip");
		let mut bytes = Vec::new();
		s.serialize_revisioned(&mut bytes).unwrap();
		let back = Strand::deserialize_revisioned(&mut bytes.as_slice()).unwrap();
		assert_eq!(s, back);
	}

	#[test]
	fn roundtrip_long_heap() {
		let s = Strand::from(LONG);
		let mut bytes = Vec::new();
		s.serialize_revisioned(&mut bytes).unwrap();
		let back = Strand::deserialize_revisioned(&mut bytes.as_slice()).unwrap();
		assert_eq!(s, back);
		assert!(!back.is_inline());
	}

	// --- revisioned edge cases ----------------------------------------
	//
	// These exercise the in-place `Box<str>` decode path and the
	// inline-buffer decode path, which bypass `String::deserialize_revisioned`
	// entirely and must cover the same cases the generic `String` impl
	// used to.

	fn roundtrip_revisioned_for(s: &str, expect_inline: bool) {
		let strand = Strand::from(s);
		let mut bytes = Vec::new();
		strand.serialize_revisioned(&mut bytes).unwrap();
		let back = Strand::deserialize_revisioned(&mut bytes.as_slice()).unwrap();
		assert_eq!(back.as_str(), s);
		assert_eq!(back.is_inline(), expect_inline);
	}

	#[test]
	fn roundtrip_revisioned_empty() {
		roundtrip_revisioned_for("", true);
	}

	#[test]
	fn roundtrip_revisioned_at_inline_cap() {
		roundtrip_revisioned_for(AT_CAP, true);
		assert_eq!(AT_CAP.len(), INLINE_CAP);
	}

	#[test]
	fn roundtrip_revisioned_one_over_inline_cap() {
		let over: String = "a".repeat(INLINE_CAP + 1);
		roundtrip_revisioned_for(&over, false);
	}

	/// Multi-byte UTF-8 that straddles the inline/heap boundary — the
	/// in-place decode must not be fooled by a codepoint whose UTF-8
	/// length is not 1, and must validate UTF-8 even when the bytes
	/// are written directly into a freshly allocated `Vec<u8>`.
	#[test]
	fn roundtrip_revisioned_utf8_heap() {
		let s = "δοκιμή αξιολόγησης κειμένου με πολυβυτικούς χαρακτήρες";
		assert!(s.len() > INLINE_CAP);
		roundtrip_revisioned_for(s, false);
	}

	#[test]
	fn deserialize_revisioned_rejects_invalid_utf8() {
		// Manually craft a payload that claims 2 bytes of payload but
		// provides invalid UTF-8 (a lone continuation byte).
		let mut bytes = Vec::new();
		2usize.serialize_revisioned(&mut bytes).unwrap();
		bytes.push(0xFF);
		bytes.push(0xFE);
		assert!(Strand::deserialize_revisioned(&mut bytes.as_slice()).is_err());
	}

	// --- storekey round-trips -----------------------------------------

	fn roundtrip_storekey_for(s: &str, expect_inline: bool) {
		use storekey::{BorrowDecode, Decode, Encode};

		let strand = Strand::from(s);
		// Encode via storekey.
		let mut buf = Vec::new();
		let mut w = storekey::Writer::new(&mut buf);
		<Strand as Encode<()>>::encode(&strand, &mut w).unwrap();

		// BorrowDecode path.
		{
			let mut r = storekey::BorrowReader::new(&buf);
			let back = <Strand as BorrowDecode<'_, ()>>::borrow_decode(&mut r).unwrap();
			assert_eq!(back.as_str(), s);
			assert_eq!(back.is_inline(), expect_inline);
		}

		// Streaming Decode path.
		{
			let mut r = storekey::Reader::new(buf.as_slice());
			let back = <Strand as Decode<()>>::decode(&mut r).unwrap();
			assert_eq!(back.as_str(), s);
			assert_eq!(back.is_inline(), expect_inline);
		}
	}

	#[test]
	fn roundtrip_storekey_empty() {
		roundtrip_storekey_for("", true);
	}

	#[test]
	fn roundtrip_storekey_short() {
		roundtrip_storekey_for(SHORT, true);
	}

	#[test]
	fn roundtrip_storekey_at_inline_cap() {
		roundtrip_storekey_for(AT_CAP, true);
	}

	#[test]
	fn roundtrip_storekey_long() {
		roundtrip_storekey_for(LONG, false);
	}

	/// Values that contain `0x00` and `0x01` bytes exercise the
	/// escape-aware decoder branch in `BorrowReader::read_str_cow`
	/// (which returns `Cow::Owned` instead of `Cow::Borrowed`).
	#[test]
	fn roundtrip_storekey_with_escape_bytes() {
		roundtrip_storekey_for("abc\0def\x01ghi", true);
		let long_with_escapes: String = format!("{}\0{}", "x".repeat(30), "y".repeat(30));
		roundtrip_storekey_for(&long_with_escapes, false);
	}

	// --- wire-format compatibility with `String` ----------------------
	//
	// `Strand` is a drop-in replacement for `String` at both the
	// `revision` (on-disk, document/change-feed) and `storekey`
	// (index-key) layers. The entire value of the small-string
	// optimisation hinges on that being invisible from the wire
	// format's perspective — any byte-level divergence between
	// `Strand::serialize` and `String::serialize` for the same input
	// would silently break on-disk data on upgrade. These tests assert
	// both byte-identity and cross-type decode compatibility for
	// every edge case the earlier roundtrip tests touch.

	/// Inputs that exercise every interesting structural case:
	/// - empty (length-prefix only, no payload);
	/// - one byte below, equal to, and one byte above `INLINE_CAP` (the inline/heap boundary only
	///   the `Strand` impl cares about; from the wire's perspective it should be invisible);
	/// - a long ASCII string (typical `LONG` payload);
	/// - multi-byte UTF-8 whose byte length straddles `INLINE_CAP` (guards against off-by-one in
	///   the boundary check or a codepoint-vs-byte confusion);
	/// - strings containing `0x00` and `0x01` bytes, which trigger the escape-aware branch in
	///   `storekey`'s writer/reader.
	fn wire_format_cases() -> Vec<String> {
		let at_cap_minus_one: String = "a".repeat(INLINE_CAP - 1);
		let at_cap: String = "a".repeat(INLINE_CAP);
		let at_cap_plus_one: String = "a".repeat(INLINE_CAP + 1);
		let utf8_heap = "δοκιμή αξιολόγησης κειμένου με πολυβυτικούς χαρακτήρες".to_owned();
		let escape_short = "abc\0def\x01ghi".to_owned();
		let escape_long = format!("{}\0{}", "x".repeat(30), "y".repeat(30));
		vec![
			String::new(),
			SHORT.to_owned(),
			at_cap_minus_one,
			at_cap,
			at_cap_plus_one,
			LONG.to_owned(),
			utf8_heap,
			escape_short,
			escape_long,
		]
	}

	/// For every fixture, assert that `Strand` produces byte-identical
	/// `revisioned` output to `String`, and that both types can decode
	/// each other's bytes back to the original value.
	#[test]
	fn revisioned_wire_matches_string() {
		for input in wire_format_cases() {
			let strand = Strand::from(input.as_str());

			let mut strand_bytes = Vec::new();
			strand.serialize_revisioned(&mut strand_bytes).unwrap();

			let mut string_bytes = Vec::new();
			input.serialize_revisioned(&mut string_bytes).unwrap();

			// (1) Byte-identical output.
			assert_eq!(
				strand_bytes, string_bytes,
				"Strand and String must produce identical revisioned bytes for {:?}",
				input
			);

			// (2) `Strand` can decode bytes produced by `String`.
			let from_string_bytes =
				Strand::deserialize_revisioned(&mut string_bytes.as_slice()).unwrap();
			assert_eq!(from_string_bytes.as_str(), input);

			// (3) `String` can decode bytes produced by `Strand`.
			let from_strand_bytes =
				String::deserialize_revisioned(&mut strand_bytes.as_slice()).unwrap();
			assert_eq!(from_strand_bytes, input);
		}
	}

	/// [`revision::LengthPrefixedBytes`] enables [`revision::LeafWalker::with_bytes`] on
	/// slice-backed readers; payload bytes must match UTF-8 encoding of the strand.
	#[test]
	fn revision_leaf_walker_with_bytes_matches_strand_utf8() {
		use revision::{SerializeRevisioned, WalkRevisioned};
		let s = Strand::from("hello ρ");
		let mut buf = Vec::new();
		s.serialize_revisioned(&mut buf).unwrap();
		let mut r = buf.as_slice();
		let w = Strand::walk_revisioned(&mut r).unwrap();
		w.with_bytes(|bytes| assert_eq!(bytes, s.as_str().as_bytes())).unwrap();
	}

	/// Same assertions for the `storekey` encoding, covering both the
	/// borrowed and streaming decode paths plus the cross-type decode.
	#[test]
	fn storekey_wire_matches_string() {
		use storekey::{BorrowDecode, Decode, Encode};

		for input in wire_format_cases() {
			let strand = Strand::from(input.as_str());

			let mut strand_bytes = Vec::new();
			{
				let mut w = storekey::Writer::new(&mut strand_bytes);
				<Strand as Encode<()>>::encode(&strand, &mut w).unwrap();
			}

			let mut string_bytes = Vec::new();
			{
				let mut w = storekey::Writer::new(&mut string_bytes);
				<String as Encode<()>>::encode(&input, &mut w).unwrap();
			}

			// (1) Byte-identical output — escape-aware encoder must
			// treat `Strand` exactly like `String`, so `0x00`/`0x01`
			// bytes come out escaped the same way.
			assert_eq!(
				strand_bytes, string_bytes,
				"Strand and String must produce identical storekey bytes for {:?}",
				input
			);

			// (2) `Strand` (both BorrowDecode and Decode) can decode
			// bytes produced by `String`.
			{
				let mut r = storekey::BorrowReader::new(&string_bytes);
				let back = <Strand as BorrowDecode<'_, ()>>::borrow_decode(&mut r).unwrap();
				assert_eq!(back.as_str(), input);
			}
			{
				let mut r = storekey::Reader::new(string_bytes.as_slice());
				let back = <Strand as Decode<()>>::decode(&mut r).unwrap();
				assert_eq!(back.as_str(), input);
			}

			// (3) `String` can decode bytes produced by `Strand`.
			{
				let mut r = storekey::BorrowReader::new(&strand_bytes);
				let back = <String as BorrowDecode<'_, ()>>::borrow_decode(&mut r).unwrap();
				assert_eq!(back, input);
			}
			{
				let mut r = storekey::Reader::new(strand_bytes.as_slice());
				let back = <String as Decode<()>>::decode(&mut r).unwrap();
				assert_eq!(back, input);
			}
		}
	}
}
