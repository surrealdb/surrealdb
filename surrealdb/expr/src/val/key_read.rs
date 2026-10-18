//! Reading a record id where it lies in a key, building none of it.
//!
//! A record is stored under the storekey encoding of its [`RecordIdKey`](super::RecordIdKey). The
//! types here mirror the storekey decode of [`RecordIdKey`](super::RecordIdKey),
//! [`RecordIdKeyRange`](super::RecordIdKeyRange), [`Value`](super::Value),
//! [`Range`](super::Range) and [`Number`], variant for variant and in their order, so they read
//! exactly the bytes those decodes read and refuse exactly the bytes they refuse, while borrowing
//! strings and numbers from the key instead of building them. They sit beside the types they
//! mirror: a change to the storekey form of one of those types is a change here.
//!
//! [`KeyIdRead`] is the id of a record key read so, and [`KeyText`] a string of a key read so.

use std::ops::Bound;

use storekey::{BorrowDecode, BorrowReader, DecodeError, EscapedSlice};

use super::number::NumberKind;
use super::{
	Bytes, Closure, Datetime, Duration, File, Geometry, Number, RecordId, Regex, Set, TableName,
	Uuid,
};

/// The id of a record key, read as `RecordIdKey`'s decode reads it and built not at all: decoding
/// one validates the id's bytes, and holds nothing of them.
pub struct KeyIdRead;

impl<'de> BorrowDecode<'de, ()> for KeyIdRead {
	fn borrow_decode(reader: &mut BorrowReader<'de>) -> Result<Self, DecodeError> {
		<KeyId<'de> as BorrowDecode<'de, ()>>::borrow_decode(reader)?;
		Ok(Self)
	}
}

/// The id of a record key, read as [`KeyIdRead`] reads it, holding the id where it is a number, a
/// string or a uuid: the number, the string's escaped bytes borrowed from the key, or the uuid. Any
/// other id is read and validated the same way and held as [`KeyIdRef::Other`].
pub enum KeyIdRef<'a> {
	Number(i64),
	String(&'a EscapedSlice),
	Uuid(Uuid),
	Other,
}

impl<'de> BorrowDecode<'de, ()> for KeyIdRef<'de> {
	fn borrow_decode(reader: &mut BorrowReader<'de>) -> Result<Self, DecodeError> {
		Ok(match <KeyId<'de> as BorrowDecode<'de, ()>>::borrow_decode(reader)? {
			KeyId::Number(n) => Self::Number(n),
			KeyId::String(text) => Self::String(text.0),
			KeyId::Uuid(uuid) => Self::Uuid(uuid),
			_ => Self::Other,
		})
	}
}

/// A string of a key, read as the string decode reads it: the escaped bytes of valid UTF-8,
/// borrowed from the key.
pub struct KeyText<'a>(&'a EscapedSlice);

impl<'a> KeyText<'a> {
	/// The string's escaped bytes, as the key holds them.
	pub fn escaped(&self) -> &'a EscapedSlice {
		self.0
	}
}

impl<'de> BorrowDecode<'de, ()> for KeyText<'de> {
	fn borrow_decode(reader: &mut BorrowReader<'de>) -> Result<Self, DecodeError> {
		Text::borrow_decode(reader).map(|text| Self(text.0))
	}
}

/// What `RecordIdKey`'s decode reads of a record id at its place in a key, and builds none of.
/// Its variants are those of `RecordIdKey`, in its order, so it reads the bytes the key decode
/// reads and refuses the bytes it refuses. The elements of an array id, the entries of an object
/// id and the bounds of a range id are read by [`Element`], which mirrors `Value`.
///
/// Reading an id that is a number, a string of any length, a uuid, or an array or object of
/// those allocates nothing. A range id holds one box, and an id that holds a component
/// [`Element`] builds (a set, geometry, bytes, file, table name, regex, closure, range or record
/// id) allocates when it is read.
#[derive(BorrowDecode)]
#[storekey(format = "()")]
#[expect(
	dead_code,
	reason = "an id other than a number or a string is read to be validated and dropped unread"
)]
enum KeyId<'a> {
	Number(i64),
	String(Text<'a>),
	Uuid(Uuid),
	Array(Elements),
	Object(Entries),
	Range(Box<KeyIdRange<'a>>),
}

/// What `RecordIdKeyRange`'s decode reads.
#[derive(BorrowDecode)]
#[storekey(format = "()")]
#[expect(dead_code, reason = "a range is read to be validated and dropped unread")]
struct KeyIdRange<'a> {
	start: Bound<KeyId<'a>>,
	end: Bound<KeyId<'a>>,
}

/// What `Value`'s decode reads of a value inside an id, and builds only where the value's
/// validity cannot be told from its bytes alone or the value is held by no one: its variants are
/// those of `Value`, in its order, so it reads the bytes the value decode reads and refuses the
/// bytes it refuses.
///
/// A string, a number, an array, an object and a range are read in place and build nothing, but
/// for the box of a range. A duration, datetime or uuid is read as its own type, which holds it
/// inline. A set is ordered when it is decoded, so it is read as a [`Set`], and a geometry, bytes,
/// file, record id, table name, regex or closure as its own type, which is built: reading one of
/// those allocates.
#[derive(BorrowDecode)]
#[storekey(format = "()")]
#[expect(dead_code, reason = "a value is read to be validated and dropped unread")]
enum Element<'a> {
	None,
	Null,
	Bool(bool),
	Number(KeyNumber),
	String(Text<'a>),
	Duration(Duration),
	Datetime(Datetime),
	Uuid(Uuid),
	Array(Elements),
	Set(Set),
	Object(Entries),
	Geometry(Geometry),
	Bytes(Bytes),
	Table(TableName),
	RecordId(RecordId),
	File(File),
	Regex(Regex),
	Range(Box<Span<'a>>),
	Closure(Box<Closure>),
}

/// What `Range`'s decode reads.
#[derive(BorrowDecode)]
#[storekey(format = "()")]
#[expect(dead_code, reason = "a range is read to be validated and dropped unread")]
struct Span<'a> {
	start: Bound<Element<'a>>,
	end: Bound<Element<'a>>,
}

/// A string in a key: the escaped bytes of valid UTF-8, borrowed from the key. The string decode
/// builds the string when it holds a byte the key encoding escapes, and so allocates; this reads
/// the same bytes, refuses the bytes it refuses and builds nothing, whatever the string's length.
struct Text<'a>(&'a EscapedSlice);

impl<'de> BorrowDecode<'de, ()> for Text<'de> {
	fn borrow_decode(reader: &mut BorrowReader<'de>) -> Result<Self, DecodeError> {
		let escaped = reader.read_escaped_slice()?;
		if is_utf8(escaped) {
			Ok(Self(escaped))
		} else {
			Err(DecodeError::Utf8)
		}
	}
}

/// Whether the bytes `escaped` holds, unescaped, are valid UTF-8, read through a window on the
/// stack: a sequence that a window ends within is carried into the next.
fn is_utf8(escaped: &EscapedSlice) -> bool {
	let bytes = escaped.as_bytes();
	let raw = bytes.get(..bytes.len().saturating_sub(1)).unwrap_or(&[]);
	if !raw.contains(&1) {
		return std::str::from_utf8(raw).is_ok();
	}
	let mut window = [0u8; 64];
	let mut carried = 0;
	let mut rest = escaped.iter().copied();
	loop {
		let mut filled = carried;
		while let Some(slot) = window.get_mut(filled) {
			let Some(byte) = rest.next() else {
				break;
			};
			*slot = byte;
			filled += 1;
		}
		let last = filled < window.len();
		let Some(held) = window.get(..filled) else {
			return false;
		};
		match std::str::from_utf8(held) {
			Ok(_) => carried = 0,
			// A sequence cut short at the end of the window, which the next bytes complete.
			Err(error) if error.error_len().is_none() && !last => {
				let from = error.valid_up_to();
				window.copy_within(from..filled, 0);
				carried = filled - from;
			}
			Err(_) => return false,
		}
		if last {
			return true;
		}
	}
}

/// The longest decimal buffer of a number that is read without being copied to the heap: the
/// encoding writes a marker, an exponent, a mantissa of at most 39 digits and a terminator.
const DECIMAL_BUFFER: usize = 64;

/// A number in a key, read as `Number`'s decode reads it: its decimal buffer, then its kind, and
/// the conversion of the one to the other, which is `Number::from_decimal_buf_kind` itself. The
/// buffer is unescaped into the stack, so that reading a number allocates nothing.
struct KeyNumber;

impl<'de> BorrowDecode<'de, ()> for KeyNumber {
	fn borrow_decode(reader: &mut BorrowReader<'de>) -> Result<Self, DecodeError> {
		let escaped = reader.read_escaped_slice()?;
		let kind: NumberKind = BorrowDecode::<'de, ()>::borrow_decode(reader)?;
		let mut stack = [0u8; DECIMAL_BUFFER];
		let mut length = 0;
		for &byte in escaped.iter() {
			let Some(slot) = stack.get_mut(length) else {
				// A buffer longer than any number writes is converted from the heap, which
				// refuses it or reads it as the number decode would.
				let heap: Vec<u8> = escaped.iter().copied().collect();
				return Number::from_decimal_buf_kind(&heap, kind)
					.map(|_| Self)
					.map_err(|_| DecodeError::InvalidFormat);
			};
			*slot = byte;
			length += 1;
		}
		Number::from_decimal_buf_kind(&stack[..length], kind)
			.map(|_| Self)
			.map_err(|_| DecodeError::InvalidFormat)
	}
}

/// The elements of an array: values up to a terminator.
struct Elements;

impl<'de> BorrowDecode<'de, ()> for Elements {
	fn borrow_decode(reader: &mut BorrowReader<'de>) -> Result<Self, DecodeError> {
		while !reader.read_terminal()? {
			<Element<'de> as BorrowDecode<'de, ()>>::borrow_decode(reader)?;
		}
		Ok(Self)
	}
}

/// The entries of an object: a string and a value each, up to a terminator, with the strings in
/// strictly ascending order as the object's decode requires.
struct Entries;

impl<'de> BorrowDecode<'de, ()> for Entries {
	fn borrow_decode(reader: &mut BorrowReader<'de>) -> Result<Self, DecodeError> {
		let mut previous: Option<Text<'de>> = None;
		while !reader.read_terminal()? {
			let key = Text::borrow_decode(reader)?;
			if previous.as_ref().is_some_and(|before| key.0.iter().cmp(before.0.iter()).is_le()) {
				return Err(DecodeError::InvalidFormat);
			}
			<Element<'de> as BorrowDecode<'de, ()>>::borrow_decode(reader)?;
			previous = Some(key);
		}
		Ok(Self)
	}
}
