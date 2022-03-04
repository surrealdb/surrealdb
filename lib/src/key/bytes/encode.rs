use byteorder::{WriteBytesExt, BE};
use serde::{self, Serialize};
use std::fmt;
use std::io::{self, Write};
use std::{self, i16, i32, i64, i8};
use thiserror::Error;

/// A serializer for a byte format that preserves lexicographic sort order.
///
/// The byte format is designed with a few goals:
///
/// * Order must be preserved
/// * Serialized representations should be as compact as possible
/// * Type information is *not* serialized with values
///
/// #### Supported Data Types
///
/// ##### Unsigned Integers
///
/// `u8`, `u16`, `u32`, and `u64` are serialized into 1, 2, 4, and 8 bytes of output, respectively.
/// Order is preserved by encoding the bytes in big-endian (most-significant bytes first) format.
/// `usize` is always serialized as if it were `u64`.
///
/// The `Serializer` also supports variable-length serialization of unsigned integers via the
/// `serialize_var_u64` method. Smaller magnitude values (closer to 0) will encode into fewer
/// bytes.
///
/// ##### Signed Integers
///
/// `i8`, `i16`, `i32`, and `i64` are encoded into 1, 2, 4, and 8 bytes of output, respectively.
/// Order is preserved by taking the bitwise complement of the value, and encoding the resulting
/// bytes in big-endian format. `isize` is always serialized as if it were `i64`.
///
/// The `Serializer` also supports variable-length serialization of signed integers via the
/// `serialize_var_i64` method. Smaller magnitude values (closer to 0) will encode into fewer
/// bytes.
///
/// ##### Floating Point Numbers
///
/// `f32` and `f64` are serialized into 4 and 8 bytes of output, respectively. Order is preserved
/// by encoding the value, or the bitwise complement of the value if negative, into bytes in
/// big-endian format. `NAN` values will sort after all other values. In general, it is unwise to
/// use IEEE 754 floating point values in keys, because rounding errors are pervasive.  It is
/// typically hard or impossible to use an approximate 'epsilon' approach when using keys for
/// lookup.
///
/// ##### Characters
///
/// Characters are serialized into between 1 and 4 bytes of output. The resulting length is
/// equivalent to the result of `char::len_utf8`.
///
/// ##### Booleans
///
/// Booleans are serialized into a single byte of output. `false` values will sort before `true`
/// values.
///
/// ##### Options
///
/// An optional wrapper type adds a 1 byte overhead to the wrapped data type. `None` values will
/// sort before `Some` values.
///
/// ##### Structs, Tuples and Fixed-Size Arrays
///
/// Structs and tuples are serialized by serializing their consituent fields in order with no
/// prefix, suffix, or padding bytes.
///
/// ##### Enums
///
/// Enums are encoded with a `u32` variant index tag, plus the consituent fields in the case of an
/// enum-struct.
///
/// ##### Sequences, Strings and Maps
///
/// Sequences are ordered from the most significant to the least. Strings are serialized into their
/// natural UTF8 representation.
///
/// The ordering of sequential elements follows the `Ord` implementation of `slice`, that is, from
/// left to write when viewing a `Vec` printed via the `{:?}` formatter.
///
/// The caveat with these types is that their length must be known before deserialization. This is
/// because the length is *not* serialized prior to the elements in order to preserve ordering and
/// there is no trivial way to tokenise between sequential elements that 1. does not corrupt
/// ordering and 2. may not confuse tokenisation with following elements of a different type during
/// tuple or struct deserialization. Thus, when deserializing sequences, strings and maps, the
/// process will only be considered complete once the inner `reader` produces an EOF character.
#[derive(Debug)]
pub struct Serializer<W>
where
	W: Write,
{
	writer: W,
}

/// Errors that might occur while serializing.
#[derive(Error, Debug)]
pub enum Error {
	#[error("Couldn't setup connection to underlying datastore")]
	Message(String),
	#[error("Couldn't setup connection to underlying datastore")]
	Io(#[from] io::Error),
}

impl serde::ser::Error for Error {
	fn custom<T: fmt::Display>(msg: T) -> Self {
		Error::Message(msg.to_string())
	}
}

/// Shorthand for `Result<T, bytekey::ser::Error>`.
pub type Result<T> = std::result::Result<T, Error>;

/// Serialize data into a vector of `u8` bytes.
pub fn serialize<T>(v: &T) -> Result<Vec<u8>>
where
	T: Serialize,
{
	let mut bytes = vec![];
	{
		let mut buffered = io::BufWriter::new(&mut bytes);
		serialize_into(&mut buffered, v)?;
	}
	Ok(bytes)
}

/// Serialize data into the given vector of `u8` bytes.
pub fn serialize_into<W, T>(writer: W, value: &T) -> Result<()>
where
	W: Write,
	T: Serialize,
{
	let mut serializer = Serializer::new(writer);
	value.serialize(&mut serializer)
}

impl<W> Serializer<W>
where
	W: Write,
{
	/// Creates a new ordered bytes encoder whose output will be written to the provided writer.
	pub fn new(writer: W) -> Serializer<W> {
		Serializer {
			writer,
		}
	}

	/// Encode a `u64` into a variable number of bytes.
	///
	/// The variable-length encoding scheme uses between 1 and 9 bytes depending on the value.
	/// Smaller magnitude (closer to 0) `u64`s will encode to fewer bytes.
	///
	/// ##### Encoding
	///
	/// The encoding uses the first 4 bits to store the number of trailing bytes, between 0 and 8.
	/// Subsequent bits are the input value in big-endian format with leading 0 bytes removed.
	///
	/// ##### Encoded Size
	///
	/// <table>
	///     <tr>
	///         <th>range</th>
	///         <th>size (bytes)</th>
	///     </tr>
	///     <tr>
	///         <td>[0, 2<sup>4</sup>)</td>
	///         <td>1</td>
	///     </tr>
	///     <tr>
	///         <td>[2<sup>4</sup>, 2<sup>12</sup>)</td>
	///         <td>2</td>
	///     </tr>
	///     <tr>
	///         <td>[2<sup>12</sup>, 2<sup>20</sup>)</td>
	///         <td>3</td>
	///     </tr>
	///     <tr>
	///         <td>[2<sup>20</sup>, 2<sup>28</sup>)</td>
	///         <td>4</td>
	///     </tr>
	///     <tr>
	///         <td>[2<sup>28</sup>, 2<sup>36</sup>)</td>
	///         <td>5</td>
	///     </tr>
	///     <tr>
	///         <td>[2<sup>36</sup>, 2<sup>44</sup>)</td>
	///         <td>6</td>
	///     </tr>
	///     <tr>
	///         <td>[2<sup>44</sup>, 2<sup>52</sup>)</td>
	///         <td>7</td>
	///     </tr>
	///     <tr>
	///         <td>[2<sup>52</sup>, 2<sup>60</sup>)</td>
	///         <td>8</td>
	///     </tr>
	///     <tr>
	///         <td>[2<sup>60</sup>, 2<sup>64</sup>)</td>
	///         <td>9</td>
	///     </tr>
	/// </table>
	pub fn serialize_var_u64(&mut self, val: u64) -> Result<()> {
		if val < 1 << 4 {
			self.writer.write_u8(val as u8)
		} else if val < 1 << 12 {
			self.writer.write_u16::<BE>((val as u16) | 1 << 12)
		} else if val < 1 << 20 {
			self.writer.write_u8(((val >> 16) as u8) | 2 << 4)?;
			self.writer.write_u16::<BE>(val as u16)
		} else if val < 1 << 28 {
			self.writer.write_u32::<BE>((val as u32) | 3 << 28)
		} else if val < 1 << 36 {
			self.writer.write_u8(((val >> 32) as u8) | 4 << 4)?;
			self.writer.write_u32::<BE>(val as u32)
		} else if val < 1 << 44 {
			self.writer.write_u16::<BE>(((val >> 32) as u16) | 5 << 12)?;
			self.writer.write_u32::<BE>(val as u32)
		} else if val < 1 << 52 {
			self.writer.write_u8(((val >> 48) as u8) | 6 << 4)?;
			self.writer.write_u16::<BE>((val >> 32) as u16)?;
			self.writer.write_u32::<BE>(val as u32)
		} else if val < 1 << 60 {
			self.writer.write_u64::<BE>((val as u64) | 7 << 60)
		} else {
			self.writer.write_u8(8 << 4)?;
			self.writer.write_u64::<BE>(val)
		}
		.map_err(From::from)
	}

	/// Encode an `i64` into a variable number of bytes.
	///
	/// The variable-length encoding scheme uses between 1 and 9 bytes depending on the value.
	/// Smaller magnitude (closer to 0) `i64`s will encode to fewer bytes.
	///
	/// ##### Encoding
	///
	/// The encoding uses the first bit to encode the sign: `0` for negative values and `1` for
	/// positive values. The following 4 bits store the number of trailing bytes, between 0 and 8.
	/// Subsequent bits are the absolute value of the input value in big-endian format with leading
	/// 0 bytes removed. If the original value was negative, than 1 is subtracted from the absolute
	/// value before encoding. Finally, if the value is negative, all bits except the sign bit are
	/// flipped (1s become 0s and 0s become 1s).
	///
	/// ##### Encoded Size
	///
	/// <table>
	///     <tr>
	///         <th>negative range</th>
	///         <th>positive range</th>
	///         <th>size (bytes)</th>
	///     </tr>
	///     <tr>
	///         <td>[-2<sup>3</sup>, 0)</td>
	///         <td>[0, 2<sup>3</sup>)</td>
	///         <td>1</td>
	///     </tr>
	///     <tr>
	///         <td>[-2<sup>11</sup>, -2<sup>3</sup>)</td>
	///         <td>[2<sup>3</sup>, 2<sup>11</sup>)</td>
	///         <td>2</td>
	///     </tr>
	///     <tr>
	///         <td>[-2<sup>19</sup>, -2<sup>11</sup>)</td>
	///         <td>[2<sup>11</sup>, 2<sup>19</sup>)</td>
	///         <td>3</td>
	///     </tr>
	///     <tr>
	///         <td>[-2<sup>27</sup>, -2<sup>19</sup>)</td>
	///         <td>[2<sup>19</sup>, 2<sup>27</sup>)</td>
	///         <td>4</td>
	///     </tr>
	///     <tr>
	///         <td>[-2<sup>35</sup>, -2<sup>27</sup>)</td>
	///         <td>[2<sup>27</sup>, 2<sup>35</sup>)</td>
	///         <td>5</td>
	///     </tr>
	///     <tr>
	///         <td>[-2<sup>43</sup>, -2<sup>35</sup>)</td>
	///         <td>[2<sup>35</sup>, 2<sup>43</sup>)</td>
	///         <td>6</td>
	///     </tr>
	///     <tr>
	///         <td>[-2<sup>51</sup>, -2<sup>43</sup>)</td>
	///         <td>[2<sup>43</sup>, 2<sup>51</sup>)</td>
	///         <td>7</td>
	///     </tr>
	///     <tr>
	///         <td>[-2<sup>59</sup>, -2<sup>51</sup>)</td>
	///         <td>[2<sup>51</sup>, 2<sup>59</sup>)</td>
	///         <td>8</td>
	///     </tr>
	///     <tr>
	///         <td>[-2<sup>63</sup>, -2<sup>59</sup>)</td>
	///         <td>[2<sup>59</sup>, 2<sup>63</sup>)</td>
	///         <td>9</td>
	///     </tr>
	/// </table>
	pub fn serialize_var_i64(&mut self, v: i64) -> Result<()> {
		// The mask is 0 for positive input and u64::MAX for negative input
		let mask = (v >> 63) as u64;
		let val = v.abs() as u64 - (1 & mask);
		if val < 1 << 3 {
			let masked = (val | (0x10 << 3)) ^ mask;
			self.writer.write_u8(masked as u8)
		} else if val < 1 << 11 {
			let masked = (val | (0x11 << 11)) ^ mask;
			self.writer.write_u16::<BE>(masked as u16)
		} else if val < 1 << 19 {
			let masked = (val | (0x12 << 19)) ^ mask;
			self.writer.write_u8((masked >> 16) as u8)?;
			self.writer.write_u16::<BE>(masked as u16)
		} else if val < 1 << 27 {
			let masked = (val | (0x13 << 27)) ^ mask;
			self.writer.write_u32::<BE>(masked as u32)
		} else if val < 1 << 35 {
			let masked = (val | (0x14 << 35)) ^ mask;
			self.writer.write_u8((masked >> 32) as u8)?;
			self.writer.write_u32::<BE>(masked as u32)
		} else if val < 1 << 43 {
			let masked = (val | (0x15 << 43)) ^ mask;
			self.writer.write_u16::<BE>((masked >> 32) as u16)?;
			self.writer.write_u32::<BE>(masked as u32)
		} else if val < 1 << 51 {
			let masked = (val | (0x16 << 51)) ^ mask;
			self.writer.write_u8((masked >> 48) as u8)?;
			self.writer.write_u16::<BE>((masked >> 32) as u16)?;
			self.writer.write_u32::<BE>(masked as u32)
		} else if val < 1 << 59 {
			let masked = (val | (0x17 << 59)) ^ mask;
			self.writer.write_u64::<BE>(masked as u64)
		} else {
			self.writer.write_u8((0x18 << 3) ^ mask as u8)?;
			self.writer.write_u64::<BE>(val ^ mask)
		}
		.map_err(From::from)
	}
}

impl<'a, W> serde::Serializer for &'a mut Serializer<W>
where
	W: Write,
{
	type Ok = ();
	type Error = Error;
	type SerializeSeq = Self;
	type SerializeTuple = Self;
	type SerializeTupleStruct = Self;
	type SerializeTupleVariant = Self;
	type SerializeMap = Self;
	type SerializeStruct = Self;
	type SerializeStructVariant = Self;

	fn is_human_readable(&self) -> bool {
		false
	}

	fn serialize_bool(self, v: bool) -> Result<()> {
		let b = if v {
			1
		} else {
			0
		};
		self.writer.write_u8(b)?;
		Ok(())
	}

	fn serialize_i8(self, v: i8) -> Result<()> {
		self.writer.write_i8(v ^ i8::MIN)?;
		Ok(())
	}

	fn serialize_i16(self, v: i16) -> Result<()> {
		self.writer.write_i16::<BE>(v ^ i16::MIN)?;
		Ok(())
	}

	fn serialize_i32(self, v: i32) -> Result<()> {
		self.writer.write_i32::<BE>(v ^ i32::MIN)?;
		Ok(())
	}

	fn serialize_i64(self, v: i64) -> Result<()> {
		self.writer.write_i64::<BE>(v ^ i64::MIN)?;
		Ok(())
	}

	fn serialize_u8(self, v: u8) -> Result<()> {
		self.writer.write_u8(v)?;
		Ok(())
	}

	fn serialize_u16(self, v: u16) -> Result<()> {
		self.writer.write_u16::<BE>(v)?;
		Ok(())
	}

	fn serialize_u32(self, v: u32) -> Result<()> {
		self.writer.write_u32::<BE>(v)?;
		Ok(())
	}

	fn serialize_u64(self, v: u64) -> Result<()> {
		self.writer.write_u64::<BE>(v)?;
		Ok(())
	}

	fn serialize_f32(self, v: f32) -> Result<()> {
		let val = v.to_bits() as i32;
		let t = (val >> 31) | i32::MIN;
		self.writer.write_i32::<BE>(val ^ t)?;
		Ok(())
	}

	fn serialize_f64(self, v: f64) -> Result<()> {
		let val = v.to_bits() as i64;
		let t = (val >> 63) | i64::MIN;
		self.writer.write_i64::<BE>(val ^ t)?;
		Ok(())
	}

	fn serialize_char(self, v: char) -> Result<()> {
		self.serialize_str(&v.to_string())?;
		Ok(())
	}

	fn serialize_str(self, v: &str) -> Result<()> {
		self.writer.write_all(v.as_bytes())?;
		self.writer.write_u8(0)?;
		Ok(())
	}

	fn serialize_bytes(self, v: &[u8]) -> Result<()> {
		self.writer.write_all(v)?;
		Ok(())
	}

	fn serialize_none(self) -> Result<()> {
		self.writer.write_u8(0)?;
		Ok(())
	}

	fn serialize_some<T>(self, v: &T) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		self.writer.write_u8(1)?;
		v.serialize(self)
	}

	fn serialize_unit(self) -> Result<()> {
		self.writer.write_all(&[])?;
		Ok(())
	}

	fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
		self.serialize_unit()
	}

	fn serialize_unit_variant(
		self,
		_name: &'static str,
		variant_index: u32,
		_variant: &'static str,
	) -> Result<()> {
		self.serialize_u32(variant_index)
	}

	fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(self)
	}

	fn serialize_newtype_variant<T>(
		self,
		_name: &'static str,
		variant_index: u32,
		_variant: &'static str,
		value: &T,
	) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		self.writer.write_u32::<BE>(variant_index)?;
		value.serialize(self)
	}

	fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
		Ok(self)
	}

	fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
		Ok(self)
	}

	fn serialize_tuple_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleStruct> {
		Ok(self)
	}

	fn serialize_tuple_variant(
		self,
		_name: &'static str,
		variant_index: u32,
		_variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleVariant> {
		self.writer.write_u32::<BE>(variant_index)?;
		Ok(self)
	}

	fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeStruct> {
		Ok(self)
	}

	fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
		Ok(self)
	}

	fn serialize_struct_variant(
		self,
		_name: &'static str,
		variant_index: u32,
		_variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStructVariant> {
		self.writer.write_u32::<BE>(variant_index)?;
		Ok(self)
	}
}

// Compound Implementations.

impl<'a, W> serde::ser::SerializeSeq for &'a mut Serializer<W>
where
	W: Write,
{
	type Ok = ();
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(&mut **self)
	}

	fn end(self) -> Result<()> {
		Ok(())
	}
}

impl<'a, W> serde::ser::SerializeTuple for &'a mut Serializer<W>
where
	W: Write,
{
	type Ok = ();
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(&mut **self)
	}

	fn end(self) -> Result<()> {
		Ok(())
	}
}

impl<'a, W> serde::ser::SerializeTupleStruct for &'a mut Serializer<W>
where
	W: Write,
{
	type Ok = ();
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(&mut **self)
	}

	fn end(self) -> Result<()> {
		Ok(())
	}
}

impl<'a, W> serde::ser::SerializeTupleVariant for &'a mut Serializer<W>
where
	W: Write,
{
	type Ok = ();
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(&mut **self)
	}

	fn end(self) -> Result<()> {
		Ok(())
	}
}

impl<'a, W> serde::ser::SerializeMap for &'a mut Serializer<W>
where
	W: Write,
{
	type Ok = ();
	type Error = Error;

	fn serialize_key<T>(&mut self, key: &T) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		key.serialize(&mut **self)
	}

	fn serialize_value<T>(&mut self, value: &T) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(&mut **self)
	}

	fn end(self) -> Result<()> {
		Ok(())
	}
}

impl<'a, W> serde::ser::SerializeStruct for &'a mut Serializer<W>
where
	W: Write,
{
	type Ok = ();
	type Error = Error;

	fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(&mut **self)
	}

	fn end(self) -> Result<()> {
		Ok(())
	}
}

impl<'a, W> serde::ser::SerializeStructVariant for &'a mut Serializer<W>
where
	W: Write,
{
	type Ok = ();
	type Error = Error;

	fn serialize_field<T>(&mut self, _key: &'static str, value: &T) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(&mut **self)
	}

	fn end(self) -> Result<()> {
		Ok(())
	}
}
