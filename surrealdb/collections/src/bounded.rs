//! Reads of `revision` length-prefixed data that allocate only as bytes arrive.
//!
//! A length read from the input is a claim, not a fact. Every reader here preallocates at most
//! [`PREALLOC_BYTES`] for a claimed length and grows the buffer only as bytes are actually read,
//! so a claim that runs past the end of the input costs at most [`PREALLOC_BYTES`] beyond the bytes
//! the input holds and ends in [`Error::Io`] (`UnexpectedEof`) or the item's own error, never in a
//! capacity overflow or an allocation sized by the claim.
//!
//! The framing read is exactly what `revision`'s derive and its `String`, `Bytes` and `Vec<T>`
//! readers read: a varint length, then the items. Nothing here changes a byte of what a serializer
//! writes.

use std::io::Read;

use revision::{DeserializeRevisioned, Error};

/// The most memory, in bytes, that a read preallocates for a length taken from the input.
///
/// Claims up to this size are served with one exact allocation and one `read_exact`; larger claims
/// are read through a growing buffer. `surrealdb_strand` mirrors this value as its own streaming
/// threshold, since the two crates do not depend on each other.
pub const PREALLOC_BYTES: usize = 64 * 1024;

/// The element capacity to preallocate for a claimed element count of `T`.
///
/// This is `claimed` capped so that the allocation stays within [`PREALLOC_BYTES`]; a zero-sized
/// `T` is treated as one byte wide, so the cap never divides by zero.
#[inline]
#[must_use]
pub fn prealloc<T>(claimed: usize) -> usize {
	claimed.min(PREALLOC_BYTES / size_of::<T>().max(1))
}

/// Reads a type's `u16` revision and checks it is one this build knows (`1..=max`).
///
/// The refusal carries the same message as the `revision` derive's own.
#[inline]
pub fn read_revision<R: Read>(r: &mut R, ty: &str, max: u16) -> Result<u16, Error> {
	let revision = u16::deserialize_revisioned(r)?;
	if revision == 0 || revision > max {
		return Err(Error::Deserialize(format!("Invalid revision `{revision}` for type `{ty}`")));
	}
	Ok(revision)
}

/// Reads a length prefix (a `revision` varint `usize`).
#[inline]
pub fn read_len<R: Read>(r: &mut R) -> Result<usize, Error> {
	usize::deserialize_revisioned(r)
}

/// Reads `len` items with `item`, preallocating at most [`PREALLOC_BYTES`].
///
/// The vector grows only as items are decoded, so `len` larger than the input ends in the first
/// failing `item` call.
pub fn read_vec_with<T, R: Read>(
	r: &mut R,
	len: usize,
	mut item: impl FnMut(&mut R) -> Result<T, Error>,
) -> Result<Vec<T>, Error> {
	let mut out = Vec::with_capacity(prealloc::<T>(len));
	for _ in 0..len {
		out.push(item(r)?);
	}
	Ok(out)
}

/// Reads a length-prefixed `Vec<T>`: a varint length, then that many items.
pub fn read_vec<T: DeserializeRevisioned, R: Read>(r: &mut R) -> Result<Vec<T>, Error> {
	let len = read_len(r)?;
	read_vec_with(r, len, T::deserialize_revisioned)
}

/// Reads exactly `len` bytes, allocating only as they arrive.
///
/// Up to [`PREALLOC_BYTES`] this is one exact allocation and one `read_exact`. Beyond it the bytes
/// are read through a buffer that starts at [`PREALLOC_BYTES`] and grows with what is read; a short
/// input is [`Error::Io`] with `UnexpectedEof`.
pub fn read_bytes<R: Read>(r: &mut R, len: usize) -> Result<Vec<u8>, Error> {
	if len <= PREALLOC_BYTES {
		let mut buf = vec![0u8; len];
		r.read_exact(&mut buf).map_err(Error::Io)?;
		return Ok(buf);
	}
	let mut buf = Vec::with_capacity(PREALLOC_BYTES);
	let read = Read::take(&mut *r, len as u64).read_to_end(&mut buf).map_err(Error::Io)?;
	if read != len {
		return Err(Error::Io(std::io::ErrorKind::UnexpectedEof.into()));
	}
	Ok(buf)
}

/// Reads a length-prefixed UTF-8 string: a varint length, then its bytes.
pub fn read_string<R: Read>(r: &mut R) -> Result<String, Error> {
	let len = read_len(r)?;
	let bytes = read_bytes(r, len)?;
	String::from_utf8(bytes).map_err(|e| Error::Utf8Error(e.utf8_error()))
}

/// Reads an optimised payload or body: a `u32` little-endian length, then that many bytes.
pub fn read_payload<R: Read>(r: &mut R) -> Result<Vec<u8>, Error> {
	let mut len = [0u8; 4];
	r.read_exact(&mut len).map_err(Error::Io)?;
	// `u32` always fits in `usize` on every target the workspace builds for.
	let len = usize::try_from(u32::from_le_bytes(len))
		.map_err(|_| Error::Deserialize("payload length overflow".into()))?;
	read_bytes(r, len)
}

/// Checks that a payload was fully consumed: `rest` is what remains of it after decoding.
pub fn finish(rest: &[u8]) -> Result<(), Error> {
	if rest.is_empty() {
		Ok(())
	} else {
		Err(Error::Deserialize(format!("{} trailing bytes in a revision payload", rest.len())))
	}
}
