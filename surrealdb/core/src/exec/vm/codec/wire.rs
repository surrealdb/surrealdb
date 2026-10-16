//! The primitives `revision` 0.30 writes, with the workspace's features:
//! variable-width integers and bulk-encoded vectors of primitives.

use anyhow::{Result, bail};

use super::types::Codec;

/// Reads a value's bytes front to back.
pub(super) struct Reader<'b> {
	bytes: &'b [u8],
	/// The codec that resolves the types the value names, when it names any.
	codec: Option<&'static Codec>,
}

impl<'b> Reader<'b> {
	/// A reader of a value that names no type but the ones it holds.
	pub(super) fn new(bytes: &'b [u8]) -> Self {
		Self {
			bytes,
			codec: None,
		}
	}

	/// A reader of a value whose named types `codec` resolves.
	pub(super) fn in_codec(bytes: &'b [u8], codec: &'static Codec) -> Self {
		Self {
			bytes,
			codec: Some(codec),
		}
	}

	/// The codec that resolves the types the value names.
	pub(super) fn codec(&self) -> Result<&'static Codec> {
		self.codec.ok_or_else(|| anyhow::anyhow!("a value names a type and no codec resolves it"))
	}

	pub(super) fn is_empty(&self) -> bool {
		self.bytes.is_empty()
	}

	pub(super) fn remaining(&self) -> usize {
		self.bytes.len()
	}

	/// The bytes not yet read, for a decoder that reads from a slice and
	/// reports how far it got; [`Self::advance`] then moves past what it read.
	pub(super) fn rest(&self) -> &'b [u8] {
		self.bytes
	}

	/// Skip `n` bytes. Fails, leaving the reader where it was, when fewer
	/// than `n` remain.
	pub(super) fn advance(&mut self, n: usize) -> Result<()> {
		self.take(n).map(|_| ())
	}

	pub(super) fn take(&mut self, n: usize) -> Result<&'b [u8]> {
		if self.bytes.len() < n {
			bail!("a value ended {} bytes early", n - self.bytes.len());
		}
		let (head, rest) = self.bytes.split_at(n);
		self.bytes = rest;
		Ok(head)
	}

	pub(super) fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
		let mut out = [0u8; N];
		out.copy_from_slice(self.take(N)?);
		Ok(out)
	}

	pub(super) fn u8(&mut self) -> Result<u8> {
		Ok(self.take(1)?[0])
	}

	/// A varint: one byte below 251, else a marker and a little-endian
	/// integer of 2, 4, 8 or 16 bytes.
	pub(super) fn varint(&mut self) -> Result<u128> {
		Ok(match self.u8()? {
			b @ 0..=250 => b as u128,
			251 => u16::from_le_bytes(self.array()?) as u128,
			252 => u32::from_le_bytes(self.array()?) as u128,
			253 => u64::from_le_bytes(self.array()?) as u128,
			254 => u128::from_le_bytes(self.array()?),
			b => bail!("an invalid varint marker {b}"),
		})
	}

	/// A zigzag-encoded signed varint.
	pub(super) fn zigzag(&mut self) -> Result<i128> {
		let v = self.varint()?;
		Ok(((v >> 1) as i128) ^ -((v & 1) as i128))
	}

	pub(super) fn len(&mut self) -> Result<usize> {
		usize::try_from(self.varint()?).map_err(|_| anyhow::anyhow!("a length past usize"))
	}

	/// The bytes of a length-prefixed payload.
	pub(super) fn bytes(&mut self) -> Result<&'b [u8]> {
		let len = self.len()?;
		self.take(len)
	}

	/// The payload of an `optimised` value: a `u32` little-endian length and
	/// that many bytes.
	pub(super) fn varlen(&mut self) -> Result<Reader<'b>> {
		let len = u32::from_le_bytes(self.array()?) as usize;
		Ok(Reader {
			bytes: self.take(len)?,
			codec: self.codec,
		})
	}
}

/// Writes a value's bytes.
#[derive(Default)]
pub(super) struct Writer {
	bytes: Vec<u8>,
	/// The codec that resolves the types the value names, when it names any.
	codec: Option<&'static Codec>,
}

/// The most capacity a thread's scratch buffer keeps between encodings.
const SCRATCH_KEPT: usize = 64 * 1024;

thread_local! {
	/// The buffer [`Writer::scratch`] writes into, empty between encodings.
	static SCRATCH: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
}

impl Writer {
	pub(super) fn into_bytes(self) -> Vec<u8> {
		self.bytes
	}

	/// A writer over this thread's scratch buffer, which grows as an encoding
	/// needs and keeps its room for the next: [`Self::finish`] answers what
	/// was written in one allocation of exactly its size. A writer dropped
	/// unfinished takes the buffer with it, and the next starts empty.
	pub(super) fn scratch() -> Self {
		let bytes = SCRATCH.try_with(|s| std::mem::take(&mut *s.borrow_mut())).unwrap_or_default();
		Self {
			bytes,
			codec: None,
		}
	}

	/// This writer, writing a value whose named types `codec` resolves.
	pub(super) fn in_codec(mut self, codec: &'static Codec) -> Self {
		self.codec = Some(codec);
		self
	}

	/// The codec that resolves the types the value names.
	pub(super) fn codec(&self) -> Result<&'static Codec> {
		self.codec.ok_or_else(|| anyhow::anyhow!("a value names a type and no codec resolves it"))
	}

	/// What a [`Self::scratch`] writer wrote, copied out; the buffer goes back
	/// to the thread, emptied, unless it grew past [`SCRATCH_KEPT`].
	pub(super) fn finish(self) -> Vec<u8> {
		let mut bytes = self.bytes;
		let out = bytes.clone();
		if bytes.capacity() <= SCRATCH_KEPT {
			bytes.clear();
			let _ = SCRATCH.try_with(|s| *s.borrow_mut() = bytes);
		}
		out
	}

	pub(super) fn raw(&mut self, bytes: &[u8]) {
		self.bytes.extend_from_slice(bytes);
	}

	/// What `f` appends to the buffer, for an encoder that writes bytes
	/// itself. What is written is unspecified when `f` fails.
	pub(super) fn embedded(&mut self, f: impl FnOnce(&mut Vec<u8>) -> Result<()>) -> Result<()> {
		f(&mut self.bytes)
	}

	pub(super) fn u8(&mut self, b: u8) {
		self.bytes.push(b);
	}

	pub(super) fn varint(&mut self, v: u128) {
		if v < 251 {
			self.u8(v as u8);
		} else if v < 1 << 16 {
			self.u8(251);
			self.raw(&(v as u16).to_le_bytes());
		} else if v < 1 << 32 {
			self.u8(252);
			self.raw(&(v as u32).to_le_bytes());
		} else if v < 1 << 64 {
			self.u8(253);
			self.raw(&(v as u64).to_le_bytes());
		} else {
			self.u8(254);
			self.raw(&v.to_le_bytes());
		}
	}

	pub(super) fn zigzag(&mut self, v: i128) {
		self.varint(((v << 1) ^ (v >> 127)) as u128);
	}

	pub(super) fn bytes(&mut self, bytes: &[u8]) {
		self.varint(bytes.len() as u128);
		self.raw(bytes);
	}

	/// How many bytes are written.
	pub(super) fn len(&self) -> usize {
		self.bytes.len()
	}

	/// Reserve `n` bytes, written as zeroes, for [`Self::patch_u32`] to fill.
	pub(super) fn reserve(&mut self, n: usize) -> usize {
		let at = self.bytes.len();
		self.bytes.resize(at + n, 0);
		at
	}

	/// Overwrite the four bytes at `at`, which [`Self::reserve`] reserved,
	/// with `v` little-endian.
	pub(super) fn patch_u32(&mut self, at: usize, v: u32) {
		self.bytes[at..at + 4].copy_from_slice(&v.to_le_bytes());
	}

	/// The distance from `from` to the end of what is written, as a table
	/// offset or a payload length.
	pub(super) fn since(&self, from: usize) -> Result<u32> {
		u32::try_from(self.bytes.len() - from)
			.map_err(|_| anyhow::anyhow!("an optimised payload past u32::MAX bytes"))
	}

	/// An `optimised` payload: written by `f`, prefixed with its `u32`
	/// little-endian length. What is written is unspecified when `f` fails.
	pub(super) fn varlen(&mut self, f: impl FnOnce(&mut Writer) -> Result<()>) -> Result<()> {
		let at = self.reserve(4);
		f(self)?;
		let len = self.since(at + 4)?;
		self.patch_u32(at, len);
		Ok(())
	}
}
