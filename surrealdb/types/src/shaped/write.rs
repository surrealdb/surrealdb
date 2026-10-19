//! Writing rows and blocks.

use std::sync::Arc;

use super::{
	CBOR_TAG, Column, MAX_EMPTY_ROWS, MAX_SHAPES, MESSAGE_BATCH, MESSAGE_SHAPE, STATE_VALUE, Shape,
	ShapedError, VERSION,
};

/// One row of a shape being written at the end of a buffer.
///
/// [`begin`](Self::begin) reserves the row's presence region, fixed region and ends table, all
/// zero: every column absent (or `NULL`, for a column that may only be `NULL`), every fixed slot
/// zero, every var slot empty. The caller then sets presences and fixed slots in any order, and
/// writes var slots in ascending slot order, each between [`open_var`](Self::open_var) and
/// [`close_var`](Self::close_var); a slot passed over is left empty. [`finish`](Self::finish)
/// completes the ends table.
pub struct RowWriter<'o> {
	out: &'o mut Vec<u8>,
	start: usize,
	fixed_at: usize,
	ends_at: usize,
	var_at: usize,
	var_count: u32,
	next: u32,
}

impl<'o> RowWriter<'o> {
	/// Starts a row of `shape` at the end of `out`.
	pub fn begin(out: &'o mut Vec<u8>, shape: &Shape) -> Self {
		let start = out.len();
		out.resize(start + shape.header_len(), 0);
		Self {
			start,
			fixed_at: start + shape.presence_len(),
			ends_at: start + shape.ends_at(),
			var_at: start + shape.header_len(),
			var_count: shape.def().var_count,
			next: 0,
			out,
		}
	}

	/// Starts a row of `shape` at the end of `out` whose presence and fixed regions are `head`, as
	/// they lie, and whose ends table is zero.
	pub fn begin_with(
		out: &'o mut Vec<u8>,
		shape: &Shape,
		head: &[u8],
	) -> Result<Self, ShapedError> {
		if head.len() != shape.ends_at() {
			return Err(ShapedError::new("a row head of another size than its shape's"));
		}
		let start = out.len();
		out.extend_from_slice(head);
		out.resize(start + shape.header_len(), 0);
		Ok(Self {
			start,
			fixed_at: start + shape.presence_len(),
			ends_at: start + shape.ends_at(),
			var_at: start + shape.header_len(),
			var_count: shape.def().var_count,
			next: 0,
			out,
		})
	}

	/// Where the row starts in the buffer.
	pub fn start(&self) -> usize {
		self.start
	}

	/// Where the row's fixed region starts in the buffer.
	pub fn fixed_at(&self) -> usize {
		self.fixed_at
	}

	/// The buffer, for writing a var slot's bytes between `open_var` and `close_var`, or fixed
	/// slots at [`fixed_at`](Self::fixed_at).
	pub fn out(&mut self) -> &mut Vec<u8> {
		self.out
	}

	/// Sets `column`'s presence to `state` ([`STATE_ABSENT`](super::STATE_ABSENT) and the other
	/// states), in the bits its flags give it; a required column has none. A one-bit column holds
	/// a value as a set bit and its one other state as a clear bit.
	#[inline]
	pub fn set_presence(&mut self, column: &Column, state: u8) {
		match column.presence_width() {
			0 => {}
			1 => self.set_bits(column.presence, 1, u8::from(state == STATE_VALUE)),
			_ => self.set_bits(column.presence, 2, state),
		}
	}

	/// Sets the `width` presence bits from bit `bit` to the low bits of `value`.
	#[inline]
	pub fn set_bits(&mut self, bit: u32, width: u32, value: u8) {
		for i in 0..width {
			let at = bit + i;
			let byte = &mut self.out[self.start + (at / 8) as usize];
			let mask = 1u8 << (at % 8);
			if (value >> i) & 1 == 1 {
				*byte |= mask;
			} else {
				*byte &= !mask;
			}
		}
	}

	/// Sets bit `bit` of the fixed region to `value`.
	#[inline]
	pub fn fixed_bit(&mut self, bit: u32, value: bool) {
		let byte = &mut self.out[self.fixed_at + (bit / 8) as usize];
		let mask = 1u8 << (bit % 8);
		if value {
			*byte |= mask;
		} else {
			*byte &= !mask;
		}
	}

	/// Writes `bytes` at fixed offset `offset`.
	#[inline]
	pub fn fixed(&mut self, offset: u32, bytes: &[u8]) {
		let at = self.fixed_at + offset as usize;
		self.out[at..at + bytes.len()].copy_from_slice(bytes);
	}

	/// Starts var slot `slot`; every slot before it not yet written is left empty.
	#[inline]
	pub fn open_var(&mut self, slot: u32) -> Result<(), ShapedError> {
		if slot < self.next || slot >= self.var_count {
			return Err(ShapedError::new("var slots written out of order"));
		}
		if slot > self.next {
			let end = self.var_len()?;
			while self.next < slot {
				self.set_end(self.next, end);
				self.next += 1;
			}
		}
		Ok(())
	}

	/// Ends the var slot [`open_var`](Self::open_var) started at the buffer's end.
	#[inline]
	pub fn close_var(&mut self) -> Result<(), ShapedError> {
		let end = self.var_len()?;
		self.set_end(self.next, end);
		self.next += 1;
		Ok(())
	}

	/// Writes var slots `first..first + ends.len()` at once: `bytes` back to back, slot `first + k`
	/// ending `ends[k]` bytes into them. Every slot before `first` not yet written is left empty;
	/// `ends` ascend and the last is `bytes.len()`.
	pub fn var_run(
		&mut self,
		first: u32,
		bytes: &[u8],
		ends: impl ExactSizeIterator<Item = u32>,
	) -> Result<(), ShapedError> {
		let count = ends.len() as u32;
		if first.checked_add(count).is_none_or(|end| end > self.var_count) {
			return Err(ShapedError::new("var slots written out of order"));
		}
		self.open_var(first)?;
		let base = self.var_len()?;
		self.out.extend_from_slice(bytes);
		for (k, end) in ends.enumerate() {
			let end = base
				.checked_add(end)
				.ok_or_else(|| ShapedError::new("a row's var region larger than 4 GiB"))?;
			self.set_end(first + k as u32, end);
		}
		self.next = first + count;
		Ok(())
	}

	/// Completes the row.
	pub fn finish(mut self) -> Result<(), ShapedError> {
		let end = self.var_len()?;
		while self.next < self.var_count {
			self.set_end(self.next, end);
			self.next += 1;
		}
		Ok(())
	}

	fn var_len(&self) -> Result<u32, ShapedError> {
		u32::try_from(self.out.len() - self.var_at)
			.map_err(|_| ShapedError::new("a row's var region larger than 4 GiB"))
	}

	#[inline]
	fn set_end(&mut self, slot: u32, end: u32) {
		let at = self.ends_at + 4 * slot as usize;
		self.out[at..at + 4].copy_from_slice(&end.to_le_bytes());
	}
}

/// The shapes one scope has announced: a shape's wire id is its position.
#[derive(Debug, Default)]
pub struct ShapeScope {
	announced: Vec<Arc<Shape>>,
}

impl ShapeScope {
	/// A scope that has announced no shape.
	pub fn new() -> Self {
		Self::default()
	}

	/// The wire id of `shape` in this scope, writing its SHAPE message to `out` the first time.
	pub fn id_of(&mut self, shape: &Arc<Shape>, out: &mut Vec<u8>) -> Result<u32, ShapedError> {
		if let Some(at) =
			self.announced.iter().rposition(|s| Arc::ptr_eq(s, shape) || **s == **shape)
		{
			return Ok(at as u32);
		}
		if self.announced.len() >= MAX_SHAPES {
			return Err(ShapedError::new("too many shapes in one scope"));
		}
		let id = self.announced.len() as u32;
		let payload = shape.payload();
		let len = u32::try_from(4 + payload.len())
			.map_err(|_| ShapedError::new("a shape larger than 4 GiB"))?;
		out.push(MESSAGE_SHAPE);
		out.extend_from_slice(&len.to_le_bytes());
		out.extend_from_slice(&id.to_le_bytes());
		out.extend_from_slice(payload);
		self.announced.push(Arc::clone(shape));
		Ok(id)
	}
}

/// The CBOR head of a block: tag [`CBOR_TAG`], then a byte string of `len` bytes, its length in
/// the four-byte form.
pub fn block_head(len: u32) -> [u8; 7] {
	let [a, b, c, d] = len.to_be_bytes();
	[0xd8, CBOR_TAG as u8, 0x5a, a, b, c, d]
}

/// Writes a block to `out`: the CBOR head, the version, then for each run (rows of one shape, the
/// row count, the rows' bytes back to back) the shape's SHAPE message if `scope` has not announced
/// it, then a BATCH of the run.
pub fn write_block<'r>(
	out: &mut Vec<u8>,
	scope: &mut ShapeScope,
	runs: impl IntoIterator<Item = (&'r Arc<Shape>, u32, &'r [u8])>,
) -> Result<(), ShapedError> {
	let head = out.len();
	out.extend_from_slice(&block_head(0));
	let contents = out.len();
	out.push(VERSION);
	for (shape, count, rows) in runs {
		let id = scope.id_of(shape, out)?;
		// Rows of a zero-width shape have no bytes to split on, so a long run is cut by count.
		let mut left = count;
		let mut rows = rows;
		loop {
			let take = if shape.header_len() == 0 {
				left.min(MAX_EMPTY_ROWS)
			} else {
				left
			};
			let len = u32::try_from(8 + rows.len())
				.map_err(|_| ShapedError::new("a batch larger than 4 GiB"))?;
			out.push(MESSAGE_BATCH);
			out.extend_from_slice(&len.to_le_bytes());
			out.extend_from_slice(&id.to_le_bytes());
			out.extend_from_slice(&take.to_le_bytes());
			out.extend_from_slice(rows);
			rows = &[];
			left -= take;
			if left == 0 {
				break;
			}
		}
	}
	let len = u32::try_from(out.len() - contents)
		.map_err(|_| ShapedError::new("a block larger than 4 GiB"))?;
	out[head..contents].copy_from_slice(&block_head(len));
	Ok(())
}
