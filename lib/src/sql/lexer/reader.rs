use crate::sql::token::Span;
use std::{fmt, marker::PhantomData, ptr::NonNull};

pub enum CharError {
	Eof,
	Unicode,
}

#[derive(Clone)]
pub struct BytesReader<'a> {
	start: NonNull<u8>,
	end: NonNull<u8>,
	current: NonNull<u8>,
	marker: PhantomData<&'a [u8]>,
}

impl fmt::Debug for BytesReader<'_> {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("BytesReader")
			.field("used", &self.used())
			.field("remaining", &self.remaining())
			.finish()
	}
}

impl<'a> BytesReader<'a> {
	pub fn new(slice: &'a [u8]) -> Self {
		let len = slice.len();
		let start = slice.as_ptr() as *mut u8;
		// SAFETY: len is derived from the slice so the end pointer is still inside the range for
		// providance.
		let end = unsafe { start.add(len) };

		// Pointers derived from references to non-empty sized slice are never null but
		// I couldn't find information about slices which are empty. Testing seems to indicate
		// that they will always have a non-null value but I can't be certain of that.
		let start = NonNull::new(start).unwrap();

		// SAFETY: Since start was non-null end should also be.
		let end = unsafe { NonNull::new_unchecked(end) };

		BytesReader {
			start,
			end,
			current: start,
			marker: PhantomData,
		}
	}

	#[inline]
	pub fn full(&self) -> &'a [u8] {
		unsafe {
			// SAFETY: start and end are created from the same pointer so have the
			// same providance.
			// Furthermore implementation ensures that end is always past start.
			let len = self.end.as_ptr().offset_from(self.start.as_ptr()) as usize;
			// SAFETY: We are essentially recreating the original slice here, since we keep track
			// of the lifetime with the marker type this is like returning the original slice we
			// passed in.
			std::slice::from_raw_parts::<'a, u8>(self.start.as_ptr(), len)
		}
	}

	#[inline]
	pub fn used(&self) -> &'a [u8] {
		unsafe {
			// SAFETY: current and end are created from the same pointer so have the
			// same providance.
			// Furthermore implementation ensures that end is always past start.
			let len = self.current.as_ptr().offset_from(self.start.as_ptr()) as usize;
			// SAFETY: We are essentially recreating the original slice here, since we keep track
			// of the lifetime with the marker type this is like returning the original slice we
			// passed in.
			std::slice::from_raw_parts::<'a, u8>(self.start.as_ptr(), len)
		}
	}

	#[inline]
	pub fn remaining(&self) -> &'a [u8] {
		unsafe {
			// SAFETY: current and end are created from the same pointer so have the
			// same providance.
			// Furthermore implementation ensures that end is always past start.
			let len = self.end.as_ptr().offset_from(self.current.as_ptr()) as usize;
			// SAFETY: We are essentially recreating the original slice here, since we keep track
			// of the lifetime with the marker type this is like returning the original slice we
			// passed in.
			std::slice::from_raw_parts::<'a, u8>(self.current.as_ptr(), len)
		}
	}

	pub fn len(&self) -> usize {
		unsafe {
			// SAFETY: current and end are created from the same pointer so have the
			// same providance.
			// Furthermore implementation ensures that end is always past start.
			self.end.as_ptr().offset_from(self.current.as_ptr()) as usize
		}
	}

	pub fn offset(&self) -> usize {
		unsafe {
			// SAFETY: current and start are created from the same pointer so have the
			// same providance.
			// Furthermore implementation ensures that end is always past start.
			self.current.as_ptr().offset_from(self.start.as_ptr()) as usize
		}
	}

	pub fn backup(&mut self, offset: usize) {
		assert!(offset <= self.offset());
		self.current = unsafe { NonNull::new_unchecked(self.start.as_ptr().add(offset)) };
	}

	pub fn is_empty(&self) -> bool {
		self.end == self.current
	}

	pub fn peek(&self) -> Option<u8> {
		if self.end == self.current {
			None
		} else {
			// SAFETY: Implementation ensures that current points to an existing byte at this point.
			unsafe { Some(self.current.as_ptr().read()) }
		}
	}

	pub fn span(&self, span: Span) -> &[u8] {
		assert!(((span.offset + span.len) as usize) < self.full().len());
		unsafe {
			let ptr = self.start.as_ptr().add(span.offset as usize);
			std::slice::from_raw_parts(ptr, span.len as usize)
		}
	}

	pub fn next_continue_byte(&mut self) -> Result<u8, CharError> {
		const CONTINUE_BYTE_PREFIX_MASK: u8 = 0b1100_0000;
		const CONTINUE_BYTE_MASK: u8 = 0b0011_1111;

		let byte = self.next().ok_or(CharError::Eof)?;
		if byte & CONTINUE_BYTE_PREFIX_MASK != 0b1000_0000 {
			return Err(CharError::Eof);
		}

		Ok(byte & CONTINUE_BYTE_MASK)
	}

	pub fn complete_char(&mut self, start: u8) -> Result<char, CharError> {
		match start & 0b1111_1000 {
			0b1100_0000 | 0b1101_0000 | 0b1100_1000 | 0b1101_1000 => {
				let mut val = (start & 0b0001_1111) as u32;
				val <<= 6;
				let next = self.next_continue_byte()?;
				val |= next as u32;
				char::from_u32(val).ok_or(CharError::Unicode)
			}
			0b1110_0000 | 0b1110_1000 => {
				let mut val = (start & 0b0000_1111) as u32;
				val <<= 6;
				let next = self.next_continue_byte()?;
				val |= next as u32;
				val <<= 6;
				let next = self.next_continue_byte()?;
				val |= next as u32;
				char::from_u32(val).ok_or(CharError::Unicode)
			}
			0b1111_0000 => {
				let mut val = (start & 0b0000_0111) as u32;
				val <<= 6;
				let next = self.next_continue_byte()?;
				val |= next as u32;
				val <<= 6;
				let next = self.next_continue_byte()?;
				val |= next as u32;
				val <<= 6;
				let next = self.next_continue_byte()?;
				val |= next as u32;
				char::from_u32(val).ok_or(CharError::Unicode)
			}
			x => panic!("start byte did not start multi byte character: {:b}", x),
		}
	}
}

impl<'a> Iterator for BytesReader<'a> {
	type Item = u8;

	#[inline]
	fn next(&mut self) -> Option<Self::Item> {
		if self.end == self.current {
			return None;
		}
		// SAFETY: Implementation ensures that current points to an existing byte at this point.
		let res = unsafe { self.current.as_ptr().read() };
		// SAFETY: current was non-null so adding one should keep it non-null.
		// SAFETY: Implementation ensures that current is between self.start and self.end so
		// adding one will keep the pointer inside the providance range.
		self.current = unsafe { NonNull::new_unchecked(self.current.as_ptr().add(1)) };
		Some(res)
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		let len = self.len();
		(len, Some(len))
	}
}

impl<'a> ExactSizeIterator for BytesReader<'a> {
	fn len(&self) -> usize {
		self.len()
	}
}
