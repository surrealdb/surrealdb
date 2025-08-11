use thiserror::Error;

use crate::syn::error::SyntaxError;
use crate::syn::token::Span;

#[derive(Error, Debug)]
pub enum CharError {
	#[error("found eof inside multi byte character")]
	Eof,
	#[error("string is not valid utf-8")]
	Unicode,
}

impl From<CharError> for SyntaxError {
	fn from(value: CharError) -> Self {
		let e = SyntaxError::new("Invalid, non valid UTF-8 bytes, in source");
		if let CharError::Eof = value {
			e.with_data_pending()
		} else {
			e
		}
	}
}

#[derive(Clone, Debug)]
pub struct BytesReader<'a> {
	data: &'a [u8],
	current: usize,
}

impl<'a> BytesReader<'a> {
	pub fn new(slice: &'a [u8]) -> Self {
		BytesReader {
			data: slice,
			current: 0,
		}
	}

	#[inline]
	pub fn remaining(&self) -> &'a [u8] {
		&self.data[self.current..]
	}

	#[inline]
	pub fn len(&self) -> usize {
		self.remaining().len()
	}

	#[inline]
	pub fn offset(&self) -> usize {
		self.current
	}

	#[inline]
	pub fn backup(&mut self, offset: usize) {
		assert!(offset <= self.offset());
		self.current = offset;
	}

	#[inline]
	pub fn is_empty(&self) -> bool {
		self.remaining().is_empty()
	}

	#[inline]
	pub fn peek(&self) -> Option<u8> {
		self.remaining().first().copied()
	}

	#[inline]
	pub fn peek1(&self) -> Option<u8> {
		self.remaining().get(1).copied()
	}

	#[inline]
	pub fn span(&self, span: Span) -> &'a [u8] {
		&self.data[(span.offset as usize)..(span.offset as usize + span.len as usize)]
	}

	#[inline]
	pub fn next_continue_byte(&mut self) -> Result<u8, CharError> {
		const CONTINUE_BYTE_PREFIX_MASK: u8 = 0b1100_0000;
		const CONTINUE_BYTE_MASK: u8 = 0b0011_1111;

		let byte = self.next().ok_or(CharError::Eof)?;
		if byte & CONTINUE_BYTE_PREFIX_MASK != 0b1000_0000 {
			return Err(CharError::Unicode);
		}

		Ok(byte & CONTINUE_BYTE_MASK)
	}

	pub fn convert_to_char(&mut self, start: u8) -> Result<char, CharError> {
		if start.is_ascii() {
			return Ok(start as char);
		}
		self.complete_char(start)
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
			_ => Err(CharError::Unicode),
		}
	}
}

impl Iterator for BytesReader<'_> {
	type Item = u8;

	#[inline]
	fn next(&mut self) -> Option<Self::Item> {
		let res = self.peek()?;
		self.current += 1;
		Some(res)
	}
	fn size_hint(&self) -> (usize, Option<usize>) {
		let len = self.len();
		(len, Some(len))
	}
}

impl ExactSizeIterator for BytesReader<'_> {
	fn len(&self) -> usize {
		self.len()
	}
}
