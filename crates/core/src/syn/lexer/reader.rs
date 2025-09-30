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

// Generally we want to attach a span to errors, but when dealing with utf-8 errors we cannot
// correctly display the source so we cannot attach a meaningfull span.
impl From<CharError> for SyntaxError {
	fn from(_: CharError) -> Self {
		SyntaxError::new("Invalid, non valid UTF-8 bytes, in source")
	}
}

#[derive(Clone, Debug)]
pub struct BytesReader<'a> {
	data: &'a [u8],
	current: u32,
}

impl<'a> BytesReader<'a> {
	pub fn new(slice: &'a [u8]) -> Self {
		debug_assert!(
			slice.len() < u32::MAX as usize,
			"BytesReader got a string which was too large for lexing"
		);
		BytesReader {
			data: slice,
			current: 0,
		}
	}

	#[inline]
	pub fn remaining(&self) -> &'a [u8] {
		&self.data[(self.current as usize)..]
	}

	#[inline]
	pub fn len(&self) -> u32 {
		self.remaining().len() as u32
	}

	#[inline]
	pub fn offset(&self) -> u32 {
		self.current
	}

	#[inline]
	pub fn backup(&mut self, offset: u32) {
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
	pub fn eat(&mut self, c: u8) -> bool {
		if self.peek() == Some(c) {
			self.current += 1;
			true
		} else {
			false
		}
	}

	#[inline]
	pub fn span(&self, span: Span) -> &'a [u8] {
		&self.data[(span.offset as usize)..(span.offset as usize + span.len as usize)]
	}

	#[inline]
	pub fn span_since(&self, offset: u32) -> Span {
		assert!(offset <= self.offset(), "Tried to get a span from a offset read in the future");
		Span {
			offset,
			len: self.offset() - offset,
		}
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

	#[inline]
	pub fn convert_to_char(&mut self, start: u8) -> Result<char, CharError> {
		if start.is_ascii() {
			return Ok(start as char);
		}
		self.complete_char(start)
	}

	#[inline]
	pub fn complete_char(&mut self, start: u8) -> Result<char, CharError> {
		debug_assert!(!start.is_ascii(), "complete_char should not be handed ascii bytes");
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
		(len as usize, Some(len as usize))
	}
}

impl ExactSizeIterator for BytesReader<'_> {
	fn len(&self) -> usize {
		self.len() as usize
	}
}
