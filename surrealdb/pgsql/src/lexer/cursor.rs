//! Safe cursor over source text for lexical scanning.

use crate::span::Span;

/// Safe zero-copy cursor over UTF-8 source string.
#[derive(Debug, Clone)]
pub struct Cursor<'a> {
	src: &'a str,
	pos: usize,
}

impl<'a> Cursor<'a> {
	/// Create a new cursor starting at position 0.
	#[inline]
	pub const fn new(src: &'a str) -> Self {
		Self {
			src,
			pos: 0,
		}
	}

	/// Return the full source text.
	#[inline]
	pub const fn src(&self) -> &'a str {
		self.src
	}

	/// Current byte offset in the source.
	#[inline]
	pub const fn pos(&self) -> u32 {
		self.pos as u32
	}

	/// Return the remaining slice of source text.
	#[inline]
	pub fn rest(&self) -> &'a str {
		self.src.get(self.pos..).unwrap_or("")
	}

	/// Return `true` if the cursor has reached the end of the input.
	#[inline]
	pub fn is_eof(&self) -> bool {
		self.pos >= self.src.len()
	}

	/// Peek the next character without advancing the cursor.
	#[inline]
	pub fn peek(&self) -> Option<char> {
		self.rest().chars().next()
	}

	/// Peek the character at `offset` characters ahead without advancing.
	#[inline]
	pub fn peek_char_at(&self, offset: usize) -> Option<char> {
		self.rest().chars().nth(offset)
	}

	/// Peek the next raw byte without advancing.
	#[inline]
	pub fn peek_byte(&self) -> Option<u8> {
		self.src.as_bytes().get(self.pos).copied()
	}

	/// Peek the raw byte at `offset` bytes ahead.
	#[inline]
	pub fn peek_byte_at(&self, offset: usize) -> Option<u8> {
		let idx = self.pos.checked_add(offset)?;
		self.src.as_bytes().get(idx).copied()
	}

	/// Return `true` if the upcoming characters match `prefix`.
	#[inline]
	pub fn starts_with(&self, prefix: &str) -> bool {
		self.rest().starts_with(prefix)
	}

	/// Return `true` if the upcoming characters match `prefix` case-insensitively (ASCII).
	#[inline]
	pub fn starts_with_ignore_ascii_case(&self, prefix: &str) -> bool {
		if let Some(slice) = self.src.get(self.pos..self.pos.saturating_add(prefix.len())) {
			slice.eq_ignore_ascii_case(prefix)
		} else {
			false
		}
	}

	/// Consume and return the next character.
	#[inline]
	pub fn bump(&mut self) -> Option<char> {
		let c = self.peek()?;
		self.pos = self.pos.saturating_add(c.len_utf8());
		Some(c)
	}

	/// Consume and return the next byte.
	#[inline]
	pub fn bump_byte(&mut self) -> Option<u8> {
		let b = self.peek_byte()?;
		self.pos = self.pos.saturating_add(1);
		Some(b)
	}

	/// Advance the cursor by `n` bytes.
	#[inline]
	pub fn advance_bytes(&mut self, n: usize) {
		self.pos = (self.pos.saturating_add(n)).min(self.src.len());
	}

	/// Consume the next char if it equals `expected`.
	#[inline]
	pub fn eat_char(&mut self, expected: char) -> bool {
		if self.peek() == Some(expected) {
			self.bump();
			true
		} else {
			false
		}
	}

	/// Consume the next string if it matches `prefix`.
	#[inline]
	pub fn eat_str(&mut self, prefix: &str) -> bool {
		if self.starts_with(prefix) {
			self.advance_bytes(prefix.len());
			true
		} else {
			false
		}
	}

	/// Safely slice the source from `start` to current position.
	#[inline]
	pub fn slice_from(&self, start: u32) -> Option<&'a str> {
		let range = start as usize..self.pos;
		self.src.get(range)
	}

	/// Safely slice a specific span from the source.
	#[inline]
	pub fn slice(&self, span: Span) -> Option<&'a str> {
		span.slice(self.src)
	}
}
