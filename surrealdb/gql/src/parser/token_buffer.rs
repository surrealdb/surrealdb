//! A fixed size token ring buffer, copied from
//! [`crate::syn::parser::token_buffer`] for the GQL token type.

use crate::gql::token::Token;

#[derive(Debug)]
pub struct TokenBuffer<const S: usize> {
	buffer: [Token; S],
	write: u8,
	read: u8,
}

impl<const S: usize> TokenBuffer<S> {
	pub fn new() -> Self {
		const {
			assert!(S < 256);
		}
		Self {
			buffer: [Token::invalid(); S],
			write: 0,
			read: 0,
		}
	}

	#[inline]
	pub fn push(&mut self, token: Token) {
		let next_write = self.write.wrapping_add(1) % S as u8;
		if next_write == self.read {
			panic!("token buffer full");
		}
		self.buffer[self.write as usize] = token;
		self.write = next_write;
	}

	#[inline]
	pub fn pop(&mut self) -> Option<Token> {
		if self.write == self.read {
			return None;
		}
		let res = self.buffer[self.read as usize];
		self.read = self.read.wrapping_add(1) % S as u8;
		Some(res)
	}

	#[inline]
	pub fn first(&mut self) -> Option<Token> {
		if self.write == self.read {
			return None;
		}
		Some(self.buffer[self.read as usize])
	}

	pub fn len(&self) -> u8 {
		if self.read > self.write {
			S as u8 - self.read + self.write
		} else {
			self.write - self.read
		}
	}

	pub fn at(&mut self, at: u8) -> Option<Token> {
		if at >= self.len() {
			return None;
		}
		let offset = (self.read as u16 + at as u16) % S as u16;
		Some(self.buffer[offset as usize])
	}
}
