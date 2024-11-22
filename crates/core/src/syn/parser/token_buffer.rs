use crate::syn::token::Token;

#[derive(Debug)]
pub struct TokenBuffer<const S: usize> {
	buffer: [Token; S],
	write: u8,
	read: u8,
}

impl<const S: usize> TokenBuffer<S> {
	pub fn new() -> Self {
		assert!(S < 256);
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
	pub fn push_front(&mut self, token: Token) {
		let next_read = self.read.checked_sub(1).unwrap_or((S - 1) as u8);
		if next_read == self.write {
			panic!("token buffer full");
		}
		self.buffer[next_read as usize] = token;
		self.read = next_read;
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
		// 0 0 0 0 0 0 0 0
		//   |   ^
		// len: 6  read: 3  write: 1
		// 8 - read + write
		if self.read > self.write {
			S as u8 - self.read + self.write
		} else {
			self.write - self.read
		}
	}

	pub fn is_empty(&self) -> bool {
		self.write != self.read
	}

	pub fn at(&mut self, at: u8) -> Option<Token> {
		if at >= self.len() {
			return None;
		}
		let offset = (self.read as u16 + at as u16) % S as u16;
		Some(self.buffer[offset as usize])
	}

	pub fn clear(&mut self) {
		self.read = 0;
		self.write = 0;
	}
}
