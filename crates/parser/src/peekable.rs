use std::mem::MaybeUninit;

use common::span::Span;
use logos::Lexer;

use token::Token;
use token::{BaseTokenKind, Joined, LexError};

#[derive(Debug)]
pub struct PeekableLexer<'source, const SIZE: usize> {
	lexer: Lexer<'source, BaseTokenKind>,
	peek: [MaybeUninit<Result<Token, LexError>>; SIZE],
	read: u8,
	write: u8,
}

impl<'source, const SIZE: usize> Clone for PeekableLexer<'source, SIZE> {
	fn clone(&self) -> Self {
		Self {
			lexer: self.lexer.clone(),
			peek: self.peek,
			read: self.read,
			write: self.write,
		}
	}
}

impl<'source, const SIZE: usize> PeekableLexer<'source, SIZE> {
	const _ASSERT_SIZE: () = const {
		assert!(SIZE < u8::MAX as usize, "Peekable Lexer SIZE must be less then u8::MAX");
		assert!(SIZE.is_power_of_two(), "Peekable lexer SIZE must be a power of 2");
	};

	pub fn new(lexer: Lexer<'source, BaseTokenKind>) -> Self {
		PeekableLexer {
			lexer,
			peek: [const { MaybeUninit::uninit() }; SIZE],
			read: 0,
			write: 0,
		}
	}

	#[inline]
	fn lex_token(&mut self) -> Option<Result<Token, LexError>> {
		self.lexer.extras = Joined::Joined;
		self.lexer.next().map(|res| {
			res.map(|token| {
				let span = self.lexer.span();
				Token {
					token,
					joined: self.lexer.extras,
					span: Span::from_range((span.start as u32)..(span.end as u32)),
				}
			})
		})
	}

	#[inline]
	pub fn peek<const OFFSET: u8>(&mut self) -> Option<Result<Token, LexError>> {
		const {
			assert!(
				(OFFSET as usize) < SIZE,
				"peek offset must be less then size of the peek buffer"
			)
		};
		if OFFSET == 0 {
			if self.read != self.write {
				return Some(unsafe { self.peek[self.read as usize].assume_init() });
			} else {
				let x = self.lex_token()?;
				self.peek[self.write as usize] = MaybeUninit::new(x);
				self.write += 1;
				self.write &= (SIZE as u8) - 1;
				Some(x)
			}
		} else {
			let mut write = self.write as usize;
			let read = self.read as usize;
			if write < read {
				write += SIZE;
			}
			for i in write..(read + OFFSET as usize) {
				let x = self.lex_token()?;
				let idx = i & SIZE - 1;
				self.peek[idx] = MaybeUninit::new(x);
				self.write = idx as u8;
			}
			Some(unsafe {
				self.peek[((self.read + OFFSET) & ((SIZE as u8) - 1)) as usize].assume_init()
			})
		}
	}

	#[inline]
	pub fn peek_span(&mut self) -> Span {
		if self.read != self.write {
			match unsafe { self.peek.get_unchecked(self.read as usize).assume_init_ref() } {
				Ok(x) => x.span,
				Err(e) => e.span(),
			}
		} else if let Some(x) = self.peek::<0>() {
			match x {
				Ok(x) => x.span,
				Err(e) => e.span(),
			}
		} else {
			self.eof_span()
		}
	}

	#[inline]
	pub fn next(&mut self) -> Option<Result<Token, LexError>> {
		if self.read == self.write {
			return self.lex_token();
		}
		let res = unsafe { self.peek[self.read as usize].assume_init() };
		self.read = (self.read + 1) & ((SIZE as u8) - 1);
		Some(res)
	}

	#[inline]
	pub fn is_empty(&self) -> bool {
		self.read == self.write
	}

	#[inline]
	pub fn lexer(&mut self) -> &mut Lexer<'source, BaseTokenKind> {
		&mut self.lexer
	}

	#[inline]
	pub fn pop_peek(&mut self) {
		debug_assert_ne!(self.read, self.write);
		self.read = (self.read + 1) & ((SIZE as u8) - 1)
	}

	#[inline]
	pub fn source(&self) -> &'source str {
		self.lexer.source()
	}

	pub fn eof_span(&self) -> Span {
		let offset = self.lexer.source().len() as u32;
		Span {
			start: offset - 1,
			end: offset - 1,
		}
	}
}
