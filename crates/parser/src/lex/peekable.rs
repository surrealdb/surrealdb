use std::mem::MaybeUninit;

use ast::Span;
use logos::{Lexer, Logos};

use super::base::Joined;
use super::{BaseTokenKind, LexError};
use crate::lex::Token;

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
				self.peek[((self.read + OFFSET) & (SIZE as u8) - 1) as usize].assume_init()
			})
		}
	}

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
			Span::new(self.lexer.slice().len() as u32, self.lexer.slice().len() as u32)
		}
	}

	#[inline]
	pub fn next(&mut self) -> Option<Result<Token, LexError>> {
		if self.read == self.write {
			return self.lex_token();
		}
		let res = unsafe { self.peek[self.write as usize].assume_init() };
		self.write = (self.write + 1) & (SIZE as u8) - 1;
		Some(res)
	}

	pub fn is_empty(&self) -> bool {
		self.read == self.write
	}

	pub fn lexer(&mut self) -> &mut Lexer<'source, BaseTokenKind> {
		&mut self.lexer
	}

	pub fn pop_peek(&mut self) {
		debug_assert_ne!(self.read, self.write);
		self.write = (self.write + 1) % SIZE as u8
	}

	/// Slice the already lexed characters from the source.
	///
	/// # Panic
	/// Will panic if the span is outside the range of already lexed characters or the span
	/// boundaries do not align with character boundaries.
	pub fn slice(&self, span: Span) -> &'source str {
		let remaining = self.lexer.remainder().len();
		let source = self.lexer.source();
		let used = source.len() - remaining;
		let lexed_source = &source[..used];

		let slice = lexed_source
			.get((span.start as usize)..(span.end as usize))
			.expect("Tried to use a span from beyond the parsers parsed tokens");

		// ensure byte boundaries are correct.
		if slice.is_empty() {
			return "";
		}

		// If the character starts with bits 10 then it is a continuation byte and not a valid
		// character boundry.
		assert!(
			slice[0] & 0b1100_0000 != 0b1000_000,
			"tried to slice a string outside of a character boundry."
		);

		// If the character is the last character it is a valid utf8 boundry as the lexed_source
		// slice is guarnteed to be valid utf8 string, otherwise we need to check if the next
		// character is starting a byte boundry.
		assert!(
			lexed_source.len() == span.end as usize
				|| lexed_source[span.end as usize] & 0b1100_0000 != 0b1000_000,
			"tried to slice a string outside of a character boundry."
		);

		// Safety:
		// We checked boundry preconditions and since the lexed_source slice must be valid utf-8
		// this is valid utf8
		unsafe { str::from_utf8_unchecked(slice) }
	}
}
