//! The lexer.
//!
//! The lexer is a pure function of `(source, position)`: [`Lexer`] is two
//! words, copying it is a checkpoint, and lexing ahead from a copy is how the
//! parser peeks. Nothing is buffered and nothing is allocated.
//!
//! SurrealQL cannot be tokenised ahead of parsing. A `/` is division or the
//! start of a regex, `1` in `a:1abc` is the start of a record-id key, and
//! `1h30m` is one duration. The base lexer therefore emits small tokens
//! (`Digits` is only the run of digits) and the parser re-lexes from a token's
//! start with a *compound* lexer when the position calls for one:
//! [`Lexer::numeric`], [`Lexer::regex`], [`Lexer::record_key_ident`]. Each one
//! validates its syntax and returns a span; values are decoded from the span
//! when the tree is lowered.

use crate::kw::{self, Kw};

/// A token kind.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum T {
	Eof,
	/// A byte sequence no token starts with.
	Invalid,
	/// An identifier. Its keyword, if it spells one, is in [`Tok::kw`].
	Ident,
	/// `NaN`, spelled exactly so: a number, not an identifier.
	NaN,
	/// `Infinity`, spelled exactly so.
	Infinity,
	/// A `` `quoted` `` or `⟨quoted⟩` identifier.
	EscIdent,
	/// `$name`.
	Param,
	/// A run of ASCII digits; see [`Lexer::numeric`].
	Digits,
	/// `'…'` or `"…"`; [`Tok::aux`] holds the prefix byte (`0`, `r`, `u`, `d`,
	/// `b` or `f`, or one of the `PREFIX_` codes for `date`, `time`, `timestamp`
	/// and `interval`), with `0` also for an `s` string, which is a plain one.
	Str,
	/// A regex, produced only by [`Lexer::regex`].
	Regex,
	LParen,
	RParen,
	LBracket,
	RBracket,
	LBrace,
	RBrace,
	Comma,
	Semi,
	Colon,
	PathSep,
	Dot,
	DotDot,
	DotDotDot,
	Arrow,
	Lt,
	Gt,
	Le,
	Ge,
	Eq,
	EqEq,
	Ne,
	Plus,
	Minus,
	Star,
	StarStar,
	Slash,
	Percent,
	Bang,
	Question,
	QQ,
	QColon,
	AnyEq,
	AllEq,
	Tilde,
	NotLike,
	AnyLike,
	AllLike,
	Pipe,
	OrOr,
	AndAnd,
	At,
	Dollar,
	KnnOpen,
	PlusEq,
	MinusEq,
	PlusQEq,
	Times,
	Divide,
	Contains,
	NotContains,
	ContainsAll,
	ContainsAny,
	ContainsNone,
	Inside,
	NotInside,
	AllInside,
	AnyInside,
	NoneInside,
}

/// [`Tok::aux`] of a `date'…'` string.
pub const PREFIX_DATE: u8 = b'D';
/// [`Tok::aux`] of a `time'…'` string.
pub const PREFIX_TIME: u8 = b'T';
/// [`Tok::aux`] of a `timestamp'…'` string.
pub const PREFIX_TIMESTAMP: u8 = b'M';
/// [`Tok::aux`] of an `interval'…'` string.
pub const PREFIX_INTERVAL: u8 = b'I';

/// The [`Tok::aux`] of the multi-letter string prefix `word`, or `None` if it is not one.
fn long_prefix(word: &[u8]) -> Option<u8> {
	match word {
		b"pd" => Some(PREFIX_DATE),
		b"pt" => Some(PREFIX_TIME),
		b"ts" => Some(PREFIX_TIMESTAMP),
		b"ivl" => Some(PREFIX_INTERVAL),
		_ => None,
	}
}

/// The length of the string prefix `text` starts with, as the letters before its opening quote.
pub fn string_prefix_len(text: &str) -> usize {
	text.bytes().take_while(u8::is_ascii_lowercase).count()
}

/// A token: 12 bytes, no payload. Values are read back from the span.
#[derive(Clone, Copy, Debug)]
pub struct Tok {
	pub kind: T,
	/// Bit 7: whitespace or a comment separates this token from the previous
	/// one (record ids, ranges and graph arrows are whitespace sensitive).
	/// Bits 0–6: per-kind data, the prefix byte of a `Str` and 1 for an
	/// escaped `Param`.
	pub bits: u8,
	/// The keyword an `Ident` spells.
	pub kw: Kw,
	pub start: u32,
	pub end: u32,
}

const WS: u8 = 0x80;

const _: () = assert!(core::mem::size_of::<Tok>() == 12);

impl Tok {
	/// Whether whitespace or a comment precedes the token.
	#[inline]
	pub fn ws(&self) -> bool {
		self.bits & WS != 0
	}
	/// The per-kind data byte.
	#[inline]
	pub fn aux(&self) -> u8 {
		self.bits & !WS
	}
	pub fn is(&self, kind: T) -> bool {
		self.kind == kind
	}
	/// Whether this token is an identifier spelling `kw`.
	pub fn is_kw(&self, kw: Kw) -> bool {
		self.kind == T::Ident && self.kw == kw
	}
	/// Whether this token can be used where an identifier is expected.
	pub fn is_ident(&self) -> bool {
		self.kind == T::Ident || self.kind == T::EscIdent
	}
}

#[derive(Clone, Copy)]
pub struct Lexer<'a> {
	pub src: &'a [u8],
	pub pos: u32,
}

#[inline]
fn is_ident_start(b: u8) -> bool {
	b.is_ascii_alphabetic() || b == b'_'
}
#[inline]
fn is_ident_cont(b: u8) -> bool {
	b.is_ascii_alphanumeric() || b == b'_'
}

/// Error codes the lexer reports through `T::Invalid` tokens and compound
/// results. The parser maps them to static messages.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LexError {
	UnexpectedChar,
	UnterminatedString,
	UnterminatedComment,
	UnterminatedIdent,
	InvalidNumber,
	InvalidDuration,
	InvalidRegex,
	IntOverflow,
	UnterminatedScript,
}

/// What a numeric literal turned out to be.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NumKind {
	Int,
	Float,
	Decimal,
	/// A `bigdec` suffix: an arbitrary-precision decimal.
	BigDecimal,
	Duration,
	I8,
	U8,
	I16,
	U16,
	I32,
	U32,
	I64,
	U64,
	F16,
	F32,
	F64,
}

impl<'a> Lexer<'a> {
	pub fn new(src: &'a [u8]) -> Self {
		Lexer {
			src,
			pos: 0,
		}
	}

	#[inline]
	fn peek_at(&self, off: u32) -> u8 {
		self.src.get((self.pos + off) as usize).copied().unwrap_or(0)
	}

	#[inline]
	fn at_end(&self) -> bool {
		self.pos as usize >= self.src.len()
	}

	/// Skips whitespace and comments. Returns whether anything was skipped,
	/// or the start of an unterminated block comment.
	fn skip_trivia(&mut self) -> Result<bool, u32> {
		let start = self.pos;
		loop {
			match self.peek_at(0) {
				b' ' | b'\t' | b'\n' | b'\r' | 0x0B | 0x0C => self.pos += 1,
				b'#' => self.skip_line(),
				b'-' if self.peek_at(1) == b'-' => self.skip_line(),
				b'/' if self.peek_at(1) == b'/' => self.skip_line(),
				b'/' if self.peek_at(1) == b'*' => {
					let comment = self.pos;
					self.pos += 2;
					loop {
						if self.at_end() {
							return Err(comment);
						}
						if self.peek_at(0) == b'*' && self.peek_at(1) == b'/' {
							self.pos += 2;
							break;
						}
						self.pos += 1;
					}
				}
				// U+00A0 no-break space.
				0xC2 if self.peek_at(1) == 0xA0 => self.pos += 2,
				_ => break,
			}
		}
		Ok(self.pos != start)
	}

	fn skip_line(&mut self) {
		while !self.at_end() && self.peek_at(0) != b'\n' {
			self.pos += 1;
		}
	}

	#[inline]
	fn tok(&self, kind: T, ws: bool, start: u32) -> Tok {
		Tok {
			kind,
			bits: if ws {
				WS
			} else {
				0
			},
			kw: Kw::None,
			start,
			end: self.pos,
		}
	}

	/// Lexes the next token.
	pub fn next_token(&mut self) -> Tok {
		let ws = match self.skip_trivia() {
			Ok(ws) => ws,
			Err(comment) => {
				self.pos = self.src.len() as u32;
				return self.tok(T::Invalid, true, comment);
			}
		};
		let start = self.pos;
		let Some(&b) = self.src.get(start as usize) else {
			return self.tok(T::Eof, ws, start);
		};
		self.pos += 1;
		let kind = match b {
			b'(' => T::LParen,
			b')' => T::RParen,
			b'[' => T::LBracket,
			b']' => T::RBracket,
			b'{' => T::LBrace,
			b'}' => T::RBrace,
			b',' => T::Comma,
			b';' => T::Semi,
			b'@' => T::At,
			b'%' => T::Percent,
			b'~' => T::Tilde,
			b'/' => T::Slash,
			b':' => self.pick(b':', T::PathSep, T::Colon),
			b'.' => {
				if self.peek_at(0) == b'.' {
					self.pos += 1;
					self.pick(b'.', T::DotDotDot, T::DotDot)
				} else {
					T::Dot
				}
			}
			b'-' => match self.peek_at(0) {
				b'>' => self.bump(T::Arrow),
				b'=' => self.bump(T::MinusEq),
				_ => T::Minus,
			},
			b'+' => match (self.peek_at(0), self.peek_at(1)) {
				(b'=', _) => self.bump(T::PlusEq),
				(b'?', b'=') => {
					self.pos += 2;
					T::PlusQEq
				}
				// `+?` without `=` is one invalid token, as it has always lexed.
				(b'?', _) => {
					self.pos += 1;
					T::Invalid
				}
				_ => T::Plus,
			},
			b'*' => match self.peek_at(0) {
				b'*' => self.bump(T::StarStar),
				b'=' => self.bump(T::AllEq),
				b'~' => self.bump(T::AllLike),
				_ => T::Star,
			},
			b'!' => match self.peek_at(0) {
				b'=' => self.bump(T::Ne),
				b'~' => self.bump(T::NotLike),
				_ => T::Bang,
			},
			b'?' => match self.peek_at(0) {
				b'?' => self.bump(T::QQ),
				b':' => self.bump(T::QColon),
				b'=' => self.bump(T::AnyEq),
				b'~' => self.bump(T::AnyLike),
				_ => T::Question,
			},
			b'<' => match self.peek_at(0) {
				b'=' => self.bump(T::Le),
				b'|' => self.bump(T::KnnOpen),
				_ => T::Lt,
			},
			b'>' => self.pick(b'=', T::Ge, T::Gt),
			b'=' => self.pick(b'=', T::EqEq, T::Eq),
			b'|' => self.pick(b'|', T::OrOr, T::Pipe),
			b'&' => {
				if self.peek_at(0) == b'&' {
					self.bump(T::AndAnd)
				} else {
					T::Invalid
				}
			}
			b'$' => {
				if self.peek_at(0) == b'`' {
					self.pos += 1;
					let mut t = self.delimited_ident(ws, start, |l| {
						if l.peek_at(0) == b'`' {
							l.pos += 1;
							true
						} else {
							false
						}
					});
					if t.kind == T::EscIdent {
						t.kind = T::Param;
						t.bits |= 1;
					}
					return t;
				}
				if self.peek_at(0) == 0xE2 && self.peek_at(1) == 0x9F && self.peek_at(2) == 0xA8 {
					// `$⟨name⟩`
					self.pos += 3;
					let mut t = self.delimited_ident(ws, start, |l| {
						if l.peek_at(0) == 0xE2 && l.peek_at(1) == 0x9F && l.peek_at(2) == 0xA9 {
							l.pos += 3;
							true
						} else {
							false
						}
					});
					if t.kind == T::EscIdent {
						t.kind = T::Param;
						t.bits |= 1;
					}
					return t;
				}
				// A parameter name starts with a letter or `_`.
				if is_ident_start(self.peek_at(0)) {
					while is_ident_cont(self.peek_at(0)) {
						self.pos += 1;
					}
					T::Param
				} else {
					T::Dollar
				}
			}
			b'0'..=b'9' => {
				while self.peek_at(0).is_ascii_digit() {
					self.pos += 1;
				}
				T::Digits
			}
			b'\'' | b'"' => return self.string(ws, start, b, 0),
			b'`' => {
				return self.delimited_ident(ws, start, |l| {
					if l.peek_at(0) == b'`' {
						l.pos += 1;
						true
					} else {
						false
					}
				});
			}
			b if is_ident_start(b) => {
				let q = self.peek_at(0);
				if (q == b'\'' || q == b'"') && matches!(b, b'r' | b'u' | b'd' | b'b' | b'f' | b's')
				{
					self.pos += 1;
					// An `s` string is a plain string.
					let prefix = if b == b's' {
						0
					} else {
						b
					};
					return self.string(ws, start, q, prefix);
				}
				while is_ident_cont(self.peek_at(0)) {
					self.pos += 1;
				}
				let text = &self.src[start as usize..self.pos as usize];
				let q = self.peek_at(0);
				if (q == b'\'' || q == b'"')
					&& let Some(prefix) = long_prefix(text)
				{
					self.pos += 1;
					return self.string(ws, start, q, prefix);
				}
				let kw = kw::lookup(text);
				let kind = match text {
					b"NaN" => T::NaN,
					b"Infinity" => T::Infinity,
					_ => T::Ident,
				};
				let mut t = self.tok(kind, ws, start);
				t.kw = kw;
				return t;
			}
			0x80.. => self.unicode(b),
			_ => T::Invalid,
		};
		self.tok(kind, ws, start)
	}

	#[inline]
	fn bump(&mut self, kind: T) -> T {
		self.pos += 1;
		kind
	}

	#[inline]
	fn pick(&mut self, next: u8, yes: T, no: T) -> T {
		if self.peek_at(0) == next {
			self.pos += 1;
			yes
		} else {
			no
		}
	}

	/// Multi-byte operators. `b` is the already consumed lead byte.
	fn unicode(&mut self, b: u8) -> T {
		let c1 = self.peek_at(0);
		let c2 = self.peek_at(1);
		let (len, kind) = match (b, c1, c2) {
			(0xC3, 0x97, _) => (1, T::Times),           // ×
			(0xC3, 0xB7, _) => (1, T::Divide),          // ÷
			(0xE2, 0x88, 0x99) => (2, T::Times),        // ∙
			(0xE2, 0x88, 0x8B) => (2, T::Contains),     // ∋
			(0xE2, 0x88, 0x8C) => (2, T::NotContains),  // ∌
			(0xE2, 0x88, 0x88) => (2, T::Inside),       // ∈
			(0xE2, 0x88, 0x89) => (2, T::NotInside),    // ∉
			(0xE2, 0x8A, 0x87) => (2, T::ContainsAll),  // ⊇
			(0xE2, 0x8A, 0x83) => (2, T::ContainsAny),  // ⊃
			(0xE2, 0x8A, 0x85) => (2, T::ContainsNone), // ⊅
			(0xE2, 0x8A, 0x86) => (2, T::AllInside),    // ⊆
			(0xE2, 0x8A, 0x82) => (2, T::AnyInside),    // ⊂
			(0xE2, 0x8A, 0x84) => (2, T::NoneInside),   // ⊄
			(0xE2, 0x80, 0xA6) => (2, T::DotDotDot),    // …
			(0xE2, 0x9F, 0xA8) => {
				// ⟨ident⟩
				self.pos -= 1;
				let start = self.pos;
				self.pos += 3;
				let t = self.delimited_ident(false, start, |l| {
					if l.peek_at(0) == 0xE2 && l.peek_at(1) == 0x9F && l.peek_at(2) == 0xA9 {
						l.pos += 3;
						true
					} else {
						false
					}
				});
				// `delimited_ident` set the end; the caller rebuilds the token
				// from `self.pos`, which is where it stopped.
				return t.kind;
			}
			_ => {
				// The token covers the whole character, so it can be sliced.
				while (0x80..0xC0).contains(&self.peek_at(0)) {
					self.pos += 1;
				}
				return T::Invalid;
			}
		};
		self.pos += len;
		kind
	}

	fn delimited_ident(&mut self, ws: bool, start: u32, close: impl Fn(&mut Self) -> bool) -> Tok {
		loop {
			if self.at_end() {
				return self.tok(T::Invalid, ws, start);
			}
			if close(self) {
				return self.tok(T::EscIdent, ws, start);
			}
			if self.peek_at(0) == b'\\' {
				self.pos += 1;
			}
			self.pos = (self.pos + 1).min(self.src.len() as u32);
		}
	}

	fn string(&mut self, ws: bool, start: u32, quote: u8, prefix: u8) -> Tok {
		loop {
			match self.peek_at(0) {
				_ if self.at_end() => return self.tok(T::Invalid, ws, start),
				b'\\' => self.pos = (self.pos + 2).min(self.src.len() as u32),
				b if b == quote => {
					self.pos += 1;
					let mut t = self.tok(T::Str, ws, start);
					t.bits |= prefix;
					return t;
				}
				_ => self.pos += 1,
			}
		}
	}

	/// Lexes a numeric literal starting at `start` (a `Digits` token, possibly
	/// preceded by a sign the caller already consumed and passes as `start`).
	/// Returns its kind and moves the lexer past it.
	///
	/// Grammar: `digits [. digits] [(e|E) [+-] digits] [f | dec]`, or a
	/// duration `(digits unit)+`, where a letter directly after the literal is
	/// an error.
	pub fn numeric(&mut self, start: u32) -> Result<NumKind, LexError> {
		self.pos = start;
		if matches!(self.peek_at(0), b'+' | b'-') {
			self.pos += 1;
		}
		self.digits();
		// A character whose UTF-8 starts like `µ`'s directly after the digits
		// starts a duration whose unit is not `µs`, as earlier releases read it; any
		// other character outside ASCII ends the number.
		if self.src[start as usize].is_ascii_digit()
			&& self.peek_at(0) == 0xC2
			&& self.duration_unit_len() == 0
		{
			return Err(LexError::InvalidDuration);
		}
		// Duration: a unit directly after the first digit run.
		if self.duration_unit_len() > 0 && !self.exponent_ahead() {
			loop {
				let n = self.duration_unit_len();
				if n == 0 {
					return Err(LexError::InvalidDuration);
				}
				self.pos += n;
				if !self.peek_at(0).is_ascii_digit() {
					break;
				}
				self.digits();
			}
			if self.peek_at(0).is_ascii_alphabetic() {
				return Err(LexError::InvalidDuration);
			}
			return Ok(NumKind::Duration);
		}
		let mut kind = NumKind::Int;
		let mut is_int = true;
		if self.peek_at(0) == b'.' && self.peek_at(1).is_ascii_digit() {
			self.pos += 1;
			self.digits();
			kind = NumKind::Float;
			is_int = false;
		}
		if matches!(self.peek_at(0), b'e' | b'E') {
			let sign = matches!(self.peek_at(1), b'+' | b'-') as u32;
			if self.peek_at(1 + sign).is_ascii_digit() {
				self.pos += 1 + sign;
				self.digits();
				kind = NumKind::Float;
				is_int = false;
			}
		}
		let b0 = self.peek_at(0);
		let b1 = self.peek_at(1);
		let b2 = self.peek_at(2);
		if b0 == b'd' && b1 == b'e' && b2 == b'c' {
			self.pos += 3;
			kind = NumKind::Decimal;
		} else if b0 == b'b'
			&& b1 == b'i'
			&& b2 == b'g'
			&& self.peek_at(3) == b'd'
			&& self.peek_at(4) == b'e'
			&& self.peek_at(5) == b'c'
		{
			self.pos += 6;
			kind = NumKind::BigDecimal;
		} else if b0 == b'f' && b1 == b'1' && b2 == b'6' {
			self.pos += 3;
			kind = NumKind::F16;
		} else if b0 == b'f' && b1 == b'3' && b2 == b'2' {
			self.pos += 3;
			kind = NumKind::F32;
		} else if b0 == b'f' && b1 == b'6' && b2 == b'4' {
			self.pos += 3;
			kind = NumKind::F64;
		} else if b0 == b'i' && b1 == b'1' && b2 == b'6' {
			if !is_int {
				return Err(LexError::InvalidNumber);
			}
			self.pos += 3;
			kind = NumKind::I16;
		} else if b0 == b'u' && b1 == b'1' && b2 == b'6' {
			if !is_int {
				return Err(LexError::InvalidNumber);
			}
			self.pos += 3;
			kind = NumKind::U16;
		} else if b0 == b'i' && b1 == b'3' && b2 == b'2' {
			if !is_int {
				return Err(LexError::InvalidNumber);
			}
			self.pos += 3;
			kind = NumKind::I32;
		} else if b0 == b'u' && b1 == b'3' && b2 == b'2' {
			if !is_int {
				return Err(LexError::InvalidNumber);
			}
			self.pos += 3;
			kind = NumKind::U32;
		} else if b0 == b'i' && b1 == b'6' && b2 == b'4' {
			if !is_int {
				return Err(LexError::InvalidNumber);
			}
			self.pos += 3;
			kind = NumKind::I64;
		} else if b0 == b'u' && b1 == b'6' && b2 == b'4' {
			if !is_int {
				return Err(LexError::InvalidNumber);
			}
			self.pos += 3;
			kind = NumKind::U64;
		} else if b0 == b'i' && b1 == b'8' {
			if !is_int {
				return Err(LexError::InvalidNumber);
			}
			self.pos += 2;
			kind = NumKind::I8;
		} else if b0 == b'u' && b1 == b'8' {
			if !is_int {
				return Err(LexError::InvalidNumber);
			}
			self.pos += 2;
			kind = NumKind::U8;
		} else if b0 == b'f' {
			self.pos += 1;
			kind = NumKind::Float;
		}
		// Only a letter ends a literal in error: digits and `_` after a
		// suffix start the next token.
		if self.peek_at(0).is_ascii_alphabetic() {
			return Err(LexError::InvalidNumber);
		}
		Ok(kind)
	}

	fn exponent_ahead(&self) -> bool {
		// `1e5` is a float and `1dec` a decimal, not durations in `d`.
		self.peek_at(0) == b'd' && self.peek_at(1) == b'e'
	}

	fn digits(&mut self) {
		while self.peek_at(0).is_ascii_digit() || self.peek_at(0) == b'_' {
			self.pos += 1;
		}
	}

	/// The byte length of a duration unit at the cursor, or 0.
	fn duration_unit_len(&self) -> u32 {
		let (a, b) = (self.peek_at(0), self.peek_at(1));
		let len = match (a, b) {
			(b'n', b's') | (b'u', b's') | (b'm', b's') => 2,
			(0xC2, 0xB5) if self.peek_at(2) == b's' => 3, // µs
			(b's' | b'm' | b'h' | b'd' | b'w' | b'y', _) => 1,
			_ => return 0,
		};
		if len == 1 && a == b'm' && b == b's' {
			return 2;
		}
		len
	}

	/// Lexes a regex starting at the `/` at `start`.
	pub fn regex(&mut self, start: u32) -> Result<(), LexError> {
		self.pos = start + 1;
		loop {
			match self.peek_at(0) {
				_ if self.at_end() => return Err(LexError::InvalidRegex),
				b'\\' => self.pos = (self.pos + 2).min(self.src.len() as u32),
				b'/' => {
					self.pos += 1;
					return Ok(());
				}
				_ => self.pos += 1,
			}
		}
	}

	/// Lexes a JavaScript function body from its `{` at `start`, returning
	/// the offset after the matching `}`. Strings, regex literals, comments
	/// and nested braces are skipped as JavaScript defines them.
	///
	/// A `/` starts a regex literal only where an expression may begin, so the
	/// scan tracks whether the previous token ends an expression. A `}` ends
	/// one when it closes an object literal or the body of a function or class
	/// expression, and does not when it closes a statement block, so each
	/// nested `{` records which it opened. A `{` where a statement may begin
	/// (after `;`, `)`, `=>`, a block or an identifier, and after `else` or
	/// `do`) opens a block, unless it is the body of a `function` or `class`
	/// that stood where an expression is expected; a `{` anywhere else opens an
	/// object literal.
	pub fn js_body(&mut self, start: u32) -> Result<u32, LexError> {
		self.pos = start + 1;
		// Whether each `{` nested in the body opened a statement block, after
		// whose `}` a statement may begin, innermost last.
		let mut blocks: Vec<bool> = Vec::new();
		let mut regex_allowed = true;
		let mut block_start = true;
		// A `function` or `class` stood where an expression is expected, so
		// the next `{` where a statement may begin is its body, and closing that
		// body ends the expression.
		let mut expression_body = false;
		loop {
			if self.at_end() {
				return Err(LexError::UnterminatedScript);
			}
			let b = self.peek_at(0);
			self.pos += 1;
			match b {
				b' ' | b'\t' | b'\n' | b'\r' | 0x0B | 0x0C => {}
				b'`' | b'\'' | b'"' => {
					loop {
						if self.at_end() {
							return Err(LexError::UnterminatedScript);
						}
						let c = self.peek_at(0);
						self.pos += 1;
						if c == b {
							break;
						}
						if c == b'\\' {
							self.pos = (self.pos + 1).min(self.src.len() as u32);
						}
					}
					regex_allowed = false;
					block_start = false;
				}
				b'/' if self.peek_at(0) == b'/' => {
					// To the end of the line: `\n`, `\r`, U+2028 or U+2029.
					while !self.at_end() {
						let c = self.peek_at(0);
						if c == b'\n' || c == b'\r' {
							self.pos += 1;
							break;
						}
						if c == 0xE2
							&& self.peek_at(1) == 0x80
							&& matches!(self.peek_at(2), 0xA8 | 0xA9)
						{
							self.pos += 3;
							break;
						}
						self.pos += 1;
					}
				}
				b'/' if self.peek_at(0) == b'*' => {
					self.pos += 1;
					loop {
						if self.at_end() {
							return Err(LexError::UnterminatedScript);
						}
						if self.peek_at(0) == b'*' && self.peek_at(1) == b'/' {
							self.pos += 2;
							break;
						}
						self.pos += 1;
					}
				}
				b'/' if regex_allowed => {
					let mut in_class = false;
					loop {
						if self.at_end() {
							return Err(LexError::UnterminatedScript);
						}
						let c = self.peek_at(0);
						if c == b'\n' || c == b'\r' {
							return Err(LexError::UnterminatedScript);
						}
						self.pos += 1;
						match c {
							b'\\' => {
								if self.at_end() || matches!(self.peek_at(0), b'\n' | b'\r') {
									return Err(LexError::UnterminatedScript);
								}
								self.pos += 1;
							}
							b'[' => in_class = true,
							b']' => in_class = false,
							b'/' if !in_class => break,
							_ => {}
						}
					}
					while self.peek_at(0).is_ascii_alphabetic() {
						self.pos += 1;
					}
					regex_allowed = false;
					block_start = false;
				}
				b'{' => {
					let body = block_start && expression_body;
					if body {
						expression_body = false;
					}
					blocks.push(block_start && !body);
					regex_allowed = true;
					block_start = true;
				}
				b'}' => {
					let Some(block) = blocks.pop() else {
						return Ok(self.pos);
					};
					regex_allowed = block;
					block_start = block;
				}
				b')' => {
					regex_allowed = false;
					block_start = true;
				}
				b']' => {
					regex_allowed = false;
					block_start = false;
				}
				b'+' | b'-' if self.peek_at(0) == b => {
					self.pos += 1;
				}
				b'0'..=b'9' => {
					while self.peek_at(0).is_ascii_alphanumeric()
						|| matches!(self.peek_at(0), b'_' | b'.')
					{
						self.pos += 1;
					}
					regex_allowed = false;
					block_start = false;
				}
				b'a'..=b'z' | b'A'..=b'Z' | b'_' | b'$' => {
					let word_start = (self.pos - 1) as usize;
					while self.peek_at(0).is_ascii_alphanumeric()
						|| matches!(self.peek_at(0), b'_' | b'$')
					{
						self.pos += 1;
					}
					let word = &self.src[word_start..self.pos as usize];
					if matches!(word, b"function" | b"class") && !block_start {
						expression_body = true;
					}
					// `async` leaves the position as it found it, so the
					// `function` after it is read where the `async` stood.
					if word == b"async" {
						regex_allowed = false;
						continue;
					}
					regex_allowed = matches!(
						word,
						b"return"
							| b"throw" | b"case" | b"delete"
							| b"void" | b"typeof" | b"instanceof"
							| b"in" | b"of" | b"new"
							| b"yield" | b"await" | b"else"
							| b"do"
					);
					block_start = !regex_allowed || matches!(word, b"else" | b"do");
				}
				_ => {
					regex_allowed = true;
					block_start =
						b == b';' || (b == b'>' && self.src[self.pos as usize - 2] == b'=');
				}
			}
		}
	}

	/// Lexes a flexible record-id key identifier at `start`: identifier
	/// characters, which may begin with digits (`person:1abc`).
	pub fn record_key_ident(&mut self, start: u32) -> u32 {
		self.pos = start;
		while is_ident_cont(self.peek_at(0)) {
			self.pos += 1;
		}
		self.pos
	}
}

/// Which compound number grammar reads a literal, which decides how a
/// malformed one is reported.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Compound {
	/// A number or a duration (`numeric`).
	Numeric,
	/// A number, after a sign or as an object key (`number`).
	Number,
	/// An integer (`integer`).
	Integer,
	/// A float (`float`).
	Float,
	/// A duration (`duration`).
	Duration,
}

/// The established error for the malformed literal at `start` read as
/// `mode`, or `None` when the literal is well formed. `start` is where the
/// compound token starts: the sign for a signed literal lexed whole, the
/// digits otherwise.
pub fn compound_error(src: &[u8], start: u32, mode: Compound) -> Option<crate::parse::Diag> {
	use crate::parse::Diag;
	let at = |i: u32| src.get(i as usize).copied();
	let len = src.len() as u32;
	let char_len = |i: u32| match at(i).unwrap_or(0) {
		0..0x80 => 1,
		0xC0..0xE0 => 2,
		0xE0..0xF0 => 3,
		_ => 4,
	};
	let mut pos = start;
	let digits = |pos: &mut u32| {
		while at(*pos).is_some_and(|b| b.is_ascii_digit() || b == b'_') {
			*pos += 1;
		}
	};
	// `eat_digits1`: at least one digit, from `from`.
	let digits1 = |pos: &mut u32, from: u32| -> Option<Diag> {
		match at(*pos) {
			Some(b) if b.is_ascii_digit() => {
				digits(pos);
				None
			}
			Some(_) => Some(Diag::message(
				from,
				*pos,
				"Invalid number token, expected a digit, found: {next}",
			)),
			None => Some(Diag::message(
				from,
				*pos,
				"Unexpected end of file, expected a number token digit",
			)),
		}
	};
	// `Lexer::expect`.
	let expect = |pos: &mut u32, c: u8| -> Option<Diag> {
		match at(*pos) {
			Some(b) if b == c => {
				*pos += 1;
				None
			}
			Some(_) => {
				let l = char_len(*pos);
				let msg = match c {
					b'e' => "Unexpected character `{char}` expected `e`",
					b'c' => "Unexpected character `{char}` expected `c`",
					_ => "Unexpected character `{char}` expected `s`",
				};
				Some(Diag::message(*pos, *pos + l, msg))
			}
			None => {
				let msg = match c {
					b'e' => "Unexpected end of file, expected character `e`",
					b'c' => "Unexpected end of file, expected character `c`",
					_ => "Unexpected end of file, expected character `s`",
				};
				Some(Diag::message(start, *pos, msg))
			}
		}
	};
	let signed = matches!(at(start), Some(b'+' | b'-'));
	if signed {
		pos += 1;
		if at(pos) == Some(b'I') {
			// `-Infinity` and `+Infinity`, spelled exactly so.
			const MISSING: [&str; 7] = [
				"Invalid number token, expected `n` found: {last_byte}",
				"Invalid number token, expected `f` found: {last_byte}",
				"Invalid number token, expected `i` found: {last_byte}",
				"Invalid number token, expected `n` found: {last_byte}",
				"Invalid number token, expected `i` found: {last_byte}",
				"Invalid number token, expected `t` found: {last_byte}",
				"Invalid number token, expected `y` found: {last_byte}",
			];
			pos += 1;
			for (i, &c) in b"nfinity".iter().enumerate() {
				match at(pos) {
					Some(x) if x == c => pos += 1,
					Some(_) => return Some(Diag::message(start, pos + 1, MISSING[i])),
					None => {
						return Some(Diag::message(
							start,
							pos,
							"Unexpected end of file, expected a number token digit",
						));
					}
				}
			}
			return None;
		}
	} else {
		digits(&mut pos);
	}
	let is_duration = mode == Compound::Duration
		|| (mode == Compound::Numeric
			&& !signed
			&& match at(pos) {
				Some(b'n' | b's' | b'm' | b'h' | b'y' | b'w' | 0xC2) => true,
				Some(b'u') => !matches!(
					(at(pos + 1), at(pos + 2)),
					(Some(b'8'), _)
						| (Some(b'1'), Some(b'6'))
						| (Some(b'3'), Some(b'2'))
						| (Some(b'6'), Some(b'4'))
				),
				Some(b'd') => at(pos + 1) != Some(b'e'),
				_ => false,
			});
	if is_duration {
		let mut total: u128 = 0;
		let mut number_start = start;
		loop {
			let digits_text = &src[number_start as usize..pos as usize];
			let value = {
				let mut v: u128 = 0;
				for &b in digits_text {
					if b != b'_' {
						// Saturates: anything past `u64::MAX` is reported below.
						v = v.saturating_mul(10).saturating_add((b - b'0') as u128);
					}
				}
				v
			};
			let unit: u128 = match at(pos) {
				Some(b'n') => {
					pos += 1;
					if let Some(d) = expect(&mut pos, b's') {
						return Some(d);
					}
					1
				}
				Some(b'u') => {
					pos += 1;
					if let Some(d) = expect(&mut pos, b's') {
						return Some(d);
					}
					1_000
				}
				Some(b'm') => {
					pos += 1;
					if at(pos) == Some(b's') {
						pos += 1;
						1_000_000
					} else {
						60_000_000_000
					}
				}
				Some(b's') => {
					pos += 1;
					1_000_000_000
				}
				Some(b'h') => {
					pos += 1;
					3_600_000_000_000
				}
				Some(b'd') => {
					pos += 1;
					86_400_000_000_000
				}
				Some(b'w') => {
					pos += 1;
					604_800_000_000_000
				}
				Some(b'y') => {
					pos += 1;
					31_536_000_000_000_000
				}
				Some(0xC2) => {
					pos += 1;
					if at(pos) != Some(0xB5) {
						return Some(Diag::message(
							start,
							pos - 1 + char_len(pos - 1),
							"Invalid duration token, expected a duration suffix found `{last}`",
						));
					}
					pos += 1;
					if let Some(d) = expect(&mut pos, b's') {
						return Some(d);
					}
					1_000
				}
				Some(_) => {
					let end = pos + char_len(pos);
					return Some(Diag::message(
						start,
						end.min(len),
						"Invalid duration token, expected a duration suffix found `{last}`",
					));
				}
				None => {
					return Some(Diag::message(
						start,
						pos,
						"Unexpected end of file, expected a duration suffix",
					));
				}
			};
			if at(pos).is_some_and(|b| b.is_ascii_alphabetic()) {
				return Some(Diag::message(
					start,
					pos + 1,
					"Invalid token, found invalid character `{last}` after duration suffix",
				));
			}
			if value > u64::MAX as u128 {
				return Some(Diag::message(
					start,
					pos,
					"Invalid token, failed to parse duration digits: number too large to fit in target type",
				));
			}
			// Units of a minute and more are whole seconds, checked against
			// `u64` seconds as earlier releases check them.
			if unit >= 60_000_000_000 && value * (unit / 1_000_000_000) > u64::MAX as u128 {
				return Some(Diag::message(
					start,
					pos,
					"Invalid duration, value overflowed maximum allowed value",
				));
			}
			total += value * unit;
			if total / 1_000_000_000 > u64::MAX as u128 {
				return Some(Diag::message(
					start,
					pos,
					"Invalid duration, value overflowed maximum allowed value",
				));
			}
			if at(pos).is_some_and(|b| b.is_ascii_digit()) {
				number_start = pos;
				digits(&mut pos);
			} else {
				return None;
			}
		}
	}
	if signed && let Some(d) = digits1(&mut pos, start) {
		return Some(d);
	}
	if mode == Compound::Integer {
		if at(pos).is_some_and(|b| b.is_ascii_alphabetic()) {
			return Some(Diag::message(
				start,
				pos + 1,
				"Invalid token, found unexpected character `{last} after integer token",
			));
		}
		if at(pos) == Some(b'.') && at(pos + 1).is_some_and(|b| b.is_ascii_digit()) {
			return Some(Diag::message(
				pos,
				pos + 1,
				"Unexpected character `.` starting float, only integers are allowed here",
			));
		}
		return None;
	}
	let mut float = false;
	if mode == Compound::Float {
		if at(pos) == Some(b'.') {
			let before = pos;
			pos += 1;
			if let Some(d) = digits1(&mut pos, before) {
				return Some(d);
			}
		}
	} else if at(pos) == Some(b'.') && at(pos + 1).is_some_and(|b| b.is_ascii_digit()) {
		pos += 1;
		digits(&mut pos);
		float = true;
	}
	if matches!(at(pos), Some(b'e' | b'E')) {
		let before = pos;
		pos += 1;
		if at(pos) == Some(b'-') || at(pos) == Some(b'+') {
			pos += 1;
		}
		if let Some(d) = digits1(&mut pos, before) {
			return Some(d);
		}
		float = true;
	}
	if mode == Compound::Float {
		let b0 = at(pos);
		let b1 = at(pos + 1);
		let b2 = at(pos + 2);
		if b0 == Some(b'f') {
			if (b1 == Some(b'1') && b2 == Some(b'6'))
				|| (b1 == Some(b'3') && b2 == Some(b'2'))
				|| (b1 == Some(b'6') && b2 == Some(b'4'))
			{
				pos += 3;
			} else {
				pos += 1;
			}
		}
		if at(pos).is_some_and(|b| b.is_ascii_alphabetic()) {
			return Some(Diag::message(
				start,
				pos + 1,
				"Invalid token, found invalid character `{last}` after number token",
			));
		}
		return None;
	}
	let mut int_limit: Option<(u64, bool, &'static str)> = None;
	let b0 = at(pos);
	let b1 = at(pos + 1);
	let b2 = at(pos + 2);
	let digit_end = pos;
	if b0 == Some(b'f') {
		if (b1 == Some(b'1') && b2 == Some(b'6'))
			|| (b1 == Some(b'3') && b2 == Some(b'2'))
			|| (b1 == Some(b'6') && b2 == Some(b'4'))
		{
			pos += 3;
		} else {
			pos += 1;
		}
		float = true;
	} else if b0 == Some(b'd') {
		pos += 1;
		if let Some(d) = expect(&mut pos, b'e') {
			return Some(d);
		}
		if let Some(d) = expect(&mut pos, b'c') {
			return Some(d);
		}
		float = true;
	} else if !float && (b0 == Some(b'i') || b0 == Some(b'u')) {
		let is_i = b0 == Some(b'i');
		if b1 == Some(b'8') {
			pos += 2;
			int_limit = Some(if is_i {
				(
					i8::MAX as u64,
					true,
					"Failed to parse number: number cannot fit within an 8bit signed integer",
				)
			} else {
				(
					u8::MAX as u64,
					false,
					"Failed to parse number: number cannot fit within an 8bit unsigned integer",
				)
			});
		} else if b1 == Some(b'1') && b2 == Some(b'6') {
			pos += 3;
			int_limit = Some(if is_i {
				(
					i16::MAX as u64,
					true,
					"Failed to parse number: number cannot fit within a 16bit signed integer",
				)
			} else {
				(
					u16::MAX as u64,
					false,
					"Failed to parse number: number cannot fit within a 16bit unsigned integer",
				)
			});
		} else if b1 == Some(b'3') && b2 == Some(b'2') {
			pos += 3;
			int_limit = Some(if is_i {
				(
					i32::MAX as u64,
					true,
					"Failed to parse number: number cannot fit within a 32bit signed integer",
				)
			} else {
				(
					u32::MAX as u64,
					false,
					"Failed to parse number: number cannot fit within a 32bit unsigned integer",
				)
			});
		} else if b1 == Some(b'6') && b2 == Some(b'4') {
			pos += 3;
			int_limit = Some(if is_i {
				(
					i64::MAX as u64,
					true,
					"Failed to parse number: number cannot fit within a 64bit signed integer",
				)
			} else {
				(
					u64::MAX,
					false,
					"Failed to parse number: number cannot fit within a 64bit unsigned integer",
				)
			});
		}
	}
	if at(pos).is_some_and(|b| b.is_ascii_alphabetic()) {
		return Some(Diag::message(
			start,
			pos + 1,
			"Invalid token, found unexpected character `{last}` after number token",
		));
	}
	if !float {
		let (max, allow_neg, msg) = int_limit.unwrap_or((
			i64::MAX as u64,
			true,
			"Failed to parse number: number cannot fit within a 64bit signed integer",
		));
		let body = &src[(start + signed as u32) as usize..digit_end as usize];
		let neg = at(start) == Some(b'-');
		let fits = decode_uint(body).is_some_and(|u| {
			if allow_neg {
				u <= max + (neg as u64)
			} else {
				(!neg || u == 0) && u <= max
			}
		});
		if !fits {
			return Some(Diag::message(start, pos, msg));
		}
	}
	None
}

/// Why the lexer produced an invalid token at `start`, worded and spanned as
/// clients have always seen it.
pub fn invalid_reason(src: &[u8], start: u32) -> crate::parse::Diag {
	use crate::parse::Diag;
	let s = start as usize;
	let end = src.len() as u32;
	let at = |i: usize| src.get(s + i).copied().unwrap_or(0);
	match at(0) {
		b'/' if at(1) == b'*' => Diag::message(
			start,
			start + 2,
			"Unexpected end of file, expected multi-line comment to end.",
		)
		.with_label("Comment starts here."),
		b'\'' | b'"' => {
			Diag::message(start, start + 1, "Unexpected end of file, expected string to end.")
				.with_label("String starting here.")
		}
		b'r' | b'u' | b'd' | b'b' | b'f' | b's' if matches!(at(1), b'\'' | b'"') => {
			Diag::message(start, start + 2, "Unexpected end of file, expected string to end.")
				.with_label("String starting here.")
		}
		b'a'..=b'z'
			if {
				let n = src[s..].iter().take_while(|&&c| is_ident_cont(c)).count();
				matches!(src.get(s + n), Some(b'\'' | b'"'))
					&& long_prefix(&src[s..s + n]).is_some()
			} =>
		{
			let n = src[s..].iter().take_while(|&&c| is_ident_cont(c)).count() as u32;
			Diag::message(start, start + n + 1, "Unexpected end of file, expected string to end.")
				.with_label("String starting here.")
		}
		b'`' => unterminated_ident(src, start, 1, BACKTICK_END),
		b'$' if at(1) == b'`' => unterminated_ident(src, start, 2, BACKTICK_END),
		0xE2 if at(1) == 0x9F && at(2) == 0xA8 => unterminated_ident(src, start, 3, ANGLE_END),
		b'$' if at(1) == 0xE2 && at(2) == 0x9F && at(3) == 0xA8 => {
			unterminated_ident(src, start, 4, ANGLE_END)
		}
		b'&' => Diag::message(
			start,
			start + 1,
			"Invalid token `&`, single `&` are not a valid token, did you mean `&&`?",
		),
		b'+' if at(1) == b'?' => {
			Diag::message(start, start + 2, "Invalid token `+?` did you maybe mean `+?=` ?")
		}
		b => {
			let len = match b {
				0..0x80 => 1,
				0xC0..0xE0 => 2,
				0xE0..0xF0 => 3,
				_ => 4,
			};
			Diag::message(start, (start + len).min(end), "Invalid token `{char}`")
		}
	}
}

const BACKTICK_END: &str = "Unexpected end of file, expected identifier to end with ```";
const ANGLE_END: &str = "Unexpected end of file, expected identifier to end with `⟩`";

/// The established error for an escaped identifier at `start` that runs to the end
/// of `src`, whose opening (`$` included) is `open` bytes long. An escape cut
/// off by the end is reported at the opening; otherwise the whole identifier
/// is, with the message `closer` naming the delimiter it lacks.
fn unterminated_ident(
	src: &[u8],
	start: u32,
	open: u32,
	closer: &'static str,
) -> crate::parse::Diag {
	let body = src.get((start + open) as usize..).unwrap_or_default();
	let mut i = 0;
	while i < body.len() {
		if body[i] == b'\\' && i + 1 == body.len() {
			return crate::parse::Diag::message(
				start,
				start + open,
				"Unexpected end of file, expected identifier to end.",
			)
			.with_label("Identifier starting here.");
		}
		i += if body[i] == b'\\' {
			2
		} else {
			1
		};
	}
	crate::parse::Diag::message(start, src.len() as u32, closer)
}

/// Decodes a lexed integer span. Underscores are separators.
pub fn decode_uint(text: &[u8]) -> Option<u64> {
	let mut v: u64 = 0;
	for &b in text {
		if b == b'_' {
			continue;
		}
		v = v.checked_mul(10)?.checked_add((b - b'0') as u64)?;
	}
	Some(v)
}

/// Parses a 32-bit float literal span: digits with `_` separators, an optional
/// fraction and exponent, and an optional `f` suffix.
pub fn parse_float32(text: &[u8]) -> f32 {
	let digits = |b: &&u8| **b != b'_' && **b != b'f';
	let mut buf = [0u8; 64];
	let n = text.iter().filter(digits).count();
	if n > buf.len() {
		let s: String = text.iter().filter(digits).map(|&b| b as char).collect();
		return s.parse().unwrap_or(f32::NAN);
	}
	for (slot, &b) in buf.iter_mut().zip(text.iter().filter(digits)) {
		*slot = b;
	}
	core::str::from_utf8(&buf[..n]).ok().and_then(|s| s.parse().ok()).unwrap_or(f32::NAN)
}

/// Parses a float literal span: digits with `_` separators, an optional
/// fraction and exponent, and an optional `f` suffix.
pub fn parse_float(text: &[u8]) -> f64 {
	let digits = |b: &&u8| **b != b'_' && **b != b'f';
	let mut buf = [0u8; 64];
	let n = text.iter().filter(digits).count();
	if n > buf.len() {
		// A literal written out in full, such as `f64::MAX` without an
		// exponent, is read whole; only these allocate.
		let s: String = text.iter().filter(digits).map(|&b| b as char).collect();
		return s.parse().unwrap_or(f64::NAN);
	}
	for (slot, &b) in buf.iter_mut().zip(text.iter().filter(digits)) {
		*slot = b;
	}
	core::str::from_utf8(&buf[..n]).ok().and_then(|s| s.parse().ok()).unwrap_or(f64::NAN)
}

/// A decoded numeric literal value (excluding `Decimal` and `Duration`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DecodedNumber {
	Int(i64),
	I8(i8),
	U8(u8),
	I16(i16),
	U16(u16),
	I32(i32),
	U32(u32),
	U64(u64),
	Float(f64),
	F16(half::f16),
	F32(f32),
}

/// Decodes a lexed numeric literal `text` of the given `kind` (including an
/// optional leading `+` or `-` sign and trailing type suffix). Returns `None`
/// if `kind` is `Decimal` or `Duration`, or if the literal overflows its
/// target type.
pub fn decode_number(kind: NumKind, text: &[u8]) -> Option<DecodedNumber> {
	let (neg, body) = match text.strip_prefix(b"-") {
		Some(b) => (true, b),
		None => (false, text.strip_prefix(b"+").unwrap_or(text)),
	};
	match kind {
		NumKind::Int => {
			let u = decode_uint(body)?;
			let max = (i64::MAX as u64) + (neg as u64);
			if u > max {
				return None;
			}
			let v = if neg {
				(u as i64).wrapping_neg()
			} else {
				u as i64
			};
			Some(DecodedNumber::Int(v))
		}
		NumKind::I8 => {
			let u = decode_uint(&body[..body.len() - 2])?;
			let max = (i8::MAX as u64) + (neg as u64);
			if u > max {
				return None;
			}
			let v = if neg {
				(u as i8).wrapping_neg()
			} else {
				u as i8
			};
			Some(DecodedNumber::I8(v))
		}
		NumKind::U8 => {
			let u = decode_uint(&body[..body.len() - 2])?;
			if (neg && u != 0) || u > u8::MAX as u64 {
				return None;
			}
			Some(DecodedNumber::U8(u as u8))
		}
		NumKind::I16 => {
			let u = decode_uint(&body[..body.len() - 3])?;
			let max = (i16::MAX as u64) + (neg as u64);
			if u > max {
				return None;
			}
			let v = if neg {
				(u as i16).wrapping_neg()
			} else {
				u as i16
			};
			Some(DecodedNumber::I16(v))
		}
		NumKind::U16 => {
			let u = decode_uint(&body[..body.len() - 3])?;
			if (neg && u != 0) || u > u16::MAX as u64 {
				return None;
			}
			Some(DecodedNumber::U16(u as u16))
		}
		NumKind::I32 => {
			let u = decode_uint(&body[..body.len() - 3])?;
			let max = (i32::MAX as u64) + (neg as u64);
			if u > max {
				return None;
			}
			let v = if neg {
				(u as i32).wrapping_neg()
			} else {
				u as i32
			};
			Some(DecodedNumber::I32(v))
		}
		NumKind::U32 => {
			let u = decode_uint(&body[..body.len() - 3])?;
			if (neg && u != 0) || u > u32::MAX as u64 {
				return None;
			}
			Some(DecodedNumber::U32(u as u32))
		}
		NumKind::I64 => {
			let u = decode_uint(&body[..body.len() - 3])?;
			let max = (i64::MAX as u64) + (neg as u64);
			if u > max {
				return None;
			}
			let v = if neg {
				(u as i64).wrapping_neg()
			} else {
				u as i64
			};
			Some(DecodedNumber::Int(v))
		}
		NumKind::U64 => {
			let u = decode_uint(&body[..body.len() - 3])?;
			if neg && u != 0 {
				return None;
			}
			Some(DecodedNumber::U64(u))
		}
		NumKind::Float => Some(DecodedNumber::Float(parse_float(text))),
		NumKind::F16 => {
			let f = parse_float(&text[..text.len() - 3]);
			let h = half::f16::from_f64(f);
			if !h.is_finite() {
				return None;
			}
			Some(DecodedNumber::F16(h))
		}
		NumKind::F32 => {
			let f = parse_float32(&text[..text.len() - 3]);
			if !f.is_finite() {
				return None;
			}
			Some(DecodedNumber::F32(f))
		}
		NumKind::F64 => {
			let f = parse_float(&text[..text.len() - 3]);
			if !f.is_finite() {
				return None;
			}
			Some(DecodedNumber::Float(f))
		}
		NumKind::Decimal | NumKind::BigDecimal | NumKind::Duration => None,
	}
}

/// Decodes a lexed duration span to `(seconds, nanoseconds)`.
pub fn decode_duration(text: &[u8]) -> Option<(u64, u32)> {
	let mut secs: u64 = 0;
	let mut nanos: u128 = 0;
	let mut i = 0;
	while i < text.len() {
		let s = i;
		while i < text.len() && (text[i].is_ascii_digit() || text[i] == b'_') {
			i += 1;
		}
		let n = decode_uint(&text[s..i])?;
		let rest = &text[i..];
		let (len, mul_s, mul_n): (usize, u64, u128) = match rest {
			[b'n', b's', ..] => (2, 0, 1),
			[b'u', b's', ..] => (2, 0, 1_000),
			[0xC2, 0xB5, b's', ..] => (3, 0, 1_000),
			[b'm', b's', ..] => (2, 0, 1_000_000),
			[b's', ..] => (1, 1, 0),
			[b'm', ..] => (1, 60, 0),
			[b'h', ..] => (1, 3_600, 0),
			[b'd', ..] => (1, 86_400, 0),
			[b'w', ..] => (1, 604_800, 0),
			[b'y', ..] => (1, 31_536_000, 0),
			_ => return None,
		};
		i += len;
		if mul_s > 0 {
			secs = secs.checked_add(n.checked_mul(mul_s)?)?;
		} else {
			nanos = nanos.checked_add((n as u128).checked_mul(mul_n)?)?;
		}
	}
	let extra_secs = u64::try_from(nanos / 1_000_000_000).ok()?;
	secs = secs.checked_add(extra_secs)?;
	Some((secs, (nanos % 1_000_000_000) as u32))
}

#[cfg(test)]
mod test {
	use super::*;

	fn kinds(src: &str) -> Vec<T> {
		let mut l = Lexer::new(src.as_bytes());
		let mut out = Vec::new();
		loop {
			let t = l.next_token();
			if t.kind == T::Eof {
				return out;
			}
			out.push(t.kind);
		}
	}

	#[test]
	fn operators() {
		assert_eq!(
			kinds("a += 1 -> b <- c ?? d ?: e ... … ∋ × ⟨x y⟩ `z`"),
			vec![
				T::Ident,
				T::PlusEq,
				T::Digits,
				T::Arrow,
				T::Ident,
				T::Lt,
				T::Minus,
				T::Ident,
				T::QQ,
				T::Ident,
				T::QColon,
				T::Ident,
				T::DotDotDot,
				T::DotDotDot,
				T::Contains,
				T::Times,
				T::EscIdent,
				T::EscIdent,
			]
		);
	}

	#[test]
	fn numbers() {
		for (src, kind) in [
			("12", NumKind::Int),
			("1.5", NumKind::Float),
			("1e10", NumKind::Float),
			("1.5f", NumKind::Float),
			("3dec", NumKind::Decimal),
			("3bigdec", NumKind::BigDecimal),
			("1.50bigdec", NumKind::BigDecimal),
			("1e40bigdec", NumKind::BigDecimal),
			("1h30m", NumKind::Duration),
			("5ms", NumKind::Duration),
			("5us", NumKind::Duration),
			("2d", NumKind::Duration),
			("5i8", NumKind::I8),
			("5u8", NumKind::U8),
			("5i16", NumKind::I16),
			("5u16", NumKind::U16),
			("5i32", NumKind::I32),
			("5u32", NumKind::U32),
			("5i64", NumKind::I64),
			("5u64", NumKind::U64),
			("1.5f16", NumKind::F16),
			("5f16", NumKind::F16),
			("1.5f32", NumKind::F32),
			("5f32", NumKind::F32),
			("1.5f64", NumKind::F64),
			("5f64", NumKind::F64),
			("1_000u32", NumKind::U32),
			("1e3f32", NumKind::F32),
		] {
			let mut l = Lexer::new(src.as_bytes());
			assert_eq!(l.numeric(0), Ok(kind), "{src}");
			assert_eq!(l.pos as usize, src.len(), "{src}");
		}
		for src in ["1.5u8", "1e3i32", "1.5i16", "1e3u64", "1big", "1bigdecimal", "1bigdecx"] {
			let mut l = Lexer::new(src.as_bytes());
			assert_eq!(l.numeric(0), Err(LexError::InvalidNumber), "{src}");
		}
		assert_eq!(decode_duration(b"1h30m"), Some((5400, 0)));
		assert_eq!(decode_duration(b"1s500ms"), Some((1, 500_000_000)));
		assert_eq!(
			decode_duration(b"18446744073709551ms"),
			Some((18_446_744_073_709, 551_000_000))
		);
	}

	#[test]
	fn js_body_divides_after_an_object_literal_and_matches_after_a_block() {
		for src in [
			&b"{ const ratio = {} / 2; }"[..],
			b"{ return { a: 1 }.a / 2 / 3; }",
			b"{ const o = { a: { b: 1 } } / 2; }",
			b"{ if (x) { y(); } /}/.test(z); }",
			b"{ { } /}/.test(z); }",
			b"{ const f = () => { return 1; }; }",
			b"{ do { x(); } while (/}/.test(y)); }",
			b"{ if (x) {} else { y(); } }",
			b"{ const ratio = function() {} / 2; }",
			b"{ const ratio = function named(a = {}) { return a; } / 2; }",
			b"{ const ratio = async function() {} / 2; }",
			b"{ const ratio = class {} / 2; }",
			b"{ const ratio = class extends Base { m() {} } / 2; }",
			b"{ return function() {} / 2; }",
			b"{ function f() {} /}/.test(z); }",
			b"{ async function f() {} /}/.test(z); }",
			b"{ class C {} /}/.test(z); }",
		] {
			let mut l = Lexer::new(src);
			let end = l
				.js_body(0)
				.unwrap_or_else(|e| panic!("{} should lex: {e:?}", String::from_utf8_lossy(src)));
			assert_eq!(end as usize, src.len(), "{}", String::from_utf8_lossy(src));
		}
	}

	#[test]
	fn js_body_skips_braces_and_quotes_inside_regex_literals() {
		let src = b"{ return /}/.test(x) && /[\"'}]/g.test(y) && (a / b) > 0; }";
		let mut l = Lexer::new(src);
		let end = l.js_body(0).expect("js_body with regex literals should lex");
		assert_eq!(end as usize, src.len());
	}
}
