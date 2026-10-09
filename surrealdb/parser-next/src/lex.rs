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
	/// `b` or `f`), with `0` also for an `s` string, which is a plain one.
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
		matches!(self.kind, T::Ident | T::EscIdent)
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
	Duration,
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
				// `syn` lexes `+?` without `=` as one invalid token.
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
			_ => return T::Invalid,
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
		// Any other character outside ASCII directly after the digits starts
		// a duration whose unit is not `µs`, as `syn` reads it.
		if self.src[start as usize].is_ascii_digit()
			&& self.peek_at(0) >= 0x80
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
		if self.peek_at(0) == b'.' && self.peek_at(1).is_ascii_digit() {
			self.pos += 1;
			self.digits();
			kind = NumKind::Float;
		}
		if matches!(self.peek_at(0), b'e' | b'E') {
			let sign = matches!(self.peek_at(1), b'+' | b'-') as u32;
			if self.peek_at(1 + sign).is_ascii_digit() {
				self.pos += 1 + sign;
				self.digits();
				kind = NumKind::Float;
			}
		}
		if self.peek_at(0) == b'f' {
			self.pos += 1;
			kind = NumKind::Float;
		} else if self.peek_at(0) == b'd' && self.peek_at(1) == b'e' && self.peek_at(2) == b'c' {
			self.pos += 3;
			kind = NumKind::Decimal;
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
	/// the offset after the matching `}`. Strings, comments and nested
	/// braces are skipped as JavaScript defines them.
	pub fn js_body(&mut self, start: u32) -> Result<u32, LexError> {
		self.pos = start + 1;
		let mut depth = 1u32;
		loop {
			if self.at_end() {
				return Err(LexError::UnterminatedScript);
			}
			let b = self.peek_at(0);
			self.pos += 1;
			match b {
				b'`' | b'\'' | b'"' => loop {
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
				},
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
				b'{' => depth += 1,
				b'}' => {
					depth -= 1;
					if depth == 0 {
						return Ok(self.pos);
					}
				}
				_ => {}
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

/// Decodes a lexed duration span to `(seconds, nanoseconds)`.
pub fn decode_duration(text: &[u8]) -> Option<(u64, u32)> {
	let mut secs: u64 = 0;
	let mut nanos: u64 = 0;
	let mut i = 0;
	while i < text.len() {
		let s = i;
		while i < text.len() && (text[i].is_ascii_digit() || text[i] == b'_') {
			i += 1;
		}
		let n = decode_uint(&text[s..i])?;
		let rest = &text[i..];
		let (len, mul_s, mul_n): (usize, u64, u64) = match rest {
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
			nanos = nanos.checked_add(n.checked_mul(mul_n)?)?;
		}
	}
	secs = secs.checked_add(nanos / 1_000_000_000)?;
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
			("1h30m", NumKind::Duration),
			("5ms", NumKind::Duration),
			("2d", NumKind::Duration),
		] {
			let mut l = Lexer::new(src.as_bytes());
			assert_eq!(l.numeric(0), Ok(kind), "{src}");
			assert_eq!(l.pos as usize, src.len(), "{src}");
		}
		assert_eq!(decode_duration(b"1h30m"), Some((5400, 0)));
		assert_eq!(decode_duration(b"1s500ms"), Some((1, 500_000_000)));
	}
}
