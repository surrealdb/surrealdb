//! Zero-allocation lexical analyzer for MySQL SQL syntax.

pub mod keyword;
pub mod span;
pub mod token;

use self::keyword::Keyword;
use self::span::Span;
use self::token::{Token, TokenKind};

/// Lexer error representing an invalid character or unclosed literal.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum LexError {
	#[error("Unexpected character '{ch}' at offset {offset}")]
	UnexpectedChar {
		ch: char,
		offset: usize,
	},
	#[error("Unclosed string literal starting at offset {offset}")]
	UnclosedString {
		offset: usize,
	},
	#[error("Unclosed backtick identifier starting at offset {offset}")]
	UnclosedBacktick {
		offset: usize,
	},
	#[error("Unclosed block comment starting at offset {offset}")]
	UnclosedComment {
		offset: usize,
	},
	#[error("Invalid number literal at offset {offset}")]
	InvalidNumber {
		offset: usize,
	},
	#[error("Invalid hex literal at offset {offset}")]
	InvalidHex {
		offset: usize,
	},
}

/// Tokenize an entire MySQL SQL source string into a vector of tokens.
pub fn tokenize(src: &str) -> Result<Vec<Token>, LexError> {
	let mut lexer = Lexer::new(src);
	let mut tokens = Vec::new();
	loop {
		let tok = lexer.next_token()?;
		let is_eof = tok.kind == TokenKind::Eof;
		tokens.push(tok);
		if is_eof {
			break;
		}
	}
	Ok(tokens)
}

/// Lexical scanner for MySQL SQL.
pub struct Lexer<'a> {
	src: &'a str,
	chars: std::str::CharIndices<'a>,
	peeked: Option<(usize, char)>,
	param_counter: usize,
}

impl<'a> Lexer<'a> {
	/// Create a new lexer for the given source text.
	pub fn new(src: &'a str) -> Self {
		let mut chars = src.char_indices();
		let peeked = chars.next();
		Self {
			src,
			chars,
			peeked,
			param_counter: 0,
		}
	}

	#[inline]
	fn peek(&self) -> Option<(usize, char)> {
		self.peeked
	}

	#[inline]
	fn bump(&mut self) -> Option<(usize, char)> {
		let current = self.peeked;
		self.peeked = self.chars.next();
		current
	}

	#[inline]
	fn offset(&self) -> usize {
		self.peeked.map_or(self.src.len(), |(pos, _)| pos)
	}

	/// Consume whitespace and comments.
	fn skip_whitespace_and_comments(&mut self) -> Result<(), LexError> {
		loop {
			match self.peek() {
				Some((_, ch)) if ch.is_whitespace() => {
					self.bump();
				}
				// MySQL '#' line comment
				Some((_, '#')) => {
					self.bump();
					while let Some((_, ch)) = self.bump() {
						if ch == '\n' {
							break;
						}
					}
				}
				// MySQL '-- ' line comment (requires whitespace after -- or newline/EOF)
				Some((_pos, '-')) => {
					let mut clone = self.chars.clone();
					if let Some((_, '-')) = clone.next() {
						let is_comment = match clone.next() {
							Some((_, next_ch)) => {
								next_ch.is_whitespace() || next_ch == '\n' || next_ch == '\r'
							}
							None => true, // EOF after -- is a valid comment
						};
						if is_comment {
							self.bump(); // first -
							self.bump(); // second -
							while let Some((_, ch)) = self.bump() {
								if ch == '\n' {
									break;
								}
							}
						} else {
							break;
						}
					} else {
						break;
					}
				}
				// Block comment '/* ... */'
				Some((start, '/')) => {
					let mut clone = self.chars.clone();
					if let Some((_, '*')) = clone.next() {
						self.bump(); // '/'
						self.bump(); // '*'
						let mut closed = false;
						while let Some((_, ch)) = self.bump() {
							if ch == '*' && matches!(self.peek(), Some((_, '/'))) {
								self.bump(); // '/'
								closed = true;
								break;
							}
						}
						if !closed {
							return Err(LexError::UnclosedComment {
								offset: start,
							});
						}
					} else {
						break;
					}
				}
				_ => break,
			}
		}
		Ok(())
	}

	/// Scan the next token from source.
	pub fn next_token(&mut self) -> Result<Token, LexError> {
		self.skip_whitespace_and_comments()?;

		let (start, ch) = match self.bump() {
			Some(pair) => pair,
			None => {
				let pos = self.src.len();
				return Ok(Token::new(TokenKind::Eof, Span::new(pos, pos)));
			}
		};

		// 1. Single character punctuation & multi-char operators
		match ch {
			',' => return Ok(Token::new(TokenKind::Comma, Span::new(start, start + 1))),
			';' => return Ok(Token::new(TokenKind::Semicolon, Span::new(start, start + 1))),
			'.' => {
				// Check for float starting with dot: .123
				if let Some((_, next)) = self.peek()
					&& next.is_ascii_digit()
				{
					return self.scan_number_starting_with_dot(start);
				}
				return Ok(Token::new(TokenKind::Dot, Span::new(start, start + 1)));
			}
			'(' => return Ok(Token::new(TokenKind::LParen, Span::new(start, start + 1))),
			')' => return Ok(Token::new(TokenKind::RParen, Span::new(start, start + 1))),
			'[' => return Ok(Token::new(TokenKind::LBracket, Span::new(start, start + 1))),
			']' => return Ok(Token::new(TokenKind::RBracket, Span::new(start, start + 1))),
			'{' => return Ok(Token::new(TokenKind::LBrace, Span::new(start, start + 1))),
			'}' => return Ok(Token::new(TokenKind::RBrace, Span::new(start, start + 1))),
			'+' => return Ok(Token::new(TokenKind::Plus, Span::new(start, start + 1))),
			'-' => return Ok(Token::new(TokenKind::Minus, Span::new(start, start + 1))),
			'*' => return Ok(Token::new(TokenKind::Star, Span::new(start, start + 1))),
			'/' => return Ok(Token::new(TokenKind::Slash, Span::new(start, start + 1))),
			'%' => return Ok(Token::new(TokenKind::Percent, Span::new(start, start + 1))),
			'^' => return Ok(Token::new(TokenKind::Caret, Span::new(start, start + 1))),
			'~' => return Ok(Token::new(TokenKind::Tilde, Span::new(start, start + 1))),
			'?' => {
				self.param_counter += 1;
				return Ok(Token::new(
					TokenKind::Param(self.param_counter),
					Span::new(start, start + 1),
				));
			}
			':' => {
				if let Some((_, '=')) = self.peek() {
					self.bump();
					return Ok(Token::new(TokenKind::ColonEqual, Span::new(start, start + 2)));
				}
				return Ok(Token::new(TokenKind::Colon, Span::new(start, start + 1)));
			}
			'=' => return Ok(Token::new(TokenKind::Equal, Span::new(start, start + 1))),
			'!' => {
				if let Some((_, '=')) = self.peek() {
					self.bump();
					return Ok(Token::new(TokenKind::NotEqual, Span::new(start, start + 2)));
				}
				return Ok(Token::new(TokenKind::Bang, Span::new(start, start + 1)));
			}
			'<' => {
				if let Some((_, '=')) = self.peek() {
					self.bump();
					if let Some((_, '>')) = self.peek() {
						self.bump();
						return Ok(Token::new(
							TokenKind::NullSafeEqual,
							Span::new(start, start + 3),
						));
					}
					return Ok(Token::new(TokenKind::LessEqual, Span::new(start, start + 2)));
				}
				if let Some((_, '>')) = self.peek() {
					self.bump();
					return Ok(Token::new(TokenKind::NotEqual, Span::new(start, start + 2)));
				}
				if let Some((_, '<')) = self.peek() {
					self.bump();
					return Ok(Token::new(TokenKind::ShiftLeft, Span::new(start, start + 2)));
				}
				return Ok(Token::new(TokenKind::Less, Span::new(start, start + 1)));
			}
			'>' => {
				if let Some((_, '=')) = self.peek() {
					self.bump();
					return Ok(Token::new(TokenKind::GreaterEqual, Span::new(start, start + 2)));
				}
				if let Some((_, '>')) = self.peek() {
					self.bump();
					return Ok(Token::new(TokenKind::ShiftRight, Span::new(start, start + 2)));
				}
				return Ok(Token::new(TokenKind::Greater, Span::new(start, start + 1)));
			}
			'&' => {
				if let Some((_, '&')) = self.peek() {
					self.bump();
					return Ok(Token::new(TokenKind::DoubleAmpersand, Span::new(start, start + 2)));
				}
				return Ok(Token::new(TokenKind::Ampersand, Span::new(start, start + 1)));
			}
			'|' => {
				if let Some((_, '|')) = self.peek() {
					self.bump();
					return Ok(Token::new(TokenKind::DoublePipe, Span::new(start, start + 2)));
				}
				return Ok(Token::new(TokenKind::Pipe, Span::new(start, start + 1)));
			}

			// Variables: @user_var, @@system_var, @@global.system_var, @@session.system_var
			'@' => return self.scan_variable(start),

			// Backtick quoted identifier
			'`' => return self.scan_backtick_ident(start),

			// String literals: single quote '...' or double quote "..."
			'\'' | '"' => return self.scan_string(start, ch),

			_ => {}
		}

		// 2. Numbers or Hex/Bit prefixes
		if ch.is_ascii_digit() {
			if ch == '0' {
				if let Some((_, 'x')) | Some((_, 'X')) = self.peek() {
					self.bump();
					return self.scan_hex_literal(start);
				}
				if let Some((_, 'b')) | Some((_, 'B')) = self.peek() {
					self.bump();
					return self.scan_bit_literal(start);
				}
			}
			return self.scan_number(start, ch);
		}

		// Hex string: x'...' or X'...'
		if (ch == 'x' || ch == 'X') && matches!(self.peek(), Some((_, '\''))) {
			self.bump(); // consume '\''
			return self.scan_hex_quote(start);
		}

		// Bit string: b'...' or B'...'
		if (ch == 'b' || ch == 'B') && matches!(self.peek(), Some((_, '\''))) {
			self.bump(); // consume '\''
			return self.scan_bit_quote(start);
		}

		// 3. Identifiers and Keywords
		if is_ident_start(ch) {
			return self.scan_ident_or_keyword(start, ch);
		}

		Err(LexError::UnexpectedChar {
			ch,
			offset: start,
		})
	}

	fn scan_variable(&mut self, start: usize) -> Result<Token, LexError> {
		if let Some((_, '@')) = self.peek() {
			// System variable: @@var or @@scope.var
			self.bump(); // second '@'
			let mut is_global = false;
			let name_start = self.offset();
			let mut name = self.scan_raw_ident();

			if (name.eq_ignore_ascii_case("global")
				|| name.eq_ignore_ascii_case("session")
				|| name.eq_ignore_ascii_case("local"))
				&& matches!(self.peek(), Some((_, '.')))
			{
				if name.eq_ignore_ascii_case("global") {
					is_global = true;
				}
				self.bump(); // consume '.'
				name = self.scan_raw_ident();
			}

			let end = self.offset();
			if name.is_empty() {
				return Err(LexError::UnexpectedChar {
					ch: '@',
					offset: name_start,
				});
			}

			Ok(Token::new(
				TokenKind::SystemVar {
					name,
					is_global,
				},
				Span::new(start, end),
			))
		} else {
			// User variable: @var
			let name = self.scan_raw_ident();
			let end = self.offset();
			Ok(Token::new(TokenKind::UserVar(name), Span::new(start, end)))
		}
	}

	fn scan_raw_ident(&mut self) -> String {
		let mut s = String::new();
		if let Some((_, '`')) = self.peek() {
			self.bump();
			while let Some((_, ch)) = self.bump() {
				if ch == '`' {
					if let Some((_, '`')) = self.peek() {
						self.bump();
						s.push('`');
					} else {
						break;
					}
				} else {
					s.push(ch);
				}
			}
			return s;
		}

		while let Some((_, ch)) = self.peek() {
			if is_ident_part(ch) {
				self.bump();
				s.push(ch);
			} else {
				break;
			}
		}
		s
	}

	fn scan_backtick_ident(&mut self, start: usize) -> Result<Token, LexError> {
		let mut name = String::new();
		loop {
			match self.bump() {
				Some((_, '`')) => {
					if let Some((_, '`')) = self.peek() {
						self.bump();
						name.push('`');
					} else {
						break;
					}
				}
				Some((_, ch)) => {
					name.push(ch);
				}
				None => {
					return Err(LexError::UnclosedBacktick {
						offset: start,
					});
				}
			}
		}
		let end = self.offset();
		Ok(Token::new(TokenKind::Ident(name), Span::new(start, end)))
	}

	fn scan_string(&mut self, start: usize, quote: char) -> Result<Token, LexError> {
		let mut content = String::new();
		loop {
			match self.bump() {
				Some((_, ch)) if ch == quote => {
					if let Some((_, next)) = self.peek()
						&& next == quote
					{
						self.bump();
						content.push(quote);
					} else {
						break;
					}
				}
				Some((_, '\\')) => match self.bump() {
					Some((_, '0')) => content.push('\0'),
					Some((_, 'b')) => content.push('\x08'),
					Some((_, 'n')) => content.push('\n'),
					Some((_, 'r')) => content.push('\r'),
					Some((_, 't')) => content.push('\t'),
					Some((_, 'Z')) => content.push('\x1a'),
					Some((_, esc)) => content.push(esc),
					None => {
						return Err(LexError::UnclosedString {
							offset: start,
						});
					}
				},
				Some((_, ch)) => {
					content.push(ch);
				}
				None => {
					return Err(LexError::UnclosedString {
						offset: start,
					});
				}
			}
		}
		let end = self.offset();
		Ok(Token::new(TokenKind::StringLiteral(content), Span::new(start, end)))
	}

	fn scan_hex_literal(&mut self, start: usize) -> Result<Token, LexError> {
		let mut hex = String::new();
		while let Some((_, ch)) = self.peek() {
			if ch.is_ascii_hexdigit() {
				self.bump();
				hex.push(ch);
			} else {
				break;
			}
		}
		if hex.is_empty() {
			return Err(LexError::InvalidHex {
				offset: start,
			});
		}
		let bytes = decode_hex(&hex).map_err(|_| LexError::InvalidHex {
			offset: start,
		})?;
		let end = self.offset();
		Ok(Token::new(TokenKind::HexLiteral(bytes), Span::new(start, end)))
	}

	fn scan_hex_quote(&mut self, start: usize) -> Result<Token, LexError> {
		let mut hex = String::new();
		loop {
			match self.bump() {
				Some((_, '\'')) => break,
				Some((_, ch)) if ch.is_ascii_hexdigit() => hex.push(ch),
				_ => {
					return Err(LexError::InvalidHex {
						offset: start,
					});
				}
			}
		}
		let bytes = decode_hex(&hex).map_err(|_| LexError::InvalidHex {
			offset: start,
		})?;
		let end = self.offset();
		Ok(Token::new(TokenKind::HexLiteral(bytes), Span::new(start, end)))
	}

	fn scan_bit_literal(&mut self, start: usize) -> Result<Token, LexError> {
		let mut bits = Vec::new();
		while let Some((_, ch)) = self.peek() {
			if ch == '0' || ch == '1' {
				self.bump();
				bits.push(ch as u8 - b'0');
			} else {
				break;
			}
		}
		if bits.is_empty() {
			return Err(LexError::InvalidNumber {
				offset: start,
			});
		}
		let end = self.offset();
		Ok(Token::new(TokenKind::BitLiteral(bits), Span::new(start, end)))
	}

	fn scan_bit_quote(&mut self, start: usize) -> Result<Token, LexError> {
		let mut bits = Vec::new();
		loop {
			match self.bump() {
				Some((_, '\'')) => break,
				Some((_, '0')) => bits.push(0),
				Some((_, '1')) => bits.push(1),
				_ => {
					return Err(LexError::InvalidNumber {
						offset: start,
					});
				}
			}
		}
		let end = self.offset();
		Ok(Token::new(TokenKind::BitLiteral(bits), Span::new(start, end)))
	}

	fn scan_number(&mut self, start: usize, first: char) -> Result<Token, LexError> {
		let mut num_str = String::new();
		num_str.push(first);
		let mut is_float = false;

		while let Some((_, ch)) = self.peek() {
			if ch.is_ascii_digit() {
				self.bump();
				num_str.push(ch);
			} else if ch == '.' && !is_float {
				// Check that next character is not another dot (e.g. range ..)
				let mut clone = self.chars.clone();
				if let Some((_, next)) = clone.next()
					&& next.is_ascii_digit()
				{
					self.bump();
					num_str.push('.');
					is_float = true;
				} else {
					break;
				}
			} else if (ch == 'e' || ch == 'E') && !num_str.contains('e') && !num_str.contains('E') {
				self.bump();
				num_str.push(ch);
				is_float = true;
				if let Some((_, sign @ ('+' | '-'))) = self.peek() {
					self.bump();
					num_str.push(sign);
				}
			} else {
				break;
			}
		}

		let end = self.offset();
		if is_float {
			let val = num_str.parse::<f64>().map_err(|_| LexError::InvalidNumber {
				offset: start,
			})?;
			Ok(Token::new(TokenKind::FloatLiteral(val), Span::new(start, end)))
		} else {
			let val = num_str.parse::<i64>().map_err(|_| LexError::InvalidNumber {
				offset: start,
			})?;
			Ok(Token::new(TokenKind::IntLiteral(val), Span::new(start, end)))
		}
	}

	fn scan_number_starting_with_dot(&mut self, start: usize) -> Result<Token, LexError> {
		let mut num_str = String::from("0.");
		while let Some((_, ch)) = self.peek() {
			if ch.is_ascii_digit() {
				self.bump();
				num_str.push(ch);
			} else if (ch == 'e' || ch == 'E') && !num_str.contains('e') && !num_str.contains('E') {
				self.bump();
				num_str.push(ch);
				if let Some((_, sign @ ('+' | '-'))) = self.peek() {
					self.bump();
					num_str.push(sign);
				}
			} else {
				break;
			}
		}
		let end = self.offset();
		let val = num_str.parse::<f64>().map_err(|_| LexError::InvalidNumber {
			offset: start,
		})?;
		Ok(Token::new(TokenKind::FloatLiteral(val), Span::new(start, end)))
	}

	fn scan_ident_or_keyword(&mut self, start: usize, first: char) -> Result<Token, LexError> {
		let mut s = String::new();
		s.push(first);
		while let Some((_, ch)) = self.peek() {
			if is_ident_part(ch) {
				self.bump();
				s.push(ch);
			} else {
				break;
			}
		}
		let end = self.offset();

		if let Some(kw) = Keyword::from_str_ignore_ascii_case(&s) {
			Ok(Token::new(TokenKind::Keyword(kw), Span::new(start, end)))
		} else {
			Ok(Token::new(TokenKind::Ident(s), Span::new(start, end)))
		}
	}
}

#[inline]
fn is_ident_start(ch: char) -> bool {
	ch.is_ascii_alphabetic() || ch == '_' || ch == '$'
}

#[inline]
fn is_ident_part(ch: char) -> bool {
	ch.is_ascii_alphanumeric() || ch == '_' || ch == '$'
}

fn decode_hex(s: &str) -> Result<Vec<u8>, ()> {
	let mut s_padded = s.to_string();
	if !s_padded.len().is_multiple_of(2) {
		s_padded.insert(0, '0');
	}
	let mut bytes = Vec::with_capacity(s_padded.len() / 2);
	for i in (0..s_padded.len()).step_by(2) {
		let byte = u8::from_str_radix(&s_padded[i..i + 2], 16).map_err(|_| ())?;
		bytes.push(byte);
	}
	Ok(bytes)
}
