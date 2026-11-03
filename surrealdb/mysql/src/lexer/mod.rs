//! Zero-allocation lexical analyzer for MySQL SQL syntax.

pub mod keyword;
pub mod span;
pub mod token;

use self::keyword::Keyword;
use self::span::Span;
pub use self::token::{Token, TokenKind};
use crate::parser::ParserSettings;

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

/// The most digits an exact-value number holds, as a DECIMAL's precision.
const MAX_EXACT_DIGITS: usize = 65;

/// The exact-value literal `text` (digits with at most one point), or an invalid number when it
/// has more significant digits than a DECIMAL holds: those of the integer part past its leading
/// zeros, and every digit of the fraction.
fn exact_literal(text: String, offset: usize) -> Result<TokenKind, LexError> {
	let (integer, fraction) = text.split_once('.').unwrap_or((&text, ""));
	if integer.trim_start_matches('0').len() + fraction.len() > MAX_EXACT_DIGITS {
		return Err(LexError::InvalidNumber {
			offset,
		});
	}
	Ok(TokenKind::DecimalLiteral(text))
}

/// Tokenize an entire MySQL SQL source string into a vector of tokens.
pub fn tokenize(src: &str) -> Result<Vec<Token>, LexError> {
	tokenize_with_settings(src, ParserSettings::default())
}

/// Tokenize a MySQL SQL source string with custom parser/lexer settings.
pub fn tokenize_with_settings(src: &str, settings: ParserSettings) -> Result<Vec<Token>, LexError> {
	let mut lexer = Lexer::with_settings(src, settings);
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
	settings: ParserSettings,
	executable_comment_depth: usize,
}

impl<'a> Lexer<'a> {
	/// Create a new lexer for the given source text.
	pub fn new(src: &'a str) -> Self {
		Self::with_settings(src, ParserSettings::default())
	}

	/// Create a new lexer with custom settings.
	pub fn with_settings(src: &'a str, settings: ParserSettings) -> Self {
		let mut chars = src.char_indices();
		let peeked = chars.next();
		Self {
			src,
			chars,
			peeked,
			param_counter: 0,
			settings,
			executable_comment_depth: 0,
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
				// Closing */ of an executable comment
				Some((_, '*')) if self.executable_comment_depth > 0 => {
					let mut clone = self.chars.clone();
					if let Some((_, '/')) = clone.next() {
						self.bump(); // '*'
						self.bump(); // '/'
						self.executable_comment_depth =
							self.executable_comment_depth.saturating_sub(1);
						continue;
					}
					break;
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
				// Block comment '/* ... */' or executable comment '/*! ... */'
				Some((start, '/')) => {
					let mut clone = self.chars.clone();
					if let Some((_, '*')) = clone.next() {
						if let Some((_, '!')) = clone.next() {
							// Executable version comment `/*!` or `/*!50000`
							let mut version_digits = String::new();
							for (_, c) in &mut clone {
								if c.is_ascii_digit() && version_digits.len() < 5 {
									version_digits.push(c);
								} else {
									break;
								}
							}
							let is_executable = if version_digits.len() == 5 {
								let ver: u32 = version_digits.parse().unwrap_or(0);
								ver <= self.settings.server_version
							} else {
								true
							};

							if is_executable {
								self.bump(); // '/'
								self.bump(); // '*'
								self.bump(); // '!'
								for _ in 0..version_digits.len() {
									self.bump();
								}
								self.executable_comment_depth += 1;
								continue;
							} else {
								// Not executable for this server version, skip to `*/`
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
								continue;
							}
						}

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
				if self.executable_comment_depth > 0 {
					return Err(LexError::UnclosedComment {
						offset: self.src.len(),
					});
				}
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

			// String literals: single quote '...' or double quote "..." (or identifier under
			// ANSI_QUOTES)
			'\'' => return self.scan_string(start, '\''),
			'"' => {
				if self.settings.sql_mode.ansi_quotes {
					return self.scan_ansi_quote_ident(start);
				}
				return self.scan_string(start, '"');
			}

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
			// User variable: @var, @'var', @"var", @`var`, or standalone @
			let name = self.scan_raw_ident();
			let end = self.offset();
			if name.is_empty() {
				Ok(Token::new(TokenKind::At, Span::new(start, end)))
			} else {
				Ok(Token::new(TokenKind::UserVar(name), Span::new(start, end)))
			}
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

		if let Some((_, quote @ ('\'' | '"'))) = self.peek() {
			self.bump();
			while let Some((_, ch)) = self.bump() {
				if ch == quote {
					if let Some((_, next)) = self.peek()
						&& next == quote
					{
						self.bump();
						s.push(quote);
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

	fn scan_ansi_quote_ident(&mut self, start: usize) -> Result<Token, LexError> {
		let mut name = String::new();
		loop {
			match self.bump() {
				Some((_, '"')) => {
					if let Some((_, '"')) = self.peek() {
						self.bump();
						name.push('"');
					} else {
						break;
					}
				}
				Some((_, ch)) => {
					name.push(ch);
				}
				None => {
					return Err(LexError::UnclosedString {
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
				Some((_, '\\')) if !self.settings.sql_mode.no_backslash_escapes => {
					match self.bump() {
						Some((_, '0')) => content.push('\0'),
						Some((_, 'b')) => content.push('\x08'),
						Some((_, 'n')) => content.push('\n'),
						Some((_, 'r')) => content.push('\r'),
						Some((_, 't')) => content.push('\t'),
						Some((_, 'Z')) => content.push('\x1a'),
						// MySQL keeps the backslash of `\%` and `\_` in the string, so that a
						// `LIKE` that reads the string finds the wildcard escaped; any other
						// reader of the string sees the two characters.
						Some((_, esc @ ('%' | '_'))) => {
							content.push('\\');
							content.push(esc);
						}
						Some((_, esc)) => content.push(esc),
						None => {
							return Err(LexError::UnclosedString {
								offset: start,
							});
						}
					}
				}
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
		if let Some((_, ch)) = self.peek()
			&& (ch.is_alphabetic() || ch == '_' || ch == '$')
		{
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

		if let Some((_, ch)) = self.peek()
			&& (ch.is_alphabetic() || ch == '$')
		{
			return Err(LexError::InvalidNumber {
				offset: start,
			});
		}

		let end = self.offset();
		if is_float && !num_str.contains(['e', 'E']) {
			Ok(Token::new(exact_literal(num_str, start)?, Span::new(start, end)))
		} else if is_float {
			let val = num_str.parse::<f64>().map_err(|_| LexError::InvalidNumber {
				offset: start,
			})?;
			Ok(Token::new(TokenKind::FloatLiteral(val), Span::new(start, end)))
		} else {
			match num_str.parse::<i64>() {
				Ok(val) => Ok(Token::new(TokenKind::IntLiteral(val), Span::new(start, end))),
				Err(_) => {
					// The magnitude of the minimum 64-bit integer stays a double, so the sign of
					// the literal can be folded into it. Every other integer is exact: up to
					// `u64::MAX` an unsigned integer, and beyond it a decimal.
					if let Ok(val) = num_str.parse::<u64>()
						&& val != 1 << 63
					{
						return Ok(Token::new(
							TokenKind::UnsignedIntLiteral(val),
							Span::new(start, end),
						));
					}
					if num_str.parse::<u64>() != Ok(1 << 63)
						&& num_str.parse::<f64>().is_ok_and(f64::is_finite)
					{
						return Ok(Token::new(
							exact_literal(num_str, start)?,
							Span::new(start, end),
						));
					}
					let value =
						num_str.parse::<f64>().ok().filter(|value| value.is_finite()).ok_or(
							LexError::InvalidNumber {
								offset: start,
							},
						)?;
					Ok(Token::new(
						TokenKind::WideIntLiteral {
							value,
							int8_minimum_magnitude: num_str.parse::<u64>() == Ok(1 << 63),
						},
						Span::new(start, end),
					))
				}
			}
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
		if !num_str.contains(['e', 'E']) {
			return Ok(Token::new(exact_literal(num_str, start)?, Span::new(start, end)));
		}
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

		// Check for character set introducer: _charset'...' or _charset 0x...
		if first == '_' && s.len() > 1 {
			let cs = &s[1..];
			if is_valid_mysql_charset(cs) {
				let is_intro = {
					let mut clone = self.chars.clone();
					let mut next_char = self.peek().map(|(_, c)| c);
					while let Some(c) = next_char {
						if c.is_whitespace() {
							next_char = clone.next().map(|(_, c)| c);
						} else {
							break;
						}
					}
					match next_char {
						Some('\'') => true,
						Some('"') if !self.settings.sql_mode.ansi_quotes => true,
						Some('0') => {
							let next2 = clone.next().map(|(_, c)| c);
							matches!(next2, Some('x' | 'X' | 'b' | 'B'))
						}
						Some('x' | 'X' | 'b' | 'B') => {
							let next2 = clone.next().map(|(_, c)| c);
							matches!(next2, Some('\''))
						}
						_ => false,
					}
				};
				if is_intro {
					return Ok(Token::new(
						TokenKind::CharsetIntro(cs.to_ascii_lowercase()),
						Span::new(start, end),
					));
				}
			}
		}

		if let Some(kw) = Keyword::from_str_ignore_ascii_case(&s) {
			Ok(Token::new(TokenKind::Keyword(kw), Span::new(start, end)))
		} else {
			Ok(Token::new(TokenKind::Ident(s), Span::new(start, end)))
		}
	}
}

/// Check if a charset identifier is a recognized MySQL character set.
pub fn is_valid_mysql_charset(cs: &str) -> bool {
	matches!(
		cs.to_ascii_lowercase().as_str(),
		"utf8mb4"
			| "utf8" | "utf8mb3"
			| "latin1"
			| "binary"
			| "ascii" | "ucs2"
			| "utf16" | "utf16le"
			| "utf32" | "armscii8"
			| "big5" | "cp1250"
			| "cp1251"
			| "cp1256"
			| "cp1257"
			| "cp850" | "cp852"
			| "cp866" | "cp932"
			| "dec8" | "eucjpms"
			| "euckr" | "gb18030"
			| "gb2312"
			| "gbk" | "geostd8"
			| "greek" | "hebrew"
			| "hp8" | "keybcs2"
			| "koi8r" | "koi8u"
			| "latin2"
			| "latin5"
			| "latin7"
			| "macce" | "macroman"
			| "sjis" | "swe7"
			| "tis620"
			| "ujis"
	)
}

#[inline]
fn is_ident_start(ch: char) -> bool {
	ch.is_ascii_alphabetic() || ch == '_' || ch == '$' || (!ch.is_ascii() && ch.is_alphabetic())
}

#[inline]
fn is_ident_part(ch: char) -> bool {
	ch.is_ascii_alphanumeric()
		|| ch == '_'
		|| ch == '$'
		|| (!ch.is_ascii() && (ch.is_alphabetic() || ch.is_numeric()))
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
