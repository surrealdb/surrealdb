use crate::syn::token::Span;

use super::{unicode::chars::JS_LINE_TERIMATORS, Error, Lexer};

impl Lexer<'_> {
	/// Lex the body of a js functions.
	///
	/// This function will never be called while lexing normally.
	pub fn lex_js_function_body(&mut self) -> Result<String, (Error, Span)> {
		self.lex_js_function_body_inner().map_err(|e| (e, self.current_span()))
	}

	/// Lex the body of a js function.
	fn lex_js_function_body_inner(&mut self) -> Result<String, Error> {
		let mut block_depth = 1;
		loop {
			let byte = self.reader.next().ok_or(Error::UnexpectedEof)?;
			match byte {
				b'`' => self.lex_js_string(b'`')?,
				b'\'' => self.lex_js_string(b'\'')?,
				b'\"' => self.lex_js_string(b'\"')?,
				b'/' => match self.reader.peek() {
					Some(b'/') => {
						self.reader.next();
						self.lex_js_single_comment()?;
					}
					Some(b'*') => {
						self.reader.next();
						self.lex_js_multi_comment()?
					}
					_ => {}
				},
				b'{' => {
					block_depth += 1;
				}
				b'}' => {
					block_depth -= 1;
					if block_depth == 0 {
						break;
					}
				}
				x if !x.is_ascii() => {
					// check for invalid characters.
					self.reader.complete_char(x)?;
				}
				_ => {}
			}
		}
		let mut span = self.current_span();
		// remove the `}` from the source text;
		span.len -= 1;
		// lexer ensures that it is valid utf8
		let source = String::from_utf8(self.reader.span(span).to_vec()).unwrap();
		Ok(source)
	}

	/// lex a js string with the given delimiter.
	fn lex_js_string(&mut self, enclosing_byte: u8) -> Result<(), Error> {
		loop {
			let byte = self.reader.next().ok_or(Error::UnexpectedEof)?;
			if byte == enclosing_byte {
				return Ok(());
			}
			if byte == b'\\' {
				self.reader.next();
			}
			// check for invalid characters.
			self.reader.convert_to_char(byte)?;
		}
	}

	/// lex a single line js comment.
	fn lex_js_single_comment(&mut self) -> Result<(), Error> {
		loop {
			let Some(byte) = self.reader.next() else {
				return Ok(());
			};
			let char = self.reader.convert_to_char(byte)?;
			if JS_LINE_TERIMATORS.contains(&char) {
				return Ok(());
			}
		}
	}

	/// lex a multi line js comment.
	fn lex_js_multi_comment(&mut self) -> Result<(), Error> {
		loop {
			let byte = self.reader.next().ok_or(Error::UnexpectedEof)?;
			if byte == b'*' && self.reader.peek() == Some(b'/') {
				self.reader.next();
				return Ok(());
			}
			// check for invalid characters.
			self.reader.convert_to_char(byte)?;
		}
	}
}
