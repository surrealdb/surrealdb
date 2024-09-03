use crate::sql::Regex;
use crate::syn::{
	error::{bail, error, SyntaxError},
	lexer::Lexer,
	token::{t, CompoundToken, Token, TokenKind},
};

mod js;
mod uuid;

pub trait CompoundValue: Sized {
	/// The token which indicates the start of this compound token.
	const START: &'static [TokenKind];

	/// Lex the start of this span to a more complex type of token.
	fn relex(lexer: &mut Lexer, start_token: Token) -> Result<CompoundToken<Self>, SyntaxError>;
}

impl<'a> Lexer<'a> {
	pub fn lex_compound<T: CompoundValue>(
		&mut self,
		start: Token,
	) -> Result<CompoundToken<T>, SyntaxError> {
		assert!(
			T::START.contains(&start.kind),
			"Invalid start of compound token, expected {} got {}",
			T::START.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(", "),
			start.kind
		);
		assert_eq!(
			start.span.offset + start.span.len,
			self.last_offset,
			"Tried to parse compound when lexer already ate past the  start token"
		);

		self.last_offset = start.span.offset;

		T::relex(self, start)
	}
}

impl CompoundValue for Regex {
	const START: &'static [TokenKind] = &[t!("/")];
	// re-lexes a `/` token to a regex token.
	fn relex(lexer: &mut Lexer, _: Token) -> Result<CompoundToken<Regex>, SyntaxError> {
		loop {
			match lexer.reader.next() {
				Some(b'\\') => {
					// We can't just eat all bytes after a \ because a byte might be non-ascii.
					lexer.eat(b'/');
				}
				Some(b'/') => break,
				Some(x) => {
					if !x.is_ascii() {
						if let Err(e) = lexer.reader.complete_char(x) {
							let span = lexer.advance_span();
							bail!("Invalid token: {e}", @span);
						}
					}
				}
				None => {
					let span = lexer.advance_span();
					return Err(
						error!("Failed to lex regex, unexpected eof", @span).with_data_pending()
					);
				}
			}
		}

		// successfully parsed the regex, time to structure it.
		let span = lexer.advance_span();
		// +1 offset to move over the first `/` -2 len to remove the last `/`
		let mut inner_span = span;
		debug_assert!(inner_span.len > 2);
		inner_span.offset += 1;
		inner_span.len -= 2;

		let str = lexer.span_str(inner_span);
		let regex = str.parse().map_err(|e| error!("Invalid regex: {e}", @span))?;
		Ok(CompoundToken {
			value: regex,
			span,
		})
	}
}
