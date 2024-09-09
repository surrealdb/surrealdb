//! Implements token gluing logic.

use crate::{
	sql::{Datetime, Duration, Strand, Uuid},
	syn::{
		lexer::compound,
		token::{t, Glued, Token, TokenKind},
	},
};

use super::{GluedValue, ParseResult, Parser};

impl Parser<'_> {
	/// Glues the next token and returns the token after.
	pub(super) fn glue_and_peek1(&mut self) -> ParseResult<Token> {
		let token = self.peek();
		match token.kind {
			TokenKind::Glued(_) => return Ok(self.peek1()),
			t!("+") | t!("-") | TokenKind::Digits => {
				self.pop_peek();
				let value = self.lexer.lex_compound(token, compound::numeric_kind)?;
				match value.value {
					compound::NumericKind::Number(x) => {
						self.glued_value = GluedValue::Number(x);
						self.prepend_token(Token {
							span: value.span,
							kind: TokenKind::Glued(Glued::Number),
						});
					}
					compound::NumericKind::Duration(x) => {
						self.glued_value = GluedValue::Duration(Duration(x));
						self.prepend_token(Token {
							span: value.span,
							kind: TokenKind::Glued(Glued::Duration),
						});
					}
				}
			}
			t!("\"") | t!("'") => {
				self.pop_peek();
				let value = self.lexer.lex_compound(token, compound::strand)?;
				self.glued_value = GluedValue::Strand(Strand(value.value));
				self.prepend_token(Token {
					span: value.span,
					kind: TokenKind::Glued(Glued::Duration),
				});
				return Ok(self.peek1());
			}
			t!("d\"") | t!("d'") => {
				self.pop_peek();
				let value = self.lexer.lex_compound(token, compound::datetime)?;
				self.glued_value = GluedValue::Datetime(Datetime(value.value));
				self.prepend_token(Token {
					span: value.span,
					kind: TokenKind::Glued(Glued::Duration),
				});
			}
			t!("u\"") | t!("u'") => {
				self.pop_peek();
				let value = self.lexer.lex_compound(token, compound::uuid)?;
				self.glued_value = GluedValue::Uuid(Uuid(value.value));
				self.prepend_token(Token {
					span: value.span,
					kind: TokenKind::Glued(Glued::Uuid),
				});
			}
			_ => {}
		}
		Ok(self.peek1())
	}
}
