//! Implements token gluing logic.
use super::{GluedValue, ParseResult, Parser};
use crate::syn::lexer::compound;
use crate::syn::token::{Glued, Token, TokenKind, t};
use crate::val::{Datetime, Duration, Strand, Uuid};

impl Parser<'_> {
	/// Glues the next token and returns the token after.
	pub(super) fn glue_and_peek1(&mut self) -> ParseResult<Token> {
		let token = self.peek();
		match token.kind {
			TokenKind::Glued(_) => return Ok(self.peek1()),
			t!("+") | t!("-") => {
				let peek1 = self.peek_whitespace1();
				if !matches!(peek1.kind, TokenKind::Digits) {
					return Ok(token);
				}

				// This is a bit of an annoying special case.
				// The problem is that `+` and `-` can be a prefix operator and at the start
				// of a number token.
				// To figure out which it is we need to peek the next whitespace token,
				// This eats the digits that the lexer needs to lex the number. So we need
				// to backup before the digits token was consumed, clear the digits token from
				// the token buffer so it isn't popped after parsing the number and then lex the
				// number.
				self.lexer.backup_before(peek1.span);
				self.token_buffer.clear();
				let value = self.lexer.lex_compound(token, compound::numeric_kind)?;
				match value.value {
					compound::NumericKind::Float => {
						self.glued_value = GluedValue::Number(compound::NumberKind::Float);
						self.prepend_token(Token {
							span: value.span,
							kind: TokenKind::Glued(Glued::Number),
						});
					}
					compound::NumericKind::Int => {
						self.glued_value = GluedValue::Number(compound::NumberKind::Integer);
						self.prepend_token(Token {
							span: value.span,
							kind: TokenKind::Glued(Glued::Number),
						});
					}
					compound::NumericKind::Decimal => {
						self.glued_value = GluedValue::Number(compound::NumberKind::Decimal);
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
			TokenKind::Digits => {
				self.pop_peek();
				let value = self.lexer.lex_compound(token, compound::numeric_kind)?;
				match value.value {
					compound::NumericKind::Int => {
						self.glued_value = GluedValue::Number(compound::NumberKind::Integer);
						self.prepend_token(Token {
							span: value.span,
							kind: TokenKind::Glued(Glued::Number),
						});
					}
					compound::NumericKind::Float => {
						self.glued_value = GluedValue::Number(compound::NumberKind::Float);
						self.prepend_token(Token {
							span: value.span,
							kind: TokenKind::Glued(Glued::Number),
						});
					}
					compound::NumericKind::Decimal => {
						self.glued_value = GluedValue::Number(compound::NumberKind::Decimal);
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
				// SAFETY: Lexer ensures that no null bytes are present in the string in
				// value.value
				self.glued_value =
					GluedValue::Strand(unsafe { Strand::new_unchecked(value.value) });
				self.prepend_token(Token {
					span: value.span,
					kind: TokenKind::Glued(Glued::Strand),
				});
				return Ok(self.peek1());
			}
			t!("d\"") | t!("d'") => {
				self.pop_peek();
				let value = self.lexer.lex_compound(token, compound::datetime)?;
				self.glued_value = GluedValue::Datetime(Datetime(value.value));
				self.prepend_token(Token {
					span: value.span,
					kind: TokenKind::Glued(Glued::Datetime),
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
			t!("b\"") | t!("b'") => {
				self.pop_peek();
				let value = self.lexer.lex_compound(token, compound::bytes)?;
				self.glued_value = GluedValue::Bytes(value.value);
				self.prepend_token(Token {
					span: value.span,
					kind: TokenKind::Glued(Glued::Bytes),
				});
			}
			t!("f\"") | t!("f'") => {
				self.pop_peek();
				let value = self.lexer.lex_compound(token, compound::file)?;
				self.glued_value = GluedValue::File(value.value);
				self.prepend_token(Token {
					span: value.span,
					kind: TokenKind::Glued(Glued::File),
				});
			}
			_ => {}
		}
		Ok(self.peek1())
	}
}
