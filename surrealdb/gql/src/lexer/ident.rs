//! Lexing of regular identifiers, keywords and parameter references.

use unicase::UniCase;

use crate::gql::lexer::{Lexer, keywords};
use crate::gql::token::{Token, TokenKind};
use crate::syn::error::syntax_error;

/// Returns if the character can start a regular identifier.
///
/// `IDENTIFIER_START : ID_Start | Pc` (GQL.g4:3612-3615). The classification
/// approximates Unicode `ID_Start` with [`char::is_alphabetic`] plus the `Pc`
/// (connector punctuation) category; see the deviation note on
/// [`is_ident_continue`].
pub(super) fn is_ident_start(char: char) -> bool {
	if char.is_ascii() {
		return matches!(char, 'a'..='z' | 'A'..='Z' | '_');
	}
	char.is_alphabetic() || is_connector_punctuation(char)
}

/// Returns if the character can continue a regular identifier (and form an
/// `EXTENDED_IDENTIFIER`, i.e. an unquoted parameter name).
///
/// `IDENTIFIER_EXTEND : ID_Continue` (GQL.g4:3617-3619).
///
/// DEVIATION: the crate has no Unicode identifier tables, so `ID_Continue` is
/// approximated with [`char::is_alphanumeric`] plus connector punctuation.
/// Compared to the exact property this accepts a few extra numeric characters
/// (`No` like `²`) and misses non-alphabetic combining marks (`Mn`/`Mc` like
/// U+0301) and the small `Other_ID_Start`/`Other_ID_Continue` sets. Delimited
/// identifiers (`"…"`, `` `…` ``) cover any affected name.
pub(super) fn is_ident_continue(char: char) -> bool {
	if char.is_ascii() {
		return matches!(char, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_');
	}
	char.is_alphanumeric() || is_connector_punctuation(char)
}

/// Returns if the character is in the Unicode `Pc` (connector punctuation)
/// category. The category is small and stable, so it is listed exhaustively.
fn is_connector_punctuation(char: char) -> bool {
	matches!(
		char,
		'\u{005F}' | '\u{203F}' | '\u{2040}' | '\u{2054}' | '\u{FE33}' | '\u{FE34}' | '\u{FE4D}'
			..='\u{FE4F}' | '\u{FF3F}'
	)
}

impl Lexer<'_> {
	/// Lex a regular identifier or keyword. The starting character must
	/// already be consumed and have been a valid identifier start character.
	///
	/// Keyword lookup is case-insensitive. All three keyword classes lex to
	/// [`TokenKind::Keyword`]; the parser converts non-reserved keywords back
	/// to identifiers wherever the grammar requires a `regularIdentifier`.
	/// The original text can always be recovered from the token span.
	pub(super) fn lex_ident(&mut self) -> Token {
		self.eat_ident_continue();
		let str = self.span_str(self.current_span());
		if let Some(keyword) = keywords::KEYWORDS.get(&UniCase::ascii(str)) {
			return self.finish_token(TokenKind::Keyword(*keyword));
		}
		self.finish_token(TokenKind::Identifier)
	}

	/// Lex a parameter reference. The starting `$` must already be consumed.
	///
	/// `GENERAL_PARAMETER_REFERENCE : DOLLAR_SIGN PARAMETER_NAME` and
	/// `SUBSTITUTED_PARAMETER_REFERENCE : DOUBLE_DOLLAR_SIGN PARAMETER_NAME`
	/// (GQL.g4:3604-3610). The name is a `SEPARATED_IDENTIFIER`: either an
	/// `EXTENDED_IDENTIFIER` (one or more identifier-continue characters, so
	/// it may start with a digit) or a delimited identifier (double- or
	/// accent-quoted, optionally with the `@` no-escape prefix).
	pub(super) fn lex_param(&mut self) -> Token {
		let kind = if self.eat(b'$') {
			TokenKind::SubstitutedParameter
		} else {
			TokenKind::Parameter
		};
		match self.reader.peek() {
			Some(quote @ (b'"' | b'`')) => {
				self.reader.next();
				if let Err(e) = self.lex_quoted(quote, false) {
					return self.invalid_token(e);
				}
			}
			Some(b'@') => {
				if let Some(quote @ (b'"' | b'`')) = self.reader.peek1() {
					self.reader.next();
					self.reader.next();
					if let Err(e) = self.lex_quoted(quote, true) {
						return self.invalid_token(e);
					}
				} else {
					let error = syntax_error!(
						"Unexpected token `{}`, expected a parameter name",
						self.span_str(self.current_span()),
						@self.current_span() => "Parameter names are identifier characters or a `\"…\"`/`` `…` `` delimited identifier"
					);
					return self.invalid_token(error);
				}
			}
			_ => {
				if !self.eat_ident_continue() {
					let error = syntax_error!(
						"Unexpected token `{}`, expected a parameter name",
						self.span_str(self.current_span()),
						@self.current_span() => "Parameter names are identifier characters or a `\"…\"`/`` `…` `` delimited identifier"
					);
					return self.invalid_token(error);
				}
			}
		}
		self.finish_token(kind)
	}

	/// Eat a run of identifier-continue characters, returning whether at
	/// least one character was consumed.
	fn eat_ident_continue(&mut self) -> bool {
		let start = self.reader.offset();
		while let Some(byte) = self.reader.peek() {
			if byte.is_ascii() {
				if !is_ident_continue(byte as char) {
					break;
				}
				self.reader.next();
				continue;
			}
			let backup = self.reader.offset();
			self.reader.next();
			match self.reader.complete_char(byte) {
				Ok(char) if is_ident_continue(char) => {}
				// The source is a string slice so the error case cannot
				// occur; either way the character does not belong to the
				// identifier.
				_ => {
					self.reader.backup(backup);
					break;
				}
			}
		}
		self.reader.offset() != start
	}
}
