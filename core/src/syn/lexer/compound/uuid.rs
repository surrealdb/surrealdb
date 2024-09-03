use crate::sql::Uuid;
use crate::syn::error::MessageKind;
use crate::syn::lexer::compound::CompoundValue;
use crate::syn::{
	error::{bail, error, SyntaxError},
	lexer::Lexer,
	token::{t, CompoundToken, Token, TokenKind},
};

impl CompoundValue for Uuid {
	const START: &'static [TokenKind] = &[t!("u\""), t!("u'")];

	fn relex(lexer: &mut Lexer, start_token: Token) -> Result<CompoundToken<Uuid>, SyntaxError> {
		let quote = match start_token.kind {
			t!("u\"") => b'"',
			t!("u'") => b'\'',
			_ => unreachable!(), // relex can be only called if START contains the start token.
		};

		for i in 0..36 {
			match lexer.reader.next() {
				Some(b'-') if i == 8 || i == 13 || i == 18 || i == 23 => {}
				Some(x) if i == 8 || i == 13 || i == 18 || i == 23 => {
					let char = lexer.reader.convert_to_char(x)?;
					let span = lexer.advance_span();
					bail!("Invalid UUID, found `{char}` but expected `-`", @span);
				}
				Some(b'"') | Some(b'\'') => {
					let span = lexer.advance_span();
					bail!("Unexpected end of UUID, expected to have 36 characters", @span);
				}
				Some(x) => {
					if !x.is_ascii_hexdigit() {
						let span = lexer.advance_span();
						bail!("Unexpected characters in UUID token, expected hex digits", @span);
					}
				}
				None => {
					let span = lexer.advance_span();
					return Err(
						error!("Failed to lex UUID, unexpected eof", @span).with_data_pending()
					);
				}
			}
		}

		let Some(closing) = lexer.reader.next() else {
			let span = lexer.advance_span();
			return Err(SyntaxError::new(format_args!("Invalid UUID, encountered unexpected eof"))
				.with_span(span, MessageKind::Error)
				.with_data_pending());
		};

		if closing != quote {
			let span = lexer.advance_span();
			let char = lexer.reader.convert_to_char(closing)?;
			bail!("UUID should end with `{}` but found `{char}`", quote as char, @span);
		}

		let mut span = lexer.advance_span();

		// remove prefix (u") and suffix (")
		debug_assert!(span.len > 3);
		span.offset += 2;
		span.len -= 3;

		let bytes = lexer.span_bytes(span);
		let uuid = uuid::Uuid::try_parse_ascii(bytes)
			.map(Uuid)
			.map_err(|e| error!("Invalid UUID: {e}", @span))?;

		Ok(CompoundToken {
			value: uuid,
			span,
		})
	}
}
