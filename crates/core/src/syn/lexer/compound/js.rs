use super::Lexer;
use crate::syn::error::{MessageKind, SyntaxError};
use crate::syn::lexer::unicode::chars::JS_LINE_TERIMATORS;
use crate::syn::token::{Token, t};

pub fn javascript(lexer: &mut Lexer, start: Token) -> Result<(), SyntaxError> {
	assert_eq!(start.kind, t!("{"), "Invalid start of JavaScript compound token");
	lex_js_function_body_inner(lexer)?;
	Ok(())
}

/// Lex the body of a js function.
fn lex_js_function_body_inner(lexer: &mut Lexer) -> Result<(), SyntaxError> {
	let mut block_depth = 1;
	loop {
		let Some(byte) = lexer.reader.next() else {
			let span = lexer.advance_span();
			return Err(SyntaxError::new(format_args!(
				"Invalid JavaScript function, encountered unexpected eof"
			))
			.with_span(span, MessageKind::Error)
			.with_data_pending());
		};
		match byte {
			b'`' => lex_js_string(lexer, b'`')?,
			b'\'' => lex_js_string(lexer, b'\'')?,
			b'\"' => lex_js_string(lexer, b'\"')?,
			b'/' => match lexer.reader.peek() {
				Some(b'/') => {
					lexer.reader.next();
					lex_js_single_comment(lexer)?;
				}
				Some(b'*') => {
					lexer.reader.next();
					lex_js_multi_comment(lexer)?
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
				lexer.reader.complete_char(x)?;
			}
			_ => {}
		}
	}

	Ok(())
}

/// lex a js string with the given delimiter.
fn lex_js_string(lexer: &mut Lexer, enclosing_byte: u8) -> Result<(), SyntaxError> {
	loop {
		let Some(byte) = lexer.reader.next() else {
			let span = lexer.advance_span();
			return Err(SyntaxError::new(format_args!(
				"Invalid JavaScript function, encountered unexpected eof"
			))
			.with_span(span, MessageKind::Error)
			.with_data_pending());
		};
		if byte == enclosing_byte {
			return Ok(());
		}
		if byte == b'\\' {
			lexer.reader.next();
		}
		// check for invalid characters.
		lexer.reader.convert_to_char(byte)?;
	}
}

/// lex a single line js comment.
fn lex_js_single_comment(lexer: &mut Lexer) -> Result<(), SyntaxError> {
	loop {
		let Some(byte) = lexer.reader.next() else {
			return Ok(());
		};
		let char = lexer.reader.convert_to_char(byte)?;
		if JS_LINE_TERIMATORS.contains(&char) {
			return Ok(());
		}
	}
}

/// lex a multi line js comment.
fn lex_js_multi_comment(lexer: &mut Lexer) -> Result<(), SyntaxError> {
	loop {
		let Some(byte) = lexer.reader.next() else {
			let span = lexer.advance_span();
			return Err(SyntaxError::new(format_args!(
				"Invalid JavaScript function, encountered unexpected eof"
			))
			.with_span(span, MessageKind::Error)
			.with_data_pending());
		};
		if byte == b'*' && lexer.reader.peek() == Some(b'/') {
			lexer.reader.next();
			return Ok(());
		}
		// check for invalid characters.
		lexer.reader.convert_to_char(byte)?;
	}
}
