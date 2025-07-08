use crate::syn::{
	error::{SyntaxError, bail},
	lexer::{Lexer, unicode::is_identifier_continue},
	token::{Token, TokenKind},
};
use std::mem;

pub fn flexible_ident(lexer: &mut Lexer, start: Token) -> Result<String, SyntaxError> {
	match start.kind {
		TokenKind::Digits => {
			let mut res = lexer.span_str(start.span).to_owned();
			while let Some(x) = lexer.reader.peek() {
				if is_identifier_continue(x) {
					lexer.reader.next();
					res.push(x as char);
				} else {
					break;
				}
			}
			Ok(res)
		}
		TokenKind::Identifier => Ok(mem::take(&mut lexer.string).unwrap()),
		x => bail!("Unexpected token {x}, expected flexible identifier", @start.span),
	}
}
