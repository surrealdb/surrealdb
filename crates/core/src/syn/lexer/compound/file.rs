use std::mem;

use crate::syn::error::{SyntaxError, bail};
use crate::syn::lexer::Lexer;
use crate::syn::token::{Token, t};
use crate::val::File;

pub fn file(lexer: &mut Lexer, start: Token) -> Result<File, SyntaxError> {
	let close_char = match start.kind {
		t!("f\"") => '"',
		t!("f'") => '\'',
		x => panic!("Invalid start token of file compound: {x}"),
	};

	let Some(bucket) = eat_segment(lexer, false)? else {
		let found = lexer.reader.peek().map(|x| format!("`{x}`")).unwrap_or("end of file".into());
		bail!("Unexpected {found}, expected a valid bucket name", @lexer.current_span());
	};

	// Expect `:/`, however `/` is part of the parsed key
	lexer.expect(':')?;
	lexer.advance_span();

	lexer.expect('/')?;
	lexer.scratch.push('/');

	let Some(key) = eat_segment(lexer, true)? else {
		let found = lexer.reader.peek().map(|x| format!("`{x}`")).unwrap_or("end of file".into());
		bail!("Unexpected {found}, expected a valid file path", @lexer.current_span());
	};

	lexer.expect(close_char)?;

	Ok(File {
		bucket,
		key,
	})
}

fn eat_segment(lexer: &mut Lexer, eat_slash: bool) -> Result<Option<String>, SyntaxError> {
	// Expect a first character, otherwise return None
	// We check this here instead of based on the lexer's scratch,
	// as the main function may have already pushed a `/` onto the scratch
	if !eat_char(lexer, eat_slash)? {
		return Ok(None);
	}

	// Keep eating characters until we're done
	while eat_char(lexer, eat_slash)? {}

	// Return the eaten segment
	lexer.advance_span();
	Ok(Some(mem::take(&mut lexer.scratch)))
}

fn eat_char(lexer: &mut Lexer, eat_slash: bool) -> Result<bool, SyntaxError> {
	let x = if let Some(peek) = lexer.reader.peek() {
		if peek.is_ascii_alphanumeric()
			|| matches!(peek, b'-' | b'_' | b'.')
			|| (eat_slash && peek == b'/')
		{
			lexer.reader.next();
			peek
		} else if peek == b'\\' {
			lexer.reader.next();
			let Some(x) = lexer.reader.next() else {
				lexer.advance_span();
				bail!("Unexpected end of file, expected escape sequence to finish",@lexer.current_span());
			};

			x
		} else {
			return Ok(false);
		}
	} else {
		return Ok(false);
	};

	lexer.scratch.push(lexer.reader.convert_to_char(x)?);
	Ok(true)
}
