use crate::{
	sql::File,
	syn::{
		error::{bail, SyntaxError},
		lexer::Lexer,
		token::{t, Token},
	},
};

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

	lexer.expect(':')?;
	lexer.expect('/')?;

	let key = if let Some(key) = eat_segment(lexer, true)? {
		format!("/{key}")
	} else {
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
	let mut segment = String::new();
	while let Some(char) = eat_char(lexer, eat_slash)? {
		segment.push(char as char);
	}

	if segment.is_empty() {
		return Ok(None);
	}

	lexer.advance_span();
	Ok(Some(segment))
}

fn eat_char(lexer: &mut Lexer, eat_slash: bool) -> Result<Option<u8>, SyntaxError> {
	if let Some(peek) = lexer.reader.peek() {
		if peek.is_ascii_alphanumeric()
			|| matches!(peek, b'-' | b'_' | b'.')
			|| (eat_slash && peek == b'/')
		{
			lexer.reader.next();
			Ok(Some(peek))
		} else if peek == b'\\' {
			lexer.reader.next();
			let Some(char) = lexer.reader.next() else {
				lexer.advance_span();
				bail!("Unexpected end of file, expected byte to finish",@lexer.current_span());
			};

			Ok(Some(char))
		} else {
			Ok(None)
		}
	} else {
		Ok(None)
	}
}
