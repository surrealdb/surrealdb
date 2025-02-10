use reblessive::Stack;

use crate::api::path::{Segment, MAX_PATH_SEGMENTS};
use crate::sql::Kind;
use crate::syn::lexer::bail;
use crate::syn::parser::Parser;
use crate::syn::{
	error::SyntaxError,
	lexer::Lexer,
	token::{t, Token},
};

pub fn path(lexer: &mut Lexer, start: Token) -> Result<Vec<Segment>, SyntaxError> {
	if !matches!(start.kind, t!("/")) {
		bail!("Invalid start of strand compound token");
	};

	let mut segments: Vec<Segment> = vec![];

	loop {
		lexer.scratch.clear();

		let mut kind: Option<Kind> = None;

		'segment: loop {
			let Some(x) = lexer.reader.peek() else {
				break 'segment;
			};

			match x {
				b'/' if lexer.scratch.is_empty() => {
					lexer.reader.advance(1);
					continue 'segment;
				}

				// We allow the first character to be an escape character to ignore potential otherwise instruction characters
				b'\\' if lexer.scratch.is_empty() => {
					lexer.reader.advance(1);
					if let Some(x @ b':' | x @ b'*') = lexer.reader.peek() {
						lexer.reader.advance(1);
						lexer.scratch.push(b'\\' as char);
						lexer.scratch.push(x as char);
						continue 'segment;
					} else {
						bail!("Expected an instruction symbol `:` or `*` to follow", @lexer.current_span());
					}
				}

				// Valid segment characters
				x if x.is_ascii_alphanumeric() => (),
				b'.' | b'-' | b'_' | b'~' | b'!' | b'$' | b'&' | b'\'' | b'(' | b')' | b'*'
				| b'+' | b',' | b';' | b'=' | b':' | b'@' => (),

				// We found a kind
				b'<' if lexer.scratch.starts_with(':') => {
					if lexer.scratch.len() == 1 {
						bail!("Expected a name or content for this segment", @lexer.current_span());
					}

					lexer.reader.advance(1);
					let mut parser = Parser::new(lexer.reader.remaining());
					let mut stack = Stack::new();
					let span = parser.last_span();
					let res = stack.enter(|stk| parser.parse_kind(stk, span)).finish().map_err(
						|mut e| {
							e.advance_span_offset(lexer.reader.offset());
							e
						},
					)?;

					kind = Some(res);
					lexer.reader.advance(parser.last_span().offset as usize + 1);
					break 'segment;
				}

				// We did not encounter a valid character
				_ => {
					break 'segment;
				}
			}

			// Persist the character
			lexer.reader.advance(1);
			lexer.scratch.push(x as char);
		}

		let (segment, done) = if lexer.scratch.is_empty() {
			lexer.advance_span();
			break;
		} else if (lexer.scratch.starts_with(':')
			|| lexer.scratch.starts_with('*')
			|| lexer.scratch.starts_with('\\'))
			&& lexer.scratch[1..].is_empty()
		{
			// We encountered a segment which starts with an instruction, but is empty
			// Let's error
			bail!("Expected a name or content for this segment", @lexer.current_span());
		} else if lexer.scratch.starts_with(':') {
			let segment = Segment::Dynamic(lexer.scratch[1..].to_string(), kind);
			(segment, false)
		} else if lexer.scratch.starts_with('*') {
			let segment = Segment::Rest(lexer.scratch[1..].to_string());
			(segment, true)
		} else if lexer.scratch.starts_with('\\') {
			let segment = Segment::Fixed(lexer.scratch[1..].to_string());
			(segment, false)
		} else {
			let segment = Segment::Fixed(lexer.scratch.to_string());
			(segment, false)
		};

		segments.push(segment);
		lexer.advance_span();

		if done {
			break;
		}
	}

	if segments.len() > MAX_PATH_SEGMENTS as usize {
		bail!("Path cannot have more than {MAX_PATH_SEGMENTS} segments", @lexer.current_span());
	}

	Ok(segments)
}
