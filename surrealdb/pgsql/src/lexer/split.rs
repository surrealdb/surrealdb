//! SQL statement boundary splitting and comment preprocessor.

use crate::error::ParseResult;
use crate::lexer::cursor::Cursor;
use crate::span::Span;

/// A single SQL statement boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementBoundary<'a> {
	/// Slice of the original source text containing the statement.
	pub text: &'a str,
	/// Span of the statement in the source text (excluding trailing semicolon).
	pub span: Span,
}

/// Split an SQL script containing multiple statements separated by `;` into individual statement
/// boundaries.
///
/// Correctly ignores `;` inside strings, dollar-quoted blocks, comments, and quoted identifiers.
pub fn split_statements(src: &str) -> ParseResult<Vec<StatementBoundary<'_>>> {
	let mut statements = Vec::new();
	let mut cursor = Cursor::new(src);
	// Skip leading whitespace
	skip_whitespace(&mut cursor);
	let mut stmt_start = cursor.pos();

	while !cursor.is_eof() {
		let Some(c) = cursor.peek() else {
			break;
		};

		// Single-line comment: skip until newline
		if cursor.starts_with("--") {
			cursor.advance_bytes(2);
			while let Some(ch) = cursor.bump() {
				if ch == '\n' {
					break;
				}
			}
			continue;
		}

		// Block comment: skip with nesting
		if cursor.starts_with("/*") {
			cursor.advance_bytes(2);
			let mut nesting: u32 = 1;
			while nesting > 0 && !cursor.is_eof() {
				if cursor.starts_with("/*") {
					nesting = nesting.saturating_add(1);
					cursor.advance_bytes(2);
				} else if cursor.starts_with("*/") {
					nesting = nesting.saturating_sub(1);
					cursor.advance_bytes(2);
				} else {
					cursor.bump();
				}
			}
			continue;
		}

		// Quoted identifier: skip until closing quote, handling ""
		if c == '"' {
			cursor.bump();
			while let Some(ch) = cursor.bump() {
				if ch == '"' {
					if cursor.eat_char('"') {
						continue;
					}
					break;
				}
			}
			continue;
		}

		// Single-quoted string literal: skip until closing quote, handling ''
		if c == '\'' {
			cursor.bump();
			while let Some(ch) = cursor.bump() {
				if ch == '\'' {
					if cursor.eat_char('\'') {
						continue;
					}
					break;
				}
			}
			continue;
		}

		// Dollar-quoted string: skip until matching delimiter
		if c == '$' {
			let mark = cursor.pos();
			cursor.bump(); // consume '$'
			let tag_start = cursor.pos();
			let mut is_tag = true;

			while let Some(ch) = cursor.peek() {
				if ch == '$' {
					break;
				}
				if ch.is_ascii_alphanumeric() || ch == '_' {
					cursor.bump();
				} else {
					is_tag = false;
					break;
				}
			}

			if is_tag && cursor.eat_char('$') {
				let tag = cursor.slice_from(tag_start).unwrap_or("");
				let tag_name = tag.strip_suffix('$').unwrap_or(tag);
				let delimiter = format!("${}$", tag_name);

				while !cursor.is_eof() {
					if cursor.starts_with(&delimiter) {
						cursor.advance_bytes(delimiter.len());
						break;
					}
					cursor.bump();
				}
				continue;
			} else {
				// Reset position if not a valid dollar quote
				cursor = Cursor::new(src);
				cursor.advance_bytes((mark + 1) as usize);
				continue;
			}
		}

		// Semicolon statement terminator
		if c == ';' {
			let end_pos = cursor.pos();
			cursor.bump(); // consume ';'

			let slice = src.get(stmt_start as usize..end_pos as usize).unwrap_or("").trim();
			if !slice.is_empty() {
				statements.push(StatementBoundary {
					text: slice,
					span: Span::new(stmt_start, end_pos),
				});
			}

			skip_whitespace(&mut cursor);
			stmt_start = cursor.pos();
			continue;
		}

		cursor.bump();
	}

	// Trailing statement without semicolon
	let end_pos = cursor.pos();
	let slice = src.get(stmt_start as usize..end_pos as usize).unwrap_or("").trim();
	if !slice.is_empty() {
		statements.push(StatementBoundary {
			text: slice,
			span: Span::new(stmt_start, end_pos),
		});
	}

	Ok(statements)
}

fn skip_whitespace(cursor: &mut Cursor<'_>) {
	while let Some(c) = cursor.peek() {
		if c.is_ascii_whitespace() {
			cursor.bump();
		} else {
			break;
		}
	}
}
