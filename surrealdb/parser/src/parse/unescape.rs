use ast::NodeId;
use common::source_error::{AnnotationKind, Level, Snippet};
use common::span::Span;
use token::{BaseTokenKind, Token};

use crate::parse::{ParseError, ParseResult, Parser};

impl<'source, 'ast> Parser<'source, 'ast> {
	// Unescape an ident token and push the string value of that token into the ast.
	pub(crate) fn unescape_ident(&mut self, token: Token) -> ParseResult<NodeId<String>> {
		assert!(token.token.is_identifier());
		let slice = self.slice(token.span);
		if slice.starts_with('`') {
			self.unescape_backtick_ident(token.span, slice)
		} else if slice.starts_with('⟨') {
			self.unescape_bracket_ident(token.span, slice)
		} else {
			// Already a valid identifier.
			Ok(self.ast.push_set_entry(slice))
		}
	}

	// Unescape an param token and push the string value of that token into the ast.
	pub(crate) fn unescape_param(&mut self, token: Token) -> ParseResult<NodeId<String>> {
		assert_eq!(token.token, BaseTokenKind::Param);
		let slice = self.slice(token.span);
		if slice.starts_with("$`") {
			let mut span = token.span;
			span.start += 1;
			self.unescape_backtick_ident(span, &slice[1..])
		} else if slice.starts_with("$⟨") {
			let mut span = token.span;
			span.start += 1;
			self.unescape_bracket_ident(span, &slice[1..])
		} else {
			// Already a valid identifier.
			Ok(self.ast.push_set_entry(&slice[1..]))
		}
	}

	pub(crate) fn unescape_common<'a>(
		slice_span: Span,
		unescape_source: &'a str,
		full_source: &'a str,
		buffer: &'a mut String,
	) -> ParseResult<&'a str> {
		parse_common::unescape(unescape_source, buffer).map_err(|e| {
			let span = Span::from_usize_range(e.span).expect("Source to be shorter the u32::MAX");
			ParseError::diagnostic(
				Level::Error
					.title(e.message)
					.snippet(
						Snippet::source(full_source)
							.annotate(AnnotationKind::Primary.span(slice_span.sub_span(span))),
					)
					.to_diagnostic()
					.to_owned(),
			)
		})
	}

	fn unescape_bracket_ident<'a>(
		&'a mut self,
		mut slice_span: Span,
		slice: &'a str,
	) -> ParseResult<NodeId<String>> {
		let start_offset = const { '⟨'.len_utf8() };
		let end_offset = const { '⟩'.len_utf8() };
		let slice = &slice[start_offset..(slice.len() - end_offset)];
		slice_span.start += start_offset as u32;
		slice_span.end -= end_offset as u32;

		let str =
			Self::unescape_common(slice_span, slice, self.source(), &mut self.unescape_buffer)?;
		Ok(self.ast.push_set_entry(str))
	}

	fn unescape_backtick_ident<'a>(
		&'a mut self,
		mut slice_span: Span,
		slice: &'a str,
	) -> ParseResult<NodeId<String>> {
		let start_offset = const { '`'.len_utf8() };
		let end_offset = const { '`'.len_utf8() };
		let slice = &slice[start_offset..(slice.len() - end_offset)];
		slice_span.start += start_offset as u32;
		slice_span.end -= end_offset as u32;

		let str =
			Self::unescape_common(slice_span, slice, self.source(), &mut self.unescape_buffer)?;
		Ok(self.ast.push_set_entry(str))
	}

	/// Unescape a string-like token into the caller-provided `buffer`.
	///
	/// The returned `&str` borrows from either the original source or `buffer`,
	/// but never from `self`, so the caller is free to re-borrow `self`
	/// mutably while the returned slice is still alive.
	pub(crate) fn unescape_str<'a>(
		&self,
		token: Token,
		buffer: &'a mut String,
	) -> ParseResult<&'a str>
	where
		'source: 'a,
	{
		let start_offset = match token.token {
			BaseTokenKind::String => 1,
			BaseTokenKind::RecordIdString
			| BaseTokenKind::UuidString
			| BaseTokenKind::DateTimeString => 2,
			_ => panic!("unescape_str should only be called with string like tokens"),
		};
		let slice = self.slice(token.span);
		let mut slice_span = token.span;
		let end_offset = 1;
		let slice = &slice[start_offset..(slice.len() - end_offset)];
		slice_span.start += start_offset as u32;
		slice_span.end -= end_offset as u32;

		Self::unescape_common(slice_span, slice, self.source(), buffer)
	}

	pub(crate) fn unescape_str_push(&mut self, token: Token) -> ParseResult<NodeId<String>> {
		let start_offset = match token.token {
			BaseTokenKind::String => 1,
			BaseTokenKind::RecordIdString
			| BaseTokenKind::UuidString
			| BaseTokenKind::DateTimeString
			| BaseTokenKind::FileString => 2,
			_ => panic!("unescape_str should only be called with string like tokens"),
		};
		let slice = self.slice(token.span);
		let mut slice_span = token.span;
		let end_offset = 1;
		let slice = &slice[start_offset..(slice.len() - end_offset)];
		slice_span.start += start_offset as u32;
		slice_span.end -= end_offset as u32;

		let str =
			Self::unescape_common(slice_span, slice, self.source(), &mut self.unescape_buffer)?;
		Ok(self.ast.push_set_entry(str))
	}

	/// Returns the offset in the escaped `unescaped_str` corresponding to `offset` in the
	/// unescaped version of the string. See [`parse_common::unescaped_to_escaped_offset`].
	///
	/// # Panics
	/// This function can panic if the escaped string has invalid escape sequences inside and
	/// therefore should only be called on strings which are already verified to have correct
	/// escape sequences.
	pub(crate) fn escape_str_offset(unescaped_str: &str, offset: u32) -> u32 {
		parse_common::unescaped_to_escaped_offset(unescaped_str, offset as usize) as u32
	}
}
