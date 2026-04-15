use ast::{Node, NodeList, NodeListId, Spanned};
use common::source_error::{AnnotationKind, Level};
use common::span::Span;
use token::BaseTokenKind;

use super::{Parse, ParseResult, ParseSync, Parser};
use crate::parse::ParseError;

impl<T: ParseSync> ParseSync for Spanned<T> {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let start = if let Some(x) = parser.peek()? {
			x.span
		} else {
			parser.eof_span()
		};

		let value = T::parse_sync(parser)?;

		let end = if let Some(x) = parser.peek()? {
			x.span
		} else {
			parser.eof_span()
		};
		Ok(Spanned {
			value,
			span: start.extend(end),
		})
	}
}

impl<T: Parse> Parse for Spanned<T> {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = if let Some(x) = parser.peek()? {
			x.span
		} else {
			parser.eof_span()
		};

		let value = T::parse(parser).await?;

		let end = if let Some(x) = parser.peek()? {
			x.span
		} else {
			parser.eof_span()
		};
		Ok(Spanned {
			value,
			span: start.extend(end),
		})
	}
}

/// Parses a delimited list starting with the open token and ending with the close token, each
/// entry seperated by seperator and allowing a trailing seperator.
///
/// Returns a list of items parsed as well as the span for the whole delimited list including
/// delimiters.
pub async fn parse_delimited_list<'src, 'ast, R, F>(
	parser: &mut Parser<'src, 'ast>,
	open: BaseTokenKind,
	close: BaseTokenKind,
	seperator: BaseTokenKind,
	mut value: F,
) -> ParseResult<(Span, Option<NodeListId<R>>)>
where
	F: AsyncFnMut(&mut Parser<'src, 'ast>) -> ParseResult<R>,
	R: Node,
{
	let start = parser.expect(open)?;

	let mut head = None;
	let mut tail = None;

	loop {
		if parser.eat(close)?.is_some() {
			break;
		}

		let v = value(parser).await?;
		parser.push_list(v, &mut head, &mut tail);

		if parser.eat(seperator)?.is_none() {
			let _ = parser.expect_closing_delimiter(close, start.span)?;
			break;
		}
	}

	let span = parser.span_since(start.span);
	Ok((span, head))
}

/// Parses a list of items without delimiters seperated by a seperator like `a, b` in
/// `SELECT a,b FROM table`.
pub async fn parse_seperated_list<'src, 'ast, R, F>(
	parser: &mut Parser<'src, 'ast>,
	seperator: BaseTokenKind,
	value: F,
) -> ParseResult<(Span, NodeListId<R>)>
where
	F: AsyncFn(&mut Parser<'src, 'ast>) -> ParseResult<R>,
	R: Node,
{
	let span = parser.peek_span();

	let start = value(parser).await?;
	let start = parser.push(start);
	let start = parser.push_list_item(NodeList {
		cur: start,
		next: None,
	});
	let mut tail = start;
	loop {
		if parser.eat(seperator)?.is_none() {
			break;
		}

		let next = value(parser).await?;
		let next = parser.push(next);
		let next = parser.push_list_item(NodeList {
			cur: next,
			next: None,
		});

		parser[tail].next = Some(next);
		tail = next;
	}

	let span = parser.span_since(span);
	Ok((span, start))
}

/// Parses a list of items without delimiters seperated by a seperator like `a, b` in
/// `SELECT a,b FROM table`.
///
/// This function is the same as [`parse_seperated_list`] except that it does not allow
/// parsing which requires a future to complete.
pub fn parse_seperated_list_sync<'src, 'ast, R, F>(
	parser: &mut Parser<'src, 'ast>,
	seperator: BaseTokenKind,
	value: F,
) -> ParseResult<(Span, NodeListId<R>)>
where
	F: Fn(&mut Parser<'src, 'ast>) -> ParseResult<R>,
	R: Node,
{
	let span = parser.peek_span();

	let start = value(parser)?;
	let start = parser.push(start);
	let start = parser.push_list_item(NodeList {
		cur: start,
		next: None,
	});
	let mut tail = start;
	loop {
		if parser.eat(seperator)?.is_none() {
			break;
		}

		let next = value(parser)?;
		let next = parser.push(next);
		let next = parser.push_list_item(NodeList {
			cur: next,
			next: None,
		});

		parser[tail].next = Some(next);
		tail = next;
	}

	let span = parser.span_since(span);
	Ok((span, start))
}

/// Generates an error for when clauses are used more then once.
#[cold]
pub fn redefined_error(parser: &mut Parser<'_, '_>, start: Span, last_span: Span) -> ParseError {
	parser.with_error(|parser| {
		Level::Error
			.title(format!("`{}` clause defined more then once", parser.slice(start)))
			.snippet(
				parser
					.snippet()
					.annotate(AnnotationKind::Primary.span(start))
					.annotate(AnnotationKind::Context.span(last_span).label("First used here")),
			)
			.to_diagnostic()
	})
}

/// Utility function implementing some re-used code,
///
/// Will parse with the given callback and store the result in the given mutable reference to an
/// option. If the option was already set it will instead throw an error.
pub async fn parse_unordered_clause<'src, 'ast, T, F>(
	parser: &mut Parser<'src, 'ast>,
	store: &mut Option<(T, Span)>,
	start: Span,
	f: F,
) -> ParseResult<()>
where
	F: AsyncFnOnce(&mut Parser<'src, 'ast>) -> ParseResult<T>,
{
	if let Some((_, last_span)) = store {
		return Err(redefined_error(parser, start, *last_span));
	}

	let res = f(parser).await?;
	let span = parser.span_since(start);
	*store = Some((res, span));

	Ok(())
}

/// Utility function implementing some re-used code,
///
/// Will parse with the given callback and store the result in the given mutable reference to an
/// option. If the option was already set it will instead throw an error.
pub fn parse_unordered_clause_sync<'src, 'ast, T, F>(
	parser: &mut Parser<'src, 'ast>,
	store: &mut Option<(T, Span)>,
	start: Span,
	f: F,
) -> ParseResult<()>
where
	F: FnOnce(&mut Parser<'src, 'ast>) -> ParseResult<T>,
{
	if let Some((_, last_span)) = store {
		return Err(redefined_error(parser, start, *last_span));
	}

	let res = f(parser)?;
	let span = parser.span_since(start);
	*store = Some((res, span));

	Ok(())
}
