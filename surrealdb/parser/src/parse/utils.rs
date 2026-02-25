use ast::{Node, NodeList, NodeListId, Spanned};
use common::span::Span;
use token::BaseTokenKind;

use super::{Parse, ParseResult, ParseSync, Parser};

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
pub async fn parse_delimited_list<R, F>(
	parser: &mut Parser<'_, '_>,
	open: BaseTokenKind,
	close: BaseTokenKind,
	seperator: BaseTokenKind,
	value: F,
) -> ParseResult<(Span, Option<NodeListId<R>>)>
where
	F: AsyncFn(&mut Parser<'_, '_>) -> ParseResult<R>,
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

pub async fn parse_seperated_list<R, F>(
	parser: &mut Parser<'_, '_>,
	seperator: BaseTokenKind,
	value: F,
) -> ParseResult<(Span, NodeListId<R>)>
where
	F: AsyncFn(&mut Parser<'_, '_>) -> ParseResult<R>,
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

pub fn parse_seperated_list_sync<R, F>(
	parser: &mut Parser<'_, '_>,
	seperator: BaseTokenKind,
	value: F,
) -> ParseResult<(Span, NodeListId<R>)>
where
	F: Fn(&mut Parser<'_, '_>) -> ParseResult<R>,
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
