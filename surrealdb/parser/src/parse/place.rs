use ast::{Ident, MemberPlace, NodeId};
use common::span::Span;
use token::{BaseTokenKind, T};

use super::{ParseResult, Parser};
use crate::Parse;
use crate::parse::ParserSettings;

enum GraphStart {
	Reference,
	In,
	Out,
	Both,
}

async fn parse_graph_name(
	parser: &mut Parser<'_, '_>,
	start: Span,
	dir: GraphStart,
) -> ParseResult<NodeId<Ident>> {
	let lookup: ast::Lookup = parser.parse_enter().await?;

	// `<-`, `<->` just behaves as if `<-` is part of the field name.
	if let ast::Lookup::Subject(s) = lookup
		&& let ast::LookupSubject::Table(t) = &parser[s]
		&& t.field.is_none()
	{
		let name = t.table;
		parser.unescape_buffer.clear();
		match dir {
			GraphStart::Reference => parser.unescape_buffer.push_str("<~"),
			GraphStart::In => parser.unescape_buffer.push_str("->"),
			GraphStart::Out => parser.unescape_buffer.push_str("<-"),
			GraphStart::Both => parser.unescape_buffer.push_str("<->"),
		}

		parser.unescape_buffer.push_str(parser.ast[name].text.index(parser.ast));
		let text = parser.ast.push_set_entry(parser.unescape_buffer.as_str());
		let span = parser.span_since(start);
		Ok(parser.push(Ident {
			text,
			span,
		}))
	} else {
		// NOTE: Not completely correct,
		// with the old parser this path would be parsed completely and then formatted back to
		// surrealql, stripping comments and whitespace. However I think it is really unlikely
		// that anyone actually relies on this behavior.
		let span = parser.span_since(start);
		let s = parser.slice(span);
		let text = parser.push_set_entry(s);
		let span = parser.span_since(start);
		Ok(parser.push(Ident {
			text,
			span,
		}))
	}
}

impl Parse for ast::Place {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.peek_span();

		let legacy = parser.settings.contains(ParserSettings::QUIRK_LEGACY_PLACE_PRODUCTIONS);

		let mut lhs;
		if legacy && let Some(peek) = parser.peek()? {
			match peek.token {
				T![<] => match parser.peek_joined1()?.map(|x| x.token) {
					Some(T![-]) => {
						let _ = parser.next();
						let _ = parser.next();

						let _ = parser.next();
						let text = parse_graph_name(parser, peek.span, GraphStart::Out).await?;
						lhs = ast::Place::Field(text)
					}
					Some(T![->]) => {
						let _ = parser.next();
						let _ = parser.next();

						let _ = parser.next();
						let text = parse_graph_name(parser, peek.span, GraphStart::In).await?;
						lhs = ast::Place::Field(text)
					}
					_ => {
						let field = parser.parse_sync()?;
						lhs = ast::Place::Field(field);
					}
				},
				T![->] => {
					let _ = parser.next();
					let text = parse_graph_name(parser, peek.span, GraphStart::In).await?;
					lhs = ast::Place::Field(text)
				}
				_ => {
					let field = parser.parse_sync()?;
					lhs = ast::Place::Field(field);
				}
			}
		} else {
			let field = parser.parse_sync()?;
			lhs = ast::Place::Field(field);
		}

		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![<~] if legacy => {
					let _ = parser.next();
					let name = parse_graph_name(parser, peek.span, GraphStart::Reference).await?;
					let new_lhs = parser.push(lhs);
					lhs = ast::Place::Member(MemberPlace {
						lhs: new_lhs,
						name,
						span: parser.span_since(peek.span),
					});
				}
				T![<] if legacy => match parser.peek_joined1()?.map(|x| x.token) {
					Some(T![-]) => {
						let _ = parser.next();
						let _ = parser.next();

						let name = parse_graph_name(parser, peek.span, GraphStart::Out).await?;
						let new_lhs = parser.push(lhs);
						lhs = ast::Place::Member(MemberPlace {
							lhs: new_lhs,
							name,
							span: parser.span_since(peek.span),
						});
					}
					Some(T![->]) => {
						let _ = parser.next();
						let _ = parser.next();

						let name = parse_graph_name(parser, peek.span, GraphStart::Both).await?;
						let new_lhs = parser.push(lhs);
						lhs = ast::Place::Member(MemberPlace {
							lhs: new_lhs,
							name,
							span: parser.span_since(peek.span),
						});
					}
					_ => break,
				},
				T![->] if legacy => {
					let _ = parser.next();
					let name = parse_graph_name(parser, peek.span, GraphStart::In).await?;
					let new_lhs = parser.push(lhs);
					lhs = ast::Place::Member(MemberPlace {
						lhs: new_lhs,
						name,
						span: parser.span_since(peek.span),
					});
				}
				T![.] => {
					let _ = parser.next();

					if legacy && let Some(_) = parser.eat(BaseTokenKind::OpenBrace)? {
						loop {
							if parser.eat(BaseTokenKind::CloseBrace)?.is_some() {
								break;
							}

							let _ = parser.parse_enter::<ast::Destructure>().await?;

							if parser.eat(T![,])?.is_none() {
								let _ = parser.expect_closing_delimiter(
									BaseTokenKind::CloseBrace,
									peek.span,
								)?;
								break;
							}
						}

						let span = parser.span_since(start);
						lhs = ast::Place::Legacy(span);
						continue;
					}

					if legacy
						&& (parser.eat_joined(T![@])?.is_some()
							|| parser.eat_joined(T![*])?.is_some()
							|| parser.eat_joined(T![?])?.is_some())
					{
						let span = parser.span_since(start);
						lhs = ast::Place::Legacy(span);
						continue;
					}

					let name = parser.parse_sync()?;
					let new_lhs = parser.push(lhs);
					let span = parser.span_since(start);
					lhs = ast::Place::Member(ast::MemberPlace {
						lhs: new_lhs,
						name,
						span,
					})
				}
				T![...] if legacy => {
					let _ = parser.next();

					let span = parser.span_since(start);
					lhs = ast::Place::Legacy(span)
				}
				BaseTokenKind::OpenBracket => {
					let _ = parser.next();

					if legacy && (parser.eat(T![WHERE])?.is_some() || parser.eat(T![?])?.is_some())
					{
						let _: ast::Expr = parser.parse_enter().await?;

						let _ = parser
							.expect_closing_delimiter(BaseTokenKind::CloseBracket, peek.span)?;

						let span = parser.span_since(start);
						lhs = ast::Place::Legacy(span);
						continue;
					}

					let index = parser.parse_enter().await?;
					let _ =
						parser.expect_closing_delimiter(BaseTokenKind::CloseBracket, peek.span)?;
					let new_lhs = parser.push(lhs);
					let span = parser.span_since(start);
					lhs = ast::Place::Index(ast::IndexPlace {
						lhs: new_lhs,
						index,
						span,
					})
				}
				_ => break,
			}
		}
		Ok(lhs)
	}
}

impl Parse for ast::PresentPlace {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.peek_span();

		let field = parser.parse_sync()?;
		let mut lhs = ast::PresentPlace::Field(field);

		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![...] => {}
				T![.] => {
					let _ = parser.next();
					let new_lhs = parser.push(lhs);

					let peek_op = parser.peek_expect("`*` or an identifier")?;
					match peek_op.token {
						T![*] => {
							let span = parser.span_since(start);
							lhs = ast::PresentPlace::All(ast::AllPresentPlace {
								lhs: new_lhs,
								span,
							});
						}
						x if x.is_identifier() => {
							let name = parser.parse_sync()?;
							let span = parser.span_since(start);
							lhs = ast::PresentPlace::Member(ast::MemberPresentPlace {
								lhs: new_lhs,
								name,
								span,
							})
						}
						_ => return Err(parser.unexpected("`*` or an identifier")),
					}
				}
				BaseTokenKind::OpenBracket => {
					let _ = parser.next();
					let new_lhs = parser.push(lhs);

					let peek_op = parser.peek_expect("`*`, `$`, or an identifier")?;
					match peek_op.token {
						T![*] => {
							let _ = parser
								.expect_closing_delimiter(BaseTokenKind::CloseBracket, peek.span)?;

							let span = parser.span_since(start);
							lhs = ast::PresentPlace::All(ast::AllPresentPlace {
								lhs: new_lhs,
								span,
							});
						}
						T![$] => {
							let _ = parser
								.expect_closing_delimiter(BaseTokenKind::CloseBracket, peek.span)?;
							let span = parser.span_since(start);
							lhs = ast::PresentPlace::Last(ast::LastPresentPlace {
								lhs: new_lhs,
								span,
							});
						}
						_ => {
							let index = parser.parse_enter().await?;
							let _ = parser
								.expect_closing_delimiter(BaseTokenKind::CloseBracket, peek.span)?;
							let span = parser.span_since(start);
							lhs = ast::PresentPlace::Index(ast::IndexPresentPlace {
								lhs: new_lhs,
								index,
								span,
							});
						}
					}
				}
				_ => break,
			}
		}
		Ok(lhs)
	}
}
