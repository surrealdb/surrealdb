use token::{BaseTokenKind, T};

use super::{ParseResult, Parser};
use crate::Parse;

impl Parse for ast::Place {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.peek_span();
		let field = parser.parse_sync()?;
		let mut lhs = ast::Place::Field(field);

		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![.] => {
					let _ = parser.next();
					let name = parser.parse_sync()?;
					let new_lhs = parser.push(lhs);
					let span = parser.span_since(start);
					lhs = ast::Place::Member(ast::MemberPlace {
						lhs: new_lhs,
						name,
						span,
					})
				}
				BaseTokenKind::OpenBracket => {
					let _ = parser.next();
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
