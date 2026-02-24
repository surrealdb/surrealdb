use std::ops::Bound;

use ast::{NodeId, RecordIdKeyGenerate, Spanned};
use token::{BaseTokenKind, T};

use crate::parse::ParseResult;
use crate::parse::peek::peek_starts_record_id_key;
use crate::{Parse, Parser};

impl Parse for ast::RecordIdKey {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let peek = parser.peek_expect("a record id key")?;
		let key = match peek.token {
			BaseTokenKind::OpenBrace => {
				let array = parser.parse_push().await?;
				ast::RecordIdKey::Object(array)
			}
			BaseTokenKind::OpenBracket => {
				let array = parser.parse_push().await?;
				ast::RecordIdKey::Array(array)
			}
			BaseTokenKind::String => {
				let i = parser.parse_sync_push()?;
				ast::RecordIdKey::String(i)
			}
			BaseTokenKind::Int => ast::RecordIdKey::Number(parser.parse_sync_push()?),
			BaseTokenKind::UuidString => {
				let uuid = parser.parse_sync_push()?;
				ast::RecordIdKey::Uuid(uuid)
			}
			x if x.is_identifier() => {
				let _ = parser.next();
				let str = parser.unescape_ident(peek)?.to_owned();
				let text = parser.push_set(str);
				let i = parser.push(ast::StringLit {
					text,
					span: peek.span,
				});
				ast::RecordIdKey::String(i)
			}
			_ => return Err(parser.unexpected("a record id key")),
		};
		Ok(key)
	}
}

impl Parse for ast::RecordId {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.peek_span();
		let name = parser.parse_sync()?;
		let name = parser.push(name);
		let _ = parser.expect(T![:])?;

		let peek = parser.peek_expect("a record id key")?;
		let key = match peek.token {
			T![..] => {
				if let Some(T![=]) = parser.peek_joined1()?.map(|x| x.token) {
					let _ = parser.next()?;
					let _ = parser.next()?;

					let key = parser.parse_push().await?;
					let span = parser.span_since(peek.span);
					let range = ast::RecordIdKeyRange {
						start: Bound::Unbounded,
						end: Bound::Included(key),
						span,
					};
					ast::RecordIdKey::Range(parser.push(range))
				} else {
					let _ = parser.next()?;

					if peek_starts_record_id_key(parser)? {
						let key = parser.parse_push().await?;
						let span = parser.span_since(peek.span);
						let range = ast::RecordIdKeyRange {
							start: Bound::Unbounded,
							end: Bound::Excluded(key),
							span,
						};
						ast::RecordIdKey::Range(parser.push(range))
					} else {
						let range = ast::RecordIdKeyRange {
							start: Bound::Unbounded,
							end: Bound::Unbounded,
							span: peek.span,
						};
						ast::RecordIdKey::Range(parser.push(range))
					}
				}
			}
			T![RAND] => {
				let _ = parser.next();
				let _ = parser.expect(BaseTokenKind::OpenParen)?;
				let _ = parser.expect(BaseTokenKind::CloseParen)?;
				ast::RecordIdKey::Generate(Spanned {
					value: RecordIdKeyGenerate::Rand,
					span: parser.span_since(peek.span),
				})
			}
			T![UUID] => {
				let _ = parser.next();
				let _ = parser.expect(BaseTokenKind::OpenParen)?;
				let _ = parser.expect(BaseTokenKind::CloseParen)?;
				ast::RecordIdKey::Generate(Spanned {
					value: RecordIdKeyGenerate::Uuid,
					span: parser.span_since(peek.span),
				})
			}
			T![ULID] => {
				let _ = parser.next();
				let _ = parser.expect(BaseTokenKind::OpenParen)?;
				let _ = parser.expect(BaseTokenKind::CloseParen)?;
				ast::RecordIdKey::Generate(Spanned {
					value: RecordIdKeyGenerate::Ulid,
					span: parser.span_since(peek.span),
				})
			}
			_ => {
				let start: ast::RecordIdKey = parser.parse().await?;
				let peek_key = parser.peek()?;
				match peek_key.map(|x| x.token) {
					Some(T![..]) => {
						if let Some(T![=]) = parser.peek_joined1()?.map(|x| x.token) {
							let _ = parser.next();
							let _ = parser.next();

							let start = parser.push(start);

							let end = parser.parse_push().await?;

							let span = parser.span_since(peek.span);
							let range = ast::RecordIdKeyRange {
								start: Bound::Included(start),
								end: Bound::Included(end),
								span,
							};
							ast::RecordIdKey::Range(parser.push(range))
						} else {
							let _ = parser.next();

							let start = parser.push(start);

							if peek_starts_record_id_key(parser)? {
								let end = parser.parse_push().await?;
								let span = parser.span_since(peek.span);
								let range = ast::RecordIdKeyRange {
									start: Bound::Included(start),
									end: Bound::Excluded(end),
									span,
								};
								ast::RecordIdKey::Range(parser.push(range))
							} else {
								let span = parser.span_since(peek.span);
								let range = ast::RecordIdKeyRange {
									start: Bound::Included(start),
									end: Bound::Unbounded,
									span,
								};
								ast::RecordIdKey::Range(parser.push(range))
							}
						}
					}
					Some(T![>]) => {
						if !matches!(parser.peek_joined1()?.map(|x| x.token), Some(T![..])) {
							return Err(parser.unexpected("a range operator"));
						}

						if let Some(T![=]) = parser.peek_joined2()?.map(|x| x.token) {
							let _ = parser.next();
							let _ = parser.next();
							let _ = parser.next();

							let start = parser.push(start);
							let end = parser.parse_push().await?;

							let span = parser.span_since(peek.span);
							let range = ast::RecordIdKeyRange {
								start: Bound::Excluded(start),
								end: Bound::Included(end),
								span,
							};
							ast::RecordIdKey::Range(parser.push(range))
						} else {
							let _ = parser.next();
							let _ = parser.next();

							let start = parser.push(start);
							if peek_starts_record_id_key(parser)? {
								let end = parser.parse_push().await?;

								let span = parser.span_since(peek.span);
								let range = ast::RecordIdKeyRange {
									start: Bound::Excluded(start),
									end: Bound::Excluded(end),
									span,
								};
								ast::RecordIdKey::Range(parser.push(range))
							} else {
								let span = parser.span_since(peek.span);
								let range = ast::RecordIdKeyRange {
									start: Bound::Excluded(start),
									end: Bound::Unbounded,
									span,
								};
								ast::RecordIdKey::Range(parser.push(range))
							}
						}
					}
					_ => start,
				}
			}
		};
		let key = parser.push(key);

		Ok(ast::RecordId {
			name,
			key,
			span: parser.span_since(start),
		})
	}
}
