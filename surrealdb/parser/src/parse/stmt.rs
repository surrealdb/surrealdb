use ast::{Base, Expr};
use token::{BaseTokenKind, T};

use super::Parser;
use crate::Parse;
use crate::parse::ParseResult;
use crate::parse::utils::{parse_seperated_list, parse_seperated_list_sync};

impl Parse for ast::If {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![IF])?;
		let condition = parser.parse_enter_push().await?;

		if parser.eat(T![THEN])?.is_some() {
			let then = parser.parse_enter_push().await?;

			let otherwise = if parser.eat(T![ELSE])?.is_some() {
				let peek = parser.peek_expect("a `else` body")?;
				if let T![IF] = peek.token {
					let otherwise = parser.parse_enter_push().await?;
					Some(parser.push(Expr::If(otherwise)))
				} else {
					let res = parser.parse_enter_push().await?;
					let _ = parser.expect(T![END])?;
					Some(res)
				}
			} else {
				let _ = parser.expect(T![END])?;
				None
			};

			let span = parser.span_since(start.span);
			Ok(ast::If {
				condition,
				then,
				otherwise,
				span,
			})
		} else {
			let then = parser.parse_push().await?;
			let then = parser.push(Expr::Block(then));

			let otherwise = if parser.eat(T![ELSE])?.is_some() {
				let peek = parser.peek_expect("a `else` body")?;
				if let T![IF] = peek.token {
					let otherwise = parser.parse_enter_push().await?;
					Some(parser.push(Expr::If(otherwise)))
				} else {
					let block = parser.parse_push().await?;
					Some(parser.push(Expr::Block(block)))
				}
			} else {
				None
			};

			let span = parser.span_since(start.span);
			Ok(ast::If {
				condition,
				then,
				otherwise,
				span,
			})
		}
	}
}

impl Parse for ast::Let {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![LET])?;

		let param = parser.parse_sync_push()?;

		let ty = if parser.eat(T![:])?.is_some() {
			Some(parser.parse_push().await?)
		} else {
			None
		};

		let _ = parser.expect(T![=])?;

		let expr = parser.parse_enter_push().await?;
		let span = parser.span_since(start.span);

		Ok(ast::Let {
			param,
			ty,
			expr,
			span,
		})
	}
}

impl Parse for ast::Info {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![INFO])?;
		let _ = parser.expect(T![FOR])?;

		let peek = parser.peek_expect("a resource to get information for")?;
		let kind = match peek.token {
			T![ROOT] => {
				let _ = parser.next();
				ast::InfoKind::Root
			}
			T![NAMESPACE] => {
				let _ = parser.next();
				ast::InfoKind::Namespace
			}
			T![DATABASE] => {
				let _ = parser.next();
				let version = if parser.eat(T![VERSION])?.is_some() {
					Some(parser.parse_enter_push().await?)
				} else {
					None
				};
				ast::InfoKind::Database {
					version,
				}
			}
			T![TABLE] => {
				let _ = parser.next();
				let name = parser.parse_enter_push().await?;
				let version = if parser.eat(T![VERSION])?.is_some() {
					Some(parser.parse_enter_push().await?)
				} else {
					None
				};
				ast::InfoKind::Table {
					name,
					version,
				}
			}
			T![USER] => {
				let _ = parser.next();
				let name = parser.parse_enter_push().await?;

				let base = if parser.eat(T![ON])?.is_some() {
					let peek = parser.peek_expect("`NAMESPACE`, `DATABASE`, or `ROOT`")?;
					let base = match peek.token {
						T![NAMESPACE] => Base::Namespace,
						T![DATABASE] => Base::Database,
						T![ROOT] => Base::Root,
						_ => return Err(parser.unexpected("`NAMESPACE`, `DATABASE`, or `ROOT`")),
					};
					let _ = parser.next();
					Some(base)
				} else {
					None
				};
				ast::InfoKind::User {
					name,
					base,
				}
			}
			T![INDEX] => {
				let _ = parser.next();
				let name = parser.parse_enter_push().await?;
				let _ = parser.expect(T![ON])?;
				parser.eat(T![TABLE])?;
				let table = parser.parse_enter_push().await?;
				ast::InfoKind::Index {
					name,
					table,
				}
			}
			_ => return Err(parser.unexpected("")),
		};

		let structure = parser.eat(T![STRUCTURE])?.is_some();

		Ok(ast::Info {
			kind,

			span: parser.span_since(start.span),
			structure,
		})
	}
}

impl Parse for ast::Show {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![SHOW])?;

		let _ = parser.expect(T![CHANGES])?;
		let _ = parser.expect(T![FOR])?;

		let peek = parser.peek_expect("keyword `TABLE` or `DATABASE`")?;
		let target = match peek.token {
			T![TABLE] => {
				let _ = parser.next();
				ast::ShowTarget::Table(parser.parse_sync_push()?)
			}
			T![DATABASE] => {
				let _ = parser.next();
				ast::ShowTarget::Database(peek.span)
			}
			_ => return Err(parser.unexpected("keyword `TABLE` or `DATABASE`")),
		};

		let _ = parser.expect(T![SINCE])?;

		let peek = parser.peek_expect("a datetime or integer")?;
		let since = match peek.token {
			BaseTokenKind::DateTimeString => ast::ShowSince::Timestamp(parser.parse_sync_push()?),
			BaseTokenKind::Int => ast::ShowSince::VersionStamp(parser.parse_sync_push()?),
			_ => return Err(parser.unexpected("a datetime or integer")),
		};

		let limit = if parser.eat(T![LIMIT])?.is_some() {
			Some(parser.parse_enter_push().await?)
		} else {
			None
		};

		let span = parser.span_since(start.span);
		Ok(ast::Show {
			target,
			since,
			limit,
			span,
		})
	}
}

impl Parse for ast::Delete {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![DELETE])?;
		let only = parser.eat(T![ONLY])?.is_some();
		let (_, targets) =
			parse_seperated_list(parser, T![,], async |parser| parser.parse_enter().await).await?;
		let with_index = if parser.eat(T![WITH])?.is_some() {
			let peek = parser.peek_expect("`INDEX` or `NOINDEX`")?;
			let idx = match peek.token {
				T![NOINDEX] => {
					let _ = parser.next();
					ast::WithIndex::None(peek.span)
				}
				T![INDEX] => {
					let _ = parser.next();
					let (_, indecies) =
						parse_seperated_list_sync(parser, T![,], |parser| parser.parse_sync())?;
					ast::WithIndex::Some(indecies)
				}
				_ => return Err(parser.unexpected("`INDEX` or `NOINDEX`")),
			};
			Some(idx)
		} else {
			None
		};
		let condition = if parser.eat(T![WHERE])?.is_some() {
			Some(parser.parse_enter_push().await?)
		} else {
			None
		};
		let timeout = if parser.eat(T![TIMEOUT])?.is_some() {
			Some(parser.parse_enter_push().await?)
		} else {
			None
		};

		let explain = if let Some(explain) = parser.eat(T![EXPLAIN])? {
			if let Some(full) = parser.eat(T![FULL])? {
				Some(ast::Explain::Full(explain.span.extend(full.span)))
			} else {
				Some(ast::Explain::Base(explain.span))
			}
		} else {
			None
		};

		let span = parser.span_since(start.span);
		Ok(ast::Delete {
			only,
			targets,
			with_index,
			condition,
			timeout,
			explain,
			span,
		})
	}
}

impl Parse for ast::Assignment {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.peek_span();
		let place = parser.parse_push().await?;

		let peek = parser.peek_expect("`=`, `+=`, `-=`, or `+?=`")?;
		let op = match peek.token {
			T![=] => ast::AssignmentOp::Assign(peek.span),
			T![+=] => ast::AssignmentOp::Add(peek.span),
			T![-=] => ast::AssignmentOp::Subtract(peek.span),
			T![+?=] => ast::AssignmentOp::Extend(peek.span),
			_ => return Err(parser.unexpected("`=`, `+=`, `-=`, or `+?=`")),
		};
		let _ = parser.next();

		let value = parser.parse_enter_push().await?;
		let span = parser.span_since(start);

		Ok(ast::Assignment {
			place,
			op,
			value,
			span,
		})
	}
}

impl Parse for ast::Create {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![CREATE])?;
		let only = parser.eat(T![ONLY])?.is_some();
		let (_, targets) =
			parse_seperated_list(parser, T![,], async |parser| parser.parse_enter().await).await?;

		let peek = parser.peek()?;
		let data = match peek.map(|x| x.token) {
			Some(T![SET]) => {
				let _ = parser.next();
				let (_, list) =
					parse_seperated_list(parser, T![,], async |parser| parser.parse().await)
						.await?;
				Some(ast::RecordData::Set(list))
			}
			Some(T![UNSET]) => {
				let _ = parser.next();
				let (_, list) =
					parse_seperated_list(parser, T![,], async |parser| parser.parse().await)
						.await?;
				Some(ast::RecordData::Unset(list))
			}
			Some(T![CONTENT]) => {
				let _ = parser.next();
				Some(ast::RecordData::Content(parser.parse_enter_push().await?))
			}
			Some(T![PATCH]) => {
				let _ = parser.next();
				Some(ast::RecordData::Patch(parser.parse_enter_push().await?))
			}
			Some(T![MERGE]) => {
				let _ = parser.next();
				Some(ast::RecordData::Merge(parser.parse_enter_push().await?))
			}
			Some(T![REPLACE]) => {
				let _ = parser.next();
				Some(ast::RecordData::Replace(parser.parse_enter_push().await?))
			}
			_ => None,
		};

		let version = if parser.eat(T![VERSION])?.is_some() {
			Some(parser.parse_enter_push().await?)
		} else {
			None
		};

		let timeout = if parser.eat(T![TIMEOUT])?.is_some() {
			Some(parser.parse_enter_push().await?)
		} else {
			None
		};

		let span = parser.span_since(start.span);
		Ok(ast::Create {
			only,
			targets,
			data,
			version,
			timeout,
			span,
		})
	}
}
