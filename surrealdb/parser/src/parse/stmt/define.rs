use ast::{ApiAction, RelationTable};
use common::source_error::{AnnotationKind, Level};
use common::span::Span;
use token::{BaseTokenKind, T};

use crate::parse::utils::{parse_delimited_list, parse_seperated_list, parse_seperated_list_sync};
use crate::parse::{ParseError, ParseResult};
use crate::{Parse, ParseSync, Parser};

impl ParseSync for ast::DefineKind {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let peek = parser.peek()?;
		let kind = match peek.map(|x| x.token) {
			Some(T![OVERWRITE]) => {
				let _ = parser.next()?;
				ast::DefineKind::Overwrite
			}
			Some(T![IF]) => {
				let _ = parser.next()?;
				let _ = parser.expect(T![NOT])?;
				let _ = parser.expect(T![EXISTS])?;
				ast::DefineKind::IfNotExists
			}
			_ => ast::DefineKind::Create,
		};
		Ok(kind)
	}
}

impl Parse for ast::Permission {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let peek = parser.peek_expect("`NONE`, `FULL` or `WHERE`")?;
		let res = match peek.token {
			T![NONE] => ast::Permission::None(peek.span),
			T![FULL] => ast::Permission::Full(peek.span),
			T![WHERE] => {
				let expr = parser.parse_enter_push().await?;
				ast::Permission::Where(expr)
			}
			_ => return Err(parser.unexpected("`NONE`, `FULL` or `WHERE`")),
		};
		Ok(res)
	}
}

#[cold]
fn reuse_error(parser: &mut Parser<'_, '_>, start: Span, last_span: Span) -> ParseError {
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

async fn parse_unordered_clause<T, F>(
	parser: &mut Parser<'_, '_>,
	store: &mut Option<(T, Span)>,
	start: Span,
	f: F,
) -> ParseResult<()>
where
	F: AsyncFnOnce(&mut Parser<'_, '_>) -> ParseResult<T>,
{
	if let Some((_, last_span)) = store {
		return Err(reuse_error(parser, start, *last_span));
	}

	let res = f(parser).await?;
	let span = parser.span_since(start);
	*store = Some((res, span));

	Ok(())
}

fn parse_unordered_clause_sync<T, F>(
	parser: &mut Parser<'_, '_>,
	store: &mut Option<(T, Span)>,
	start: Span,
	f: F,
) -> ParseResult<()>
where
	F: FnOnce(&mut Parser<'_, '_>) -> ParseResult<T>,
{
	if let Some((_, last_span)) = store {
		return Err(reuse_error(parser, start, *last_span));
	}

	let res = f(parser)?;
	let span = parser.span_since(start);
	*store = Some((res, span));

	Ok(())
}

impl Parse for ast::DefineNamespace {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![NAMESPACE])?;

		let kind = parser.parse_sync()?;
		let name = parser.parse_enter_push().await?;

		let mut comment = None;
		while let Some(x) = parser.peek()? {
			match x.token {
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, x.span, async |parser| {
						parser.parse_enter_push::<ast::Expr>().await
					})
					.await?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(define.span);
		Ok(ast::DefineNamespace {
			kind,
			name,
			comment: comment.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::DefineDatabase {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![DATABASE])?;

		let kind = parser.parse_sync()?;
		let name = parser.parse_enter_push().await?;

		let mut comment = None;
		let mut changefeed = None;
		let mut strict = None;
		while let Some(x) = parser.peek()? {
			match x.token {
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, x.span, async |parser| {
						parser.parse_enter_push::<ast::Expr>().await
					})
					.await?;
				}
				T![STRICT] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut strict, x.span, |_| Ok(()))?;
				}
				T![CHANGEFEED] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut changefeed, x.span, |parser| {
						let duration = parser.parse_sync_push()?;
						if parser.eat(T![INCLUDE])?.is_some() {
							let _ = parser.expect(T![ORIGINAL])?;
							Ok(ast::ChangeFeed::WithOriginal(duration))
						} else {
							Ok(ast::ChangeFeed::Base(duration))
						}
					})?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(define.span);
		Ok(ast::DefineDatabase {
			kind,
			name,
			comment: comment.map(|x| x.0),
			strict: strict.is_some(),
			changefeed: changefeed.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::DefineFunction {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![FUNCTION])?;

		let kind = parser.parse_sync()?;
		let name = parser.parse_sync_push::<ast::Path>()?;

		// TODO: Maybe pre-allocate?;
		let fn_name = parser.ast.push_set_entry::<String, _>("fn");
		if name.index(parser).start.index(parser).text != fn_name {
			return Err(parser.with_error(|parser| {
				Level::Error
					.title("Defined functions must start with `fn`")
					.snippet(
						parser
							.snippet()
							.annotate(AnnotationKind::Primary.span(name.index(parser).span)),
					)
					.to_diagnostic()
			}));
		}

		let parameters = parse_delimited_list(
			parser,
			BaseTokenKind::OpenParen,
			BaseTokenKind::CloseParen,
			T![,],
			async |parser| parser.parse().await,
		)
		.await?
		.1;

		let return_ty = if parser.eat(T![->])?.is_some() {
			Some(parser.parse_push().await?)
		} else {
			None
		};

		let body = parser.parse_push().await?;

		let mut comment = None;
		let mut permissions = None;
		while let Some(x) = parser.peek()? {
			match x.token {
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, x.span, async |parser| {
						parser.parse_enter_push::<ast::Expr>().await
					})
					.await?;
				}
				T![PERMISSIONS] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut permissions, x.span, async |parser| {
						parser.parse().await
					})
					.await?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(define.span);
		Ok(ast::DefineFunction {
			kind,
			name,
			parameters,
			return_ty,
			body,
			comment: comment.map(|x| x.0),
			permission: permissions.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::DefineModule {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![MODULE])?;
		let kind = parser.parse_sync()?;

		let peek = parser.peek_expect("a file literal or a path")?;
		let subject = match peek.token {
			BaseTokenKind::FileString => ast::ModuleName::File(parser.parse_sync_push()?),
			x if x.is_identifier() => ast::ModuleName::Path(parser.parse_sync_push()?),
			_ => return Err(parser.unexpected("a file literal or a path")),
		};
		let alias = if parser.eat(T![AS])?.is_some() {
			Some(parser.parse_sync_push()?)
		} else {
			None
		};

		let mut comment = None;
		let mut permissions = None;
		while let Some(x) = parser.peek()? {
			match x.token {
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, x.span, async |parser| {
						parser.parse_enter_push::<ast::Expr>().await
					})
					.await?;
				}
				T![PERMISSIONS] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut permissions, x.span, async |parser| {
						parser.parse().await
					})
					.await?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(define.span);
		Ok(ast::DefineModule {
			kind,
			subject,
			alias,
			comment: comment.map(|x| x.0),
			permission: permissions.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::DefineParam {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![PARAM])?;
		let kind = parser.parse_sync()?;

		let param = parser.parse_sync_push()?;

		let mut comment = None;
		let mut value = None;
		let mut permissions = None;
		while let Some(x) = parser.peek()? {
			match x.token {
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, x.span, async |parser| {
						parser.parse_enter_push::<ast::Expr>().await
					})
					.await?;
				}
				T![VALUE] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut value, x.span, async |parser| {
						parser.parse_enter_push::<ast::Expr>().await
					})
					.await?;
				}
				T![PERMISSIONS] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut permissions, x.span, async |parser| {
						parser.parse().await
					})
					.await?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(define.span);
		Ok(ast::DefineParam {
			kind,
			param,
			span,
			value: value.map(|x| x.0),
			comment: comment.map(|x| x.0),
			permission: permissions.map(|x| x.0),
		})
	}
}

impl Parse for ast::DefineTable {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![TABLE])?;
		let kind = parser.parse_sync()?;

		let name = parser.parse_enter_push().await?;

		let mut comment = None;
		let mut permissions = None;
		let mut drop = None;
		let mut schema = None;
		let mut view = None;
		let mut changefeed = None;
		let mut table_kind = None;
		while let Some(x) = parser.peek()? {
			match x.token {
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, x.span, async |parser| {
						parser.parse_enter_push::<ast::Expr>().await
					})
					.await?;
				}
				T![DROP] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut drop, x.span, |_| Ok(()))?;
				}
				T![SCHEMAFULL] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut schema, x.span, |_| {
						Ok(ast::Schema::Full)
					})?;
				}
				T![SCHEMALESS] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut schema, x.span, |_| {
						Ok(ast::Schema::Less)
					})?;
				}
				T![PERMISSIONS] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut permissions, x.span, async |parser| {
						parser.parse().await
					})
					.await?;
				}
				T![AS] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut view, x.span, async |parser| {
						parser.parse_push::<ast::Select>().await
					})
					.await?;
				}
				T![CHANGEFEED] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut changefeed, x.span, |parser| {
						let duration = parser.parse_sync_push()?;
						if parser.eat(T![INCLUDE])?.is_some() {
							let _ = parser.expect(T![ORIGINAL])?;
							Ok(ast::ChangeFeed::WithOriginal(duration))
						} else {
							Ok(ast::ChangeFeed::Base(duration))
						}
					})?;
				}
				T![TYPE] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut table_kind, x.span, |parser| {
						let peek = parser.peek_expect("`NORMAL`, `RELATION`, `ANY`")?;
						let res = match peek.token {
							T![NORMAL] => {
								let _ = parser.next();
								ast::TableKind::Normal(peek.span)
							}
							T![ANY] => {
								let _ = parser.next();
								ast::TableKind::Any(peek.span)
							}
							T![RELATION] => {
								let mut from = None;
								let mut to = None;

								while let Some(x) = parser.peek()? {
									match x.token {
										T![FROM] | T![IN] => {
											parse_unordered_clause_sync(
												parser,
												&mut from,
												x.span,
												|parser| {
													parse_seperated_list_sync(
														parser,
														T![,],
														|parser| parser.parse_sync(),
													)
													.map(|x| x.1)
												},
											)?;
										}
										T![TO] | T![OUT] => {
											parse_unordered_clause_sync(
												parser,
												&mut to,
												x.span,
												|parser| {
													parse_seperated_list_sync(
														parser,
														T![,],
														|parser| parser.parse_sync(),
													)
													.map(|x| x.1)
												},
											)?;
										}
										_ => break,
									}
								}

								let enforced = parser.eat(T![ENFORCED])?.is_some();

								let span = parser.span_since(peek.span);
								ast::TableKind::Relation(RelationTable {
									from: from.map(|x| x.0),
									to: to.map(|x| x.0),
									enforced,
									span,
								})
							}
							_ => return Err(parser.unexpected("`NORMAL`, `RELATION`, `ANY`")),
						};
						Ok(res)
					})?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(define.span);
		Ok(ast::DefineTable {
			kind,
			name,
			comment: comment.map(|x| x.0),
			permission: permissions.map(|x| x.0),
			drop: drop.is_some(),
			schema: schema.map(|x| x.0),
			view: view.map(|x| x.0),
			changefeed: changefeed.map(|x| x.0),
			table_kind: table_kind.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::ApiMiddleware {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let span = parser.peek_span();
		let path = parser.parse_sync_push()?;
		let (_, args) = parse_delimited_list(
			parser,
			BaseTokenKind::OpenParen,
			BaseTokenKind::CloseParen,
			T![,],
			async |parser| parser.parse_enter().await,
		)
		.await?;
		let span = parser.span_since(span);
		Ok(ast::ApiMiddleware {
			path,
			args,
			span,
		})
	}
}

impl Parse for ast::ApiAction {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let span = parser.peek_span();
		let mut permission = None;
		let mut middleware = None;

		let mut did_parse = false;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![PERMISSIONS] => {
					let _ = parser.next();

					parse_unordered_clause(parser, &mut permission, peek.span, async |parser| {
						parser.parse().await
					})
					.await?;
					did_parse = true;
				}
				T![MIDDLEWARE] => {
					let _ = parser.next();

					parse_unordered_clause(parser, &mut middleware, peek.span, async |parser| {
						parse_seperated_list(parser, T![,], async |parser| parser.parse().await)
							.await
							.map(|x| x.1)
					})
					.await?;
					did_parse = true;
				}
				_ => break,
			}
		}
		if !did_parse {
			return Err(parser.unexpected("`PERMISSIONS`, or `MIDDLEWARE`"));
		}

		let _ = parser.expect(T![THEN])?;

		let action = parser.parse_enter_push().await?;

		Ok(ast::ApiAction {
			middleware: middleware.map(|x| x.0),
			permission: permission.map(|x| x.0),
			action,
			span,
		})
	}
}

macro_rules! impl_method_matching {
    (($parser:expr) => {$($pat:pat => ($store:ident, $new_span:ident)),*}) => {
		$(let mut $new_span = None;)*
		let peek = $parser
			.peek_expect("`DELETE`, `GET`, `PATCH`, `POST`, `PUT`, or `TRACE`")?;
		loop{
			match peek.token {
				$($pat => {
					let _ = $parser.next();
					if let Some(span) = $store.map(|x: (_, Span)| x.1).or($new_span) {
						return Err(reuse_error($parser, peek.span, span));
					}
					$new_span = Some(peek.span)
				})*
				_ => {
					return Err($parser.unexpected(
						"`DELETE`, `GET`, `PATCH`, `POST`, `PUT`, or `TRACE`",
					));
				}
			}

			if $parser.eat(T![,])?.is_none(){
				break
			}
		}

		let action = $parser.parse_push::<ast::ApiAction>().await?;

		$(
			if let Some($new_span) = $new_span{
				$store = Some((action, $new_span));
			}
		)*
    };
}

impl Parse for ast::DefineApi {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![API])?;
		let kind = parser.parse_sync()?;

		let path = parser.parse_enter_push().await?;

		let mut base_permission = None;
		let mut base_middleware = None;
		let mut fallback = None;
		let mut get = None;
		let mut patch = None;
		let mut put = None;
		let mut post = None;
		let mut trace = None;
		let mut delete = None;
		loop {
			if parser.eat(T![FOR])?.is_none() {
				break;
			}

			let peek =
				parser.peek_expect("`ANY`, `DELETE`, `GET`, `PATCH`, `POST`, `PUT`, or `TRACE`")?;
			match peek.token {
				T![ANY] => {
					let _ = parser.next();

					let mut did_parse = false;
					loop {
						let Some(peek) = parser.peek()? else {
							break;
						};
						match peek.token {
							T![PERMISSIONS] => {
								let _ = parser.next();

								parse_unordered_clause(
									parser,
									&mut base_permission,
									peek.span,
									async |parser| parser.parse().await,
								)
								.await?;
								did_parse = true;
							}
							T![MIDDLEWARE] => {
								let _ = parser.next();

								parse_unordered_clause(
									parser,
									&mut base_middleware,
									peek.span,
									async |parser| {
										parse_seperated_list(parser, T![,], async |parser| {
											parser.parse().await
										})
										.await
										.map(|x| x.1)
									},
								)
								.await?;
								did_parse = true;
							}
							_ => break,
						}
					}

					if let Some(x) = parser.eat(T![THEN])? {
						parse_unordered_clause(parser, &mut fallback, x.span, async |parser| {
							parser.parse_enter_push().await
						})
						.await?;
					}

					if !did_parse {
						return Err(parser.unexpected("`PERMISSIONS`, `MIDDLEWARE`, or `THEN`"));
					}
				}
				T![DELETE] | T![GET] | T![PATCH] | T![POST] | T![PUT] | T![TRACE] => {
					// macro for some very repetitive code
					// Don't forget to update the expectation strings inside the macro if you ever
					// add new methods.
					//
					// Matches any number of methods, checks if the method was already defined
					// somewhere, if so, throw an error, otherwise parse a ApiAction and set the
					// methods to the parsed action
					impl_method_matching! {
						(parser) => {
							T![DELETE] => (delete,delete_span),
							T![GET] => (get,get_span),
							T![PATCH] => (patch,patch_span),
							T![POST] => (post,post_span),
							T![PUT] => (put,put_span),
							T![TRACE] => (trace,trace_span)
						}
					}
				}
				_ => {
					return Err(parser
						.unexpected("`ANY`, `DELETE`, `GET`, `PATCH`, `POST`, `PUT`, or `TRACE`"));
				}
			}
		}

		let mut comment = None;
		while let Some(x) = parser.peek()? {
			match x.token {
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, x.span, async |parser| {
						parser.parse_enter_push::<ast::Expr>().await
					})
					.await?;
				}
				_ => break,
			}
		}

		let methods = ast::DefineMethodApiActions {
			get: get.map(|x| x.0),
			post: post.map(|x| x.0),
			patch: patch.map(|x| x.0),
			put: put.map(|x| x.0),
			trace: trace.map(|x| x.0),
			delete: delete.map(|x| x.0),
		};

		let span = parser.span_since(define.span);
		Ok(ast::DefineApi {
			kind,
			path,
			base_middleware: base_middleware.map(|x| x.0),
			base_permission: base_permission.map(|x| x.0),
			fallback: fallback.map(|x| x.0),
			methods,
			comment: comment.map(|x| x.0),
			span,
		})
	}
}
