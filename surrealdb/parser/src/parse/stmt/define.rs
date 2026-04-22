use ast::{
	AccessType, Base, CountIndex, DefineConfigKind, FullTextScoring, NodeId, RelationTable,
	UserSecret,
};
use common::source_error::{AnnotationKind, Level};
use common::span::Span;
use token::{BaseTokenKind, T};

use crate::parse::utils::{
	parse_delimited_list, parse_seperated_list, parse_seperated_list_sync, parse_unordered_clause,
	parse_unordered_clause_sync, redefined_error,
};
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
			T![NONE] => {
				let _ = parser.next();
				ast::Permission::None(peek.span)
			}
			T![FULL] => {
				let _ = parser.next();
				ast::Permission::Full(peek.span)
			}
			T![WHERE] => {
				let _ = parser.next();
				let expr = parser.parse_enter().await?;
				ast::Permission::Where(expr)
			}
			_ => return Err(parser.unexpected("`NONE`, `FULL` or `WHERE`")),
		};
		Ok(res)
	}
}

impl Parse for ast::DefineNamespace {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![NAMESPACE])?;

		let kind = parser.parse_sync()?;
		let name = parser.parse_enter().await?;

		let mut comment = None;
		while let Some(x) = parser.peek()? {
			match x.token {
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, x.span, Parser::parse_enter)
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

impl ParseSync for ast::ChangeFeed {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let duration = parser.parse_sync()?;
		if parser.eat(T![INCLUDE])?.is_some() {
			let _ = parser.expect(T![ORIGINAL])?;
			Ok(ast::ChangeFeed::WithOriginal(duration))
		} else {
			Ok(ast::ChangeFeed::Base(duration))
		}
	}
}

impl Parse for ast::DefineDatabase {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![DATABASE])?;

		let kind = parser.parse_sync()?;
		let name = parser.parse_enter().await?;

		let mut comment = None;
		let mut changefeed = None;
		let mut strict = None;
		while let Some(x) = parser.peek()? {
			match x.token {
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, x.span, Parser::parse_enter)
						.await?;
				}
				T![STRICT] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut strict, x.span, |_| Ok(()))?;
				}
				T![CHANGEFEED] => {
					let _ = parser.next();
					parse_unordered_clause_sync(
						parser,
						&mut changefeed,
						x.span,
						Parser::parse_sync,
					)?;
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
		let name = parser.parse_sync::<NodeId<ast::Path>>()?;

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
			Some(parser.parse().await?)
		} else {
			None
		};

		let body = parser.parse().await?;

		let mut comment = None;
		let mut permissions = None;
		while let Some(x) = parser.peek()? {
			match x.token {
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, x.span, Parser::parse_enter)
						.await?;
				}
				T![PERMISSIONS] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut permissions, x.span, Parser::parse).await?;
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
			BaseTokenKind::FileString => ast::ModuleName::File(parser.parse_sync()?),
			x if x.is_identifier() => ast::ModuleName::Path(parser.parse_sync()?),
			_ => return Err(parser.unexpected("a file literal or a path")),
		};
		let alias = if parser.eat(T![AS])?.is_some() {
			Some(parser.parse_sync()?)
		} else {
			None
		};

		let mut comment = None;
		let mut permissions = None;
		while let Some(x) = parser.peek()? {
			match x.token {
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, x.span, Parser::parse_enter)
						.await?;
				}
				T![PERMISSIONS] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut permissions, x.span, Parser::parse).await?;
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

		let param = parser.parse_sync()?;

		let mut comment = None;
		let mut value = None;
		let mut permissions = None;
		while let Some(x) = parser.peek()? {
			match x.token {
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, x.span, Parser::parse_enter)
						.await?;
				}
				T![VALUE] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut value, x.span, Parser::parse_enter).await?;
				}
				T![PERMISSIONS] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut permissions, x.span, Parser::parse).await?;
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

impl Parse for ast::TablePermissions {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let _ = parser.expect(T![PERMISSIONS])?;
		let peek = parser.peek_expect("`NONE`, `FULL`, or `FOR`")?;
		match peek.token {
			T![NONE] => {
				let _ = parser.next();
				Ok(ast::TablePermissions {
					create: Some(ast::Permission::None(peek.span)),
					delete: Some(ast::Permission::None(peek.span)),
					update: Some(ast::Permission::None(peek.span)),
					select: Some(ast::Permission::None(peek.span)),
				})
			}
			T![FULL] => {
				let _ = parser.next();
				Ok(ast::TablePermissions {
					create: Some(ast::Permission::Full(peek.span)),
					delete: Some(ast::Permission::Full(peek.span)),
					update: Some(ast::Permission::Full(peek.span)),
					select: Some(ast::Permission::Full(peek.span)),
				})
			}
			T![FOR] => {
				let mut create = None;
				let mut delete = None;
				let mut update = None;
				let mut select = None;

				let mut res = ast::TablePermissions {
					create: None,
					delete: None,
					update: None,
					select: None,
				};
				let _ = parser.expect(T![FOR])?;

				loop {
					loop {
						let peek =
							parser.peek_expect("`SELECT`, `UPDATE`, `CREATE`, or `DELETE`")?;
						match peek.token {
							T![SELECT] => {
								let _ = parser.next();
								parse_unordered_clause_sync(
									parser,
									&mut select,
									peek.span,
									|_| Ok(true),
								)?;
							}
							T![UPDATE] => {
								let _ = parser.next();
								parse_unordered_clause_sync(
									parser,
									&mut update,
									peek.span,
									|_| Ok(true),
								)?;
							}
							T![CREATE] => {
								let _ = parser.next();
								parse_unordered_clause_sync(
									parser,
									&mut create,
									peek.span,
									|_| Ok(true),
								)?;
							}
							T![DELETE] => {
								let _ = parser.next();
								parse_unordered_clause_sync(
									parser,
									&mut delete,
									peek.span,
									|_| Ok(true),
								)?;
							}
							_ => {
								return Err(
									parser.unexpected("`SELECT`, `UPDATE`, `CREATE`, or `DELETE`")
								);
							}
						}

						if parser.eat(T![,])?.is_none() {
							break;
						}
					}

					let permission = parser.parse::<ast::Permission>().await?;

					if let Some((x, _)) = &mut create
						&& *x
					{
						*x = false;
						res.create = Some(permission.clone())
					}
					if let Some((x, _)) = &mut update
						&& *x
					{
						*x = false;
						res.update = Some(permission.clone())
					}
					if let Some((x, _)) = &mut select
						&& *x
					{
						*x = false;
						res.select = Some(permission.clone())
					}
					if let Some((x, _)) = &mut delete
						&& *x
					{
						*x = false;
						res.delete = Some(permission.clone())
					}

					let _ = parser.eat(T![,])?;

					if parser.eat(T![FOR])?.is_none() {
						break;
					}
				}

				Ok(res)
			}
			_ => Err(parser.unexpected("`NONE`, `FULL`, or `FOR`")),
		}
	}
}

impl ParseSync for ast::TableKind {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let peek = parser.peek_expect("`NORMAL`, `RELATION`, `ANY`")?;
		match peek.token {
			T![NORMAL] => {
				let _ = parser.next();
				Ok(ast::TableKind::Normal(peek.span))
			}
			T![ANY] => {
				let _ = parser.next();
				Ok(ast::TableKind::Any(peek.span))
			}
			T![RELATION] => {
				let _ = parser.next();
				let mut from = None;
				let mut to = None;

				while let Some(x) = parser.peek()? {
					match x.token {
						T![FROM] | T![IN] => {
							let _ = parser.next();
							parse_unordered_clause_sync(parser, &mut from, x.span, |parser| {
								parse_seperated_list_sync(parser, T![|], Parser::parse_sync)
									.map(|x| x.1)
							})?;
						}
						T![TO] | T![OUT] => {
							let _ = parser.next();
							parse_unordered_clause_sync(parser, &mut to, x.span, |parser| {
								parse_seperated_list_sync(parser, T![|], Parser::parse_sync)
									.map(|x| x.1)
							})?;
						}
						_ => break,
					}
				}

				let enforced = parser.eat(T![ENFORCED])?.is_some();

				let span = parser.span_since(peek.span);
				Ok(ast::TableKind::Relation(RelationTable {
					from: from.map(|x| x.0),
					to: to.map(|x| x.0),
					enforced,
					span,
				}))
			}
			_ => Err(parser.unexpected("`NORMAL`, `RELATION`, `ANY`")),
		}
	}
}

impl Parse for ast::DefineTable {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![TABLE])?;
		let kind = parser.parse_sync()?;

		let name = parser.parse_enter().await?;

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
					parse_unordered_clause(parser, &mut comment, x.span, Parser::parse_enter)
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
					parse_unordered_clause(parser, &mut permissions, x.span, Parser::parse).await?;
				}
				T![AS] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut view, x.span, Parser::parse).await?;
				}
				T![CHANGEFEED] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut changefeed, x.span, |parser| {
						let duration = parser.parse_sync()?;
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
					parse_unordered_clause_sync(
						parser,
						&mut table_kind,
						x.span,
						Parser::parse_sync,
					)?;
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
		let path = parser.parse_sync()?;
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

		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![PERMISSIONS] => {
					let _ = parser.next();

					parse_unordered_clause(parser, &mut permission, peek.span, Parser::parse)
						.await?;
				}
				T![MIDDLEWARE] => {
					let _ = parser.next();

					parse_unordered_clause(parser, &mut middleware, peek.span, async |parser| {
						parse_seperated_list(parser, T![,], Parser::parse).await.map(|x| x.1)
					})
					.await?;
				}
				_ => break,
			}
		}

		let _ = parser.expect(T![THEN])?;

		let action = parser.parse_enter().await?;

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
				loop{
					let peek = $parser
						.peek_expect("`DELETE`, `GET`, `PATCH`, `POST`, `PUT`, or `TRACE`")?;
					match peek.token {
						$($pat => {
							let _ = $parser.next();
							if let Some(span) = $store.map(|x: (_, Span)| x.1).or($new_span) {
								return Err(redefined_error($parser, peek.span, span));
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

			let action = $parser.parse::<ast::NodeId<ast::ApiAction>>().await?;

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

		let path = parser.parse_enter().await?;

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
									Parser::parse,
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
										parse_seperated_list(parser, T![,], Parser::parse)
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
						did_parse = true;
						parse_unordered_clause(parser, &mut fallback, x.span, Parser::parse_enter)
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
					parse_unordered_clause(parser, &mut comment, x.span, Parser::parse_enter)
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

impl Parse for ast::DefineEvent {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![EVENT])?;
		let kind = parser.parse_sync()?;

		let name = parser.parse_enter().await?;
		let _ = parser.expect(T![ON])?;
		let _ = parser.eat(T![TABLE])?;
		let table = parser.parse_enter().await?;

		let mut comment = None;
		let mut async_ = None;
		let mut then = None;
		let mut condition = None;

		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};

			match peek.token {
				T![WHEN] => {
					let _ = parser.next();

					parse_unordered_clause(parser, &mut condition, peek.span, Parser::parse_enter)
						.await?
				}
				T![THEN] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut then, peek.span, async |parser| {
						parse_seperated_list(parser, T![,], Parser::parse_enter).await.map(|x| x.1)
					})
					.await?
				}
				T![COMMENT] => {
					let _ = parser.next();

					parse_unordered_clause(parser, &mut comment, peek.span, Parser::parse_enter)
						.await?
				}
				T![ASYNC] => {
					let _ = parser.next();

					parse_unordered_clause_sync(parser, &mut async_, peek.span, |parser| {
						let mut retry = None;
						let mut max_depth = None;
						loop {
							let Some(peek) = parser.peek()? else {
								break;
							};

							match peek.token {
								T![RETRY] => {
									let _ = parser.next();
									parse_unordered_clause_sync(
										parser,
										&mut retry,
										peek.span,
										Parser::parse_sync,
									)?;
								}
								T![MAXDEPTH] => {
									let _ = parser.next();
									parse_unordered_clause_sync(
										parser,
										&mut max_depth,
										peek.span,
										Parser::parse_sync,
									)?;
								}
								_ => break,
							}
						}

						let span = parser.span_since(peek.span);
						Ok(ast::DefineEventAsync {
							retry: retry.map(|x| x.0),
							max_depth: max_depth.map(|x| x.0),
							span,
						})
					})?
				}
				_ => break,
			}
		}

		let Some((then, _)) = then else {
			return Err(parser.with_error(|parser| {
				Level::Error
					.title(
						"Event is missing an event expression, expected atleast one `THEN` clause",
					)
					.snippet(
						parser.snippet().annotate(AnnotationKind::Primary.span(parser.last_span)),
					)
					.to_diagnostic()
			}));
		};

		let span = parser.span_since(define.span);
		Ok(ast::DefineEvent {
			kind,
			name,
			table,
			then,
			comment: comment.map(|x| x.0),
			async_: async_.map(|x| x.0),
			condition: condition.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::FieldPermissions {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let _ = parser.expect(T![PERMISSIONS])?;
		let peek = parser.peek_expect("`NONE`, `FULL`, or `FOR`")?;
		match peek.token {
			T![NONE] => {
				let _ = parser.next();
				Ok(ast::FieldPermissions {
					create: Some(ast::Permission::None(peek.span)),
					update: Some(ast::Permission::None(peek.span)),
					select: Some(ast::Permission::None(peek.span)),
				})
			}
			T![FULL] => {
				let _ = parser.next();
				Ok(ast::FieldPermissions {
					create: Some(ast::Permission::Full(peek.span)),
					update: Some(ast::Permission::Full(peek.span)),
					select: Some(ast::Permission::Full(peek.span)),
				})
			}
			T![FOR] => {
				let mut create = None;
				let mut update = None;
				let mut select = None;

				let mut res = ast::FieldPermissions {
					create: None,
					update: None,
					select: None,
				};

				loop {
					let _ = parser.expect(T![FOR])?;

					loop {
						let peek =
							parser.peek_expect("`SELECT`, `UPDATE`, `CREATE`, or `DELETE`")?;
						match peek.token {
							T![SELECT] => {
								let _ = parser.next();
								parse_unordered_clause_sync(
									parser,
									&mut select,
									peek.span,
									|_| Ok(true),
								)?;
							}
							T![UPDATE] => {
								let _ = parser.next();
								parse_unordered_clause_sync(
									parser,
									&mut update,
									peek.span,
									|_| Ok(true),
								)?;
							}
							T![CREATE] => {
								let _ = parser.next();
								parse_unordered_clause_sync(
									parser,
									&mut create,
									peek.span,
									|_| Ok(true),
								)?;
							}
							_ => {
								return Err(
									parser.unexpected("`SELECT`, `UPDATE`, `CREATE`, or `DELETE`")
								);
							}
						}

						if parser.eat(T![,])?.is_none() {
							break;
						}
					}

					let permission = parser.parse::<ast::Permission>().await?;

					if let Some((x, _)) = &mut create
						&& *x
					{
						*x = false;
						res.create = Some(permission.clone())
					}
					if let Some((x, _)) = &mut update
						&& *x
					{
						*x = false;
						res.update = Some(permission.clone())
					}
					if let Some((x, _)) = &mut select
						&& *x
					{
						*x = false;
						res.select = Some(permission.clone())
					}
					if parser.eat(T![,])?.is_none() {
						break;
					}
				}

				Ok(res)
			}
			_ => Err(parser.unexpected("`NONE`, `FULL`, or `FOR`")),
		}
	}
}

impl Parse for ast::OnDelete {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let _ = parser.expect(T![REFERENCE])?;

		if parser.eat(T![ON])?.is_some() {
			let _ = parser.expect(T![DELETE])?;

			let peek = parser.peek_expect("`REJECT`, `CASCADE`, `IGNORE`, `UNSET`, or `THEN`")?;
			match peek.token {
				T![REJECT] => {
					let _ = parser.next();
					Ok(ast::OnDelete::Reject)
				}
				T![CASCADE] => {
					let _ = parser.next();
					Ok(ast::OnDelete::Cascade)
				}
				T![IGNORE] => {
					let _ = parser.next();
					Ok(ast::OnDelete::Ignore)
				}
				T![UNSET] => {
					let _ = parser.next();
					Ok(ast::OnDelete::Unset)
				}
				T![THEN] => {
					let _ = parser.next();
					let expr = parser.parse_enter().await?;
					Ok(ast::OnDelete::Then(expr))
				}
				_ => Err(parser.unexpected("`REJECT`, `CASCADE`, `IGNORE`, `UNSET`, or `THEN`")),
			}
		} else {
			Ok(ast::OnDelete::Ignore)
		}
	}
}

impl Parse for ast::DefineField {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![FIELD])?;
		let kind = parser.parse_sync()?;

		let name = parser.parse_enter().await?;
		let _ = parser.expect(T![ON])?;
		let _ = parser.eat(T![TABLE])?;
		let table = parser.parse_enter().await?;

		let mut ty = None;
		let mut flexible = None;
		let mut readonly = None;
		let mut value = None;
		let mut assert = None;
		let mut computed = None;
		let mut default = None;
		let mut permissions = None;
		let mut comment = None;
		let mut on_delete = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, peek.span, Parser::parse_enter)
						.await?
				}
				T![TYPE] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut ty, peek.span, Parser::parse).await?
				}
				T![FLEXIBLE] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut flexible, peek.span, |_| Ok(()))?
				}
				T![READONLY] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut readonly, peek.span, |_| Ok(()))?
				}
				T![VALUE] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut value, peek.span, Parser::parse_enter)
						.await?
				}
				T![COMPUTED] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut computed, peek.span, Parser::parse_enter)
						.await?
				}
				T![ASSERT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut assert, peek.span, Parser::parse_enter)
						.await?
				}
				T![PERMISSIONS] => {
					parse_unordered_clause(parser, &mut permissions, peek.span, Parser::parse)
						.await?
				}
				T![DEFAULT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut default, peek.span, async |parser| {
						if parser.eat(T![ALWAYS])?.is_some() {
							Ok(ast::FieldDefault::Always(parser.parse_enter().await?))
						} else {
							Ok(ast::FieldDefault::Some(parser.parse_enter().await?))
						}
					})
					.await?
				}
				T![REFERENCE] => {
					parse_unordered_clause(parser, &mut on_delete, peek.span, Parser::parse).await?
				}
				_ => break,
			}
		}

		let span = parser.span_since(define.span);
		Ok(ast::DefineField {
			kind,
			name,
			table,
			ty: ty.map(|x| x.0),
			flexible: flexible.is_some(),
			readonly: readonly.is_some(),
			value: value.map(|x| x.0),
			assert: assert.map(|x| x.0),
			computed: computed.map(|x| x.0),
			default: default.map(|x| x.0),
			permissions: permissions.map(|x| x.0),
			comment: comment.map(|x| x.0),
			on_delete: on_delete.map(|x| x.0),
			span,
		})
	}
}

impl ParseSync for ast::Distance {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let peek = parser.peek_expect("a distance measure")?;
		match peek.token {
			T![EUCLIDEAN] => {
				let _ = parser.next();
				Ok(ast::Distance::Euclidean)
			}
			T![CHEBYSHEV] => {
				let _ = parser.next();
				Ok(ast::Distance::Chebyshev)
			}
			T![COSINE] => {
				let _ = parser.next();
				Ok(ast::Distance::Cosine)
			}
			T![HAMMING] => {
				let _ = parser.next();
				Ok(ast::Distance::Hamming)
			}
			T![JACCARD] => {
				let _ = parser.next();
				Ok(ast::Distance::Jaccard)
			}
			T![MANHATTAN] => {
				let _ = parser.next();
				Ok(ast::Distance::Manhattan)
			}
			T![MINKOWSKI] => {
				let _ = parser.next();
				Ok(ast::Distance::Minkowski(parser.parse_sync()?))
			}
			T![PEARSON] => {
				let _ = parser.next();
				Ok(ast::Distance::Pearson)
			}
			_ => Err(parser.unexpected("a distance measure")),
		}
	}
}

impl ParseSync for ast::VectorType {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let peek = parser.peek_expect("a vector type")?;
		match peek.token {
			T![F64] => {
				let _ = parser.next();
				Ok(ast::VectorType::F64)
			}
			T![F32] => {
				let _ = parser.next();
				Ok(ast::VectorType::F32)
			}
			T![I64] => {
				let _ = parser.next();
				Ok(ast::VectorType::I64)
			}
			T![I32] => {
				let _ = parser.next();
				Ok(ast::VectorType::I32)
			}
			T![I16] => {
				let _ = parser.next();
				Ok(ast::VectorType::I16)
			}
			_ => Err(parser.unexpected("a vector type")),
		}
	}
}

impl ParseSync for ast::HnswIndex {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let start = parser.expect(T![HNSW])?;

		let _ = parser.expect(T![DIMENSION])?;
		let dimension = parser.parse_sync()?;

		let mut distance = None;
		let mut ty = None;
		let mut m = None;
		let mut m0 = None;
		let mut ml = None;
		let mut ef_construction = None;
		let mut extend_candidates = None;
		let mut keep_pruned_connections = None;
		let mut use_hashed_vector = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![DISTANCE] => {
					let _ = parser.next();
					parse_unordered_clause_sync(
						parser,
						&mut distance,
						peek.span,
						Parser::parse_sync,
					)?;
				}
				T![TYPE] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut ty, peek.span, Parser::parse_sync)?;
				}
				T![LM] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut ml, peek.span, Parser::parse_sync)?;
				}
				T![M0] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut m0, peek.span, Parser::parse_sync)?;
				}
				T![M] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut m, peek.span, Parser::parse_sync)?;
				}
				T![EFC] => {
					let _ = parser.next();
					parse_unordered_clause_sync(
						parser,
						&mut ef_construction,
						peek.span,
						Parser::parse_sync,
					)?;
				}
				T![EXTEND_CANDIDATES] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut extend_candidates, peek.span, |_| {
						Ok(())
					})?;
				}
				T![KEEP_PRUNED_CONNECTIONS] => {
					let _ = parser.next();
					parse_unordered_clause_sync(
						parser,
						&mut keep_pruned_connections,
						peek.span,
						|_| Ok(()),
					)?;
				}
				T![HASHED_VECTOR] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut use_hashed_vector, peek.span, |_| {
						Ok(())
					})?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(start.span);
		Ok(ast::HnswIndex {
			dimension,
			distance: distance.map(|x| x.0),
			ty: ty.map(|x| x.0),
			m: m.map(|x| x.0),
			m0: m0.map(|x| x.0),
			ml: ml.map(|x| x.0),
			ef_construction: ef_construction.map(|x| x.0),
			extend_candidates: extend_candidates.is_some(),
			keep_pruned_connections: keep_pruned_connections.is_some(),
			use_hashed_vector: use_hashed_vector.is_some(),
			span,
		})
	}
}

impl ParseSync for ast::FullTextIndex {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let start = parser.expect(T![FULLTEXT])?;

		let mut scoring = None;
		let mut analyzer = None;
		let mut highlights = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};

			match peek.token {
				T![ANALYZER] => {
					let _ = parser.next();
					parse_unordered_clause_sync(
						parser,
						&mut analyzer,
						peek.span,
						Parser::parse_sync,
					)?;
				}
				T![HIGHLIGHTS] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut highlights, peek.span, |_| Ok(()))?;
				}
				T![BM25] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut scoring, peek.span, |parser| {
						if let Some(open) = parser.eat(BaseTokenKind::OpenParen)? {
							let k1 = parser.parse_sync()?;
							let _ = parser.expect(T![,])?;
							let b = parser.parse_sync()?;
							let _ = parser
								.expect_closing_delimiter(BaseTokenKind::CloseParen, open.span)?;
							Ok(FullTextScoring::Bm25 {
								k1,
								b,
							})
						} else {
							Ok(FullTextScoring::VectorSearch)
						}
					})?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(start.span);
		Ok(ast::FullTextIndex {
			analyzer: analyzer.map(|x| x.0),
			scoring: scoring.map(|x| x.0),
			highlights: highlights.is_some(),
			span,
		})
	}
}

impl Parse for ast::DefineIndex {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		#[cold]
		fn index_type_redefined(
			parser: &mut Parser<'_, '_>,
			span: Span,
			old_span: Span,
		) -> ParseError {
			parser.with_error(|parser| {
				Level::Error
					.title("Index type specified in more then one clause")
					.snippet(
						parser.snippet().annotate(AnnotationKind::Primary.span(span)).annotate(
							AnnotationKind::Context
								.span(old_span)
								.label("Index type first defined here"),
						),
					)
					.to_diagnostic()
			})
		}

		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![INDEX])?;
		let kind = parser.parse_sync()?;
		let name = parser.parse_enter().await?;
		let _ = parser.expect(T![ON])?;
		let _ = parser.eat(T![TABLE])?;
		let table = parser.parse_enter().await?;

		let mut index = None;
		let mut concurrently = None;
		let mut comment = None;
		let mut fields = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, peek.span, Parser::parse_enter)
						.await?;
				}
				T![FIELDS] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut fields, peek.span, async |parser| {
						parse_seperated_list(parser, T![,], Parser::parse_enter).await.map(|x| x.1)
					})
					.await?;
				}
				T![CONCURRENTLY] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut concurrently, peek.span, |_| Ok(()))?;
				}
				T![COUNT] => {
					let _ = parser.next();

					if let Some((_, old_span)) = index {
						return Err(index_type_redefined(parser, peek.span, old_span));
					}

					let condition = if parser.eat(T![WHERE])?.is_some() {
						Some(parser.parse_enter().await?)
					} else {
						None
					};

					let span = parser.span_since(peek.span);
					index = Some((
						ast::Index::Count(CountIndex {
							condition,
							span,
						}),
						span,
					));
				}
				T![HNSW] => {
					if let Some((_, old_span)) = index {
						return Err(index_type_redefined(parser, peek.span, old_span));
					}

					let idx = parser.parse_sync()?;

					let span = parser.span_since(peek.span);
					index = Some((ast::Index::Hnsw(idx), span));
				}
				T![FULLTEXT] => {
					if let Some((_, old_span)) = index {
						return Err(index_type_redefined(parser, peek.span, old_span));
					}

					let idx = parser.parse_sync()?;

					let span = parser.span_since(peek.span);
					index = Some((ast::Index::FullText(idx), span));
				}
				T![UNIQUE] => {
					let _ = parser.next();

					if let Some((_, old_span)) = index {
						return Err(index_type_redefined(parser, peek.span, old_span));
					}

					index = Some((ast::Index::Unique(peek.span), peek.span));
				}
				_ => break,
			}
		}

		let span = parser.span_since(define.span);
		Ok(ast::DefineIndex {
			kind,
			name,
			table,
			fields: fields.map(|x| x.0),
			index: index.map(|x| x.0),
			comment: comment.map(|x| x.0),
			concurrently: concurrently.is_some(),
			span,
		})
	}
}

impl ParseSync for ast::Filter {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let peek = parser.peek_expect("a analyzer filter")?;
		match peek.token {
			T![ASCII] => {
				let _ = parser.next();
				Ok(ast::Filter::Ascii(peek.span))
			}
			T![LOWERCASE] => {
				let _ = parser.next();
				Ok(ast::Filter::Lowercase(peek.span))
			}
			T![UPPERCASE] => {
				let _ = parser.next();
				Ok(ast::Filter::Uppercase(peek.span))
			}
			T![EDGENGRAM] => {
				let _ = parser.next();
				let open = parser.expect(BaseTokenKind::OpenParen)?;
				let min = parser.parse_sync()?;
				let _ = parser.expect(T![,])?;
				let max = parser.parse_sync()?;
				let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseParen, open.span)?;
				let span = parser.span_since(peek.span);
				Ok(ast::Filter::EdgeNgram(ast::NgramMapper {
					min,
					max,
					span,
				}))
			}
			T![NGRAM] => {
				let _ = parser.next();
				let open = parser.expect(BaseTokenKind::OpenParen)?;
				let min = parser.parse_sync()?;
				let _ = parser.expect(T![,])?;
				let max = parser.parse_sync()?;
				let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseParen, open.span)?;
				let span = parser.span_since(peek.span);
				Ok(ast::Filter::Ngram(ast::NgramMapper {
					min,
					max,
					span,
				}))
			}
			T![SNOWBALL] => {
				let _ = parser.next();
				let open = parser.expect(BaseTokenKind::OpenParen)?;
				let language = parser.parse_sync()?;
				let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseParen, open.span)?;
				Ok(ast::Filter::Snowball(language))
			}
			T![MAPPER] => {
				let _ = parser.next();
				let open = parser.expect(BaseTokenKind::OpenParen)?;
				let mapper = parser.parse_sync()?;
				let _ = parser.expect_closing_delimiter(BaseTokenKind::CloseParen, open.span)?;
				Ok(ast::Filter::Mapper(mapper))
			}
			_ => Err(parser.unexpected("a analyzer filter")),
		}
	}
}

impl Parse for ast::DefineAnalyzer {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![ANALYZER])?;
		let kind = parser.parse_sync()?;

		let name = parser.parse_enter().await?;

		let mut comment = None;
		let mut function = None;
		let mut tokenizer = None;
		let mut filters = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, peek.span, Parser::parse_enter)
						.await?;
				}
				T![FUNCTION] => {
					let _ = parser.next();
					parse_unordered_clause_sync(
						parser,
						&mut function,
						peek.span,
						Parser::parse_sync,
					)?;
				}
				T![TOKENIZERS] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut tokenizer, peek.span, |parser| {
						parse_seperated_list_sync(parser, T![,], Parser::parse_sync).map(|x| x.1)
					})?;
				}
				T![FILTERS] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut filters, peek.span, |parser| {
						parse_seperated_list_sync(parser, T![,], Parser::parse_sync).map(|x| x.1)
					})?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(define.span);
		Ok(ast::DefineAnalyzer {
			kind,
			name,
			filters: filters.map(|x| x.0),
			tokenizer: tokenizer.map(|x| x.0),
			function: function.map(|x| x.0),
			comment: comment.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::DefineBucket {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![BUCKET])?;
		let kind = parser.parse_sync()?;

		let name = parser.parse_enter().await?;

		let mut comment = None;
		let mut backend = None;
		let mut permission = None;
		let mut readonly = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![PERMISSIONS] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut permission, peek.span, Parser::parse)
						.await?;
				}
				T![BACKEND] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut backend, peek.span, Parser::parse_enter)
						.await?;
				}
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, peek.span, Parser::parse_enter)
						.await?;
				}
				T![READONLY] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut readonly, peek.span, |_| Ok(()))?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(define.span);
		Ok(ast::DefineBucket {
			kind,
			name,
			comment: comment.map(|x| x.0),
			backend: backend.map(|x| x.0),
			permission: permission.map(|x| x.0),
			readonly: readonly.is_some(),
			span,
		})
	}
}

impl Parse for ast::DefineSequence {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![SEQUENCE])?;
		let kind = parser.parse_sync()?;

		let name = parser.parse_enter().await?;

		let mut batch = None;
		let mut start = None;
		let mut timeout = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![BATCH] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut batch, peek.span, Parser::parse_enter)
						.await?;
				}
				T![START] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut start, peek.span, Parser::parse_enter)
						.await?;
				}
				T![TIMEOUT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut timeout, peek.span, Parser::parse_enter)
						.await?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(define.span);
		Ok(ast::DefineSequence {
			kind,
			name,
			batch: batch.map(|x| x.0),
			start: start.map(|x| x.0),
			timeout: timeout.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::DefineConfigApi {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![API])?;

		let mut permission = None;
		let mut middleware = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![PERMISSIONS] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut permission, peek.span, Parser::parse)
						.await?;
				}
				T![MIDDLEWARE] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut middleware, peek.span, async |parser| {
						parse_seperated_list(parser, T![,], Parser::parse).await.map(|x| x.1)
					})
					.await?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(start.span);
		Ok(ast::DefineConfigApi {
			permission: permission.map(|x| x.0),
			middleware: middleware.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::DefineConfigGraphql {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![GRAPHQL])?;

		let mut functions = None;
		let mut tables = None;
		let mut depth = None;
		let mut complexity = None;
		let mut introspection = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};

			match peek.token {
				T![NONE] => {
					let _ = parser.next();
					functions = Some((ast::FunctionConfig::None, peek.span));
					tables = Some((ast::TablesConfig::None, peek.span));
				}
				T![AUTO] => {
					let _ = parser.next();
					functions = Some((ast::FunctionConfig::Auto, peek.span));
					tables = Some((ast::TablesConfig::Auto, peek.span));
				}
				T![TABLES] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut tables, peek.span, |parser| {
						let peek = parser.peek_expect("`INCLUDE`, `EXCLUDE`, `NONE`, or `AUTO`")?;
						match peek.token {
							T![INCLUDE] => {
								let _ = parser.next();
								parse_seperated_list_sync(parser, T![,], Parser::parse_sync)
									.map(|x| ast::TablesConfig::Include(x.1))
							}
							T![EXCLUDE] => {
								let _ = parser.next();
								parse_seperated_list_sync(parser, T![,], Parser::parse_sync)
									.map(|x| ast::TablesConfig::Exclude(x.1))
							}
							T![NONE] => {
								let _ = parser.next();
								Ok(ast::TablesConfig::None)
							}
							T![AUTO] => {
								let _ = parser.next();
								Ok(ast::TablesConfig::Auto)
							}
							_ => Err(parser.unexpected("`INCLUDE`, `EXCLUDE`, `NONE`, or `AUTO`")),
						}
					})?;
				}
				T![FUNCTIONS] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut functions, peek.span, |parser| {
						let peek = parser.peek_expect("`INCLUDE`, `EXCLUDE`, `NONE`, or `AUTO`")?;
						match peek.token {
							T![INCLUDE] => {
								let _ = parser.next();
								parse_seperated_list_sync(parser, T![,], Parser::parse_sync)
									.map(|x| ast::FunctionConfig::Include(x.1))
							}
							T![EXCLUDE] => {
								let _ = parser.next();
								parse_seperated_list_sync(parser, T![,], Parser::parse_sync)
									.map(|x| ast::FunctionConfig::Exclude(x.1))
							}
							T![NONE] => {
								let _ = parser.next();
								Ok(ast::FunctionConfig::None)
							}
							T![AUTO] => {
								let _ = parser.next();
								Ok(ast::FunctionConfig::Auto)
							}
							_ => Err(parser.unexpected("`INCLUDE`, `EXCLUDE`, `NONE`, or `AUTO`")),
						}
					})?;
				}
				T![DEPTH] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut depth, peek.span, Parser::parse_sync)?
				}
				T![COMPLEXITY] => {
					let _ = parser.next();
					parse_unordered_clause_sync(
						parser,
						&mut complexity,
						peek.span,
						Parser::parse_sync,
					)?
				}
				T![INTROSPECTION] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut introspection, peek.span, |parser| {
						let peek = parser.peek_expect("`AUTO` or `NONE`")?;
						match peek.token {
							T![AUTO] => {
								let _ = parser.next();
								Ok(ast::GraphqlIntrospection::Auto)
							}
							T![NONE] => {
								let _ = parser.next();
								Ok(ast::GraphqlIntrospection::None)
							}
							_ => Err(parser.unexpected("`AUTO` or `NONE`")),
						}
					})?
				}
				_ => break,
			}
		}

		let span = parser.span_since(start.span);
		Ok(ast::DefineConfigGraphql {
			table_config: tables.map(|x| x.0),
			function_config: functions.map(|x| x.0),
			depth_limit: depth.map(|x| x.0),
			complexity_limit: complexity.map(|x| x.0),
			introspection: introspection.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::DefineConfigDefault {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let start = parser.expect(T![DEFAULT])?;

		let mut namespace = None;
		let mut database = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![NAMESPACE] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut namespace, peek.span, Parser::parse_enter)
						.await?;
				}
				T![DATABASE] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut database, peek.span, Parser::parse_enter)
						.await?;
				}
				_ => break,
			}
		}

		let span = parser.span_since(start.span);
		Ok(ast::DefineConfigDefault {
			namespace: namespace.map(|x| x.0),
			database: database.map(|x| x.0),
			span,
		})
	}
}

impl Parse for ast::DefineConfig {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![CONFIG])?;
		let kind = parser.parse_sync()?;

		let peek = parser.peek_expect("`API`, `GRAPHQL` or `DEFAULT`")?;
		let inner = match peek.token {
			T![API] => DefineConfigKind::Api(parser.parse().await?),
			T![GRAPHQL] => DefineConfigKind::Graphql(parser.parse().await?),
			T![DEFAULT] => DefineConfigKind::Default(parser.parse().await?),
			_ => return Err(parser.unexpected("`API`, `GRAPHQL` or `DEFAULT`")),
		};

		let span = parser.span_since(define.span);
		Ok(ast::DefineConfig {
			kind,
			inner,
			span,
		})
	}
}

impl Parse for ast::DefineUser {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![USER])?;
		let kind = parser.parse_sync()?;

		let name = parser.parse_enter().await?;
		let _ = parser.expect(T![ON])?;
		let base = parser.parse_sync()?;

		let mut comment = None;
		let mut secret = None;
		let mut roles = None;
		let mut token_duration = None;
		let mut session_duration = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, peek.span, Parser::parse_enter)
						.await?;
				}
				T![PASSWORD] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut secret, peek.span, |parser| {
						parser.parse_sync().map(UserSecret::PassWord)
					})?;
				}
				T![PASSHASH] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut secret, peek.span, |parser| {
						parser.parse_sync().map(UserSecret::PassHash)
					})?;
				}
				T![ROLES] => {
					let _ = parser.next();
					parse_unordered_clause_sync(parser, &mut roles, peek.span, |parser| {
						parse_seperated_list_sync(parser, T![,], Parser::parse_sync).map(|x| x.1)
					})?;
				}
				T![DURATION] => {
					let _ = parser.next();
					let _ = parser.expect(T![FOR])?;
					loop {
						let expect = "`TOKEN` or `SESSION`";
						let token = parser.peek_expect(expect)?;
						match token.token {
							T![TOKEN] => {
								let _ = parser.next();
								parse_unordered_clause(
									parser,
									&mut token_duration,
									peek.span,
									Parser::parse_enter,
								)
								.await?
							}
							T![SESSION] => {
								let _ = parser.next();
								parse_unordered_clause(
									parser,
									&mut session_duration,
									peek.span,
									Parser::parse_enter,
								)
								.await?
							}
							_ => return Err(parser.unexpected(expect)),
						}
						let _ = parser.eat(T![,])?;
						if parser.eat(T![FOR])?.is_none() {
							break;
						}
					}
				}
				_ => break,
			}
		}

		let span = parser.span_since(define.span);
		Ok(ast::DefineUser {
			kind,
			name,
			base,
			comment: comment.map(|x| x.0),
			secret: secret.map(|x| x.0),
			roles: roles.map(|x| x.0),
			session_duration: session_duration.map(|x| x.0),
			token_duration: token_duration.map(|x| x.0),
			span,
		})
	}
}

impl ParseSync for ast::Algorithm {
	fn parse_sync(parser: &mut Parser) -> ParseResult<Self> {
		let expect = "an jwt encoding algorithm";
		let peek = parser.peek_expect(expect)?;
		let res = match peek.token {
			T![EDDSA] => ast::Algorithm::EdDSA,
			T![ES256] => ast::Algorithm::Es256,
			T![ES384] => ast::Algorithm::Es384,
			T![ES512] => ast::Algorithm::Es512,
			T![HS256] => ast::Algorithm::Hs256,
			T![HS384] => ast::Algorithm::Hs384,
			T![HS512] => ast::Algorithm::Hs512,
			T![PS256] => ast::Algorithm::Ps256,
			T![PS384] => ast::Algorithm::Ps384,
			T![PS512] => ast::Algorithm::Ps512,
			T![RS256] => ast::Algorithm::Rs256,
			T![RS384] => ast::Algorithm::Rs384,
			T![RS512] => ast::Algorithm::Rs512,
			_ => return Err(parser.unexpected(expect)),
		};
		let _ = parser.next();
		Ok(res)
	}
}

impl Parse for ast::Jwt {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let expect = "`ALGORITHM` or `URL`";
		let peek = parser.peek_expect(expect)?;
		let verify = match peek.token {
			T![ALGORITHM] => {
				let _ = parser.next();
				let algorithm = parser.parse_sync()?;
				let _ = parser.expect(T![KEY])?;
				let key = parser.parse_enter().await?;
				ast::JwtVerify::Key {
					algorithm,
					key,
				}
			}
			T![URL] => {
				let _ = parser.next();
				let url = parser.parse_enter().await?;
				ast::JwtVerify::Jwks {
					url,
				}
			}
			_ => return Err(parser.unexpected(expect)),
		};

		let issue = if parser.eat(T![WITH])?.is_some() {
			let _ = parser.expect(T![ISSUER])?;
			let mut algorithm = None;
			let mut key = None;
			loop {
				let Some(peek) = parser.peek()? else {
					break;
				};
				match peek.token {
					T![ALGORITHM] => {
						let _ = parser.next();
						parse_unordered_clause_sync(
							parser,
							&mut algorithm,
							peek.span,
							Parser::parse_sync,
						)?;
					}
					T![KEY] => {
						let _ = parser.next();
						parse_unordered_clause(parser, &mut key, peek.span, Parser::parse_enter)
							.await?;
					}
					_ => break,
				}
			}
			Some(ast::JwtIssue {
				algorithm: algorithm.map(|x| x.0),
				key: key.map(|x| x.0),
			})
		} else {
			None
		};

		Ok(ast::Jwt {
			verify,
			issue,
		})
	}
}

impl Parse for ast::RecordAccess {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let mut signup = None;
		let mut signin = None;
		let mut jwt = None;
		let mut refresh = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![SIGNIN] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut signin, peek.span, Parser::parse_enter)
						.await?;
				}
				T![SIGNUP] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut signup, peek.span, Parser::parse_enter)
						.await?;
				}
				T![WITH] => {
					let _ = parser.next();
					let expect = "`JWT` or `REFRESH`";
					let peek_with = parser.peek_expect(expect)?;
					match peek_with.token {
						T![JWT] => {
							let _ = parser.next();
							parse_unordered_clause(
								parser,
								&mut jwt,
								peek.span.extend(peek_with.span),
								Parser::parse,
							)
							.await?;
						}
						T![REFRESH] => {
							let _ = parser.next();
							parse_unordered_clause_sync(
								parser,
								&mut refresh,
								peek.span.extend(peek_with.span),
								|_| Ok(()),
							)?;
						}
						_ => return Err(parser.unexpected(expect)),
					}
				}
				_ => break,
			}

			parser.eat(T![,])?;
		}

		Ok(ast::RecordAccess {
			signup: signup.map(|x| x.0),
			signin: signin.map(|x| x.0),
			jwt: jwt.map(|x| x.0),
			refresh: refresh.is_some(),
		})
	}
}

impl Parse for ast::DefineAccess {
	async fn parse(parser: &mut Parser<'_, '_>) -> ParseResult<Self> {
		let define = parser.expect(T![DEFINE])?;
		let _ = parser.expect(T![ACCESS])?;
		let kind = parser.parse_sync()?;

		let name = parser.parse_enter().await?;
		let _ = parser.expect(T![ON])?;
		let base = parser.parse_sync()?;

		let span = parser.span_since(define.span);

		let mut comment = None;
		let mut duration_session = None;
		let mut duration_token = None;
		let mut duration_grant = None;
		let mut authenticate = None;
		let mut ty = None;
		loop {
			let Some(peek) = parser.peek()? else {
				break;
			};
			match peek.token {
				T![COMMENT] => {
					let _ = parser.next();
					parse_unordered_clause(parser, &mut comment, peek.span, Parser::parse_enter)
						.await?;
				}
				T![AUTHENTICATE] => {
					let _ = parser.next();
					parse_unordered_clause(
						parser,
						&mut authenticate,
						peek.span,
						Parser::parse_enter,
					)
					.await?;
				}
				T![DURATION] => {
					let _ = parser.next();
					let _ = parser.expect(T![FOR])?;
					loop {
						let expect = "`GRANT`, `TOKEN`, or `SESSION`";
						let peek = parser.peek_expect(expect)?;
						match peek.token {
							T![SESSION] => {
								let _ = parser.next();
								parse_unordered_clause(
									parser,
									&mut duration_session,
									peek.span,
									Parser::parse_enter,
								)
								.await?
							}
							T![TOKEN] => {
								let _ = parser.next();
								parse_unordered_clause(
									parser,
									&mut duration_token,
									peek.span,
									Parser::parse_enter,
								)
								.await?
							}
							T![GRANT] => {
								let _ = parser.next();
								parse_unordered_clause(
									parser,
									&mut duration_grant,
									peek.span,
									Parser::parse_enter,
								)
								.await?
							}
							_ => return Err(parser.unexpected(expect)),
						}

						let _ = parser.eat(T![,])?;
						if parser.eat(T![FOR])?.is_none() {
							break;
						}
					}
				}
				T![TYPE] => {
					let _ = parser.next();
					let expect = "`JWT`, `RECORD`, or `BEARER`";
					let peek_type = parser.peek_expect(expect)?;
					match peek_type.token {
						T![JWT] => {
							let _ = parser.next();
							parse_unordered_clause(
								parser,
								&mut ty,
								peek.span.extend(peek_type.span),
								async |parser| parser.parse().await.map(AccessType::Jwt),
							)
							.await?;
						}
						T![RECORD] => {
							let _ = parser.next();
							parse_unordered_clause(
								parser,
								&mut ty,
								peek.span.extend(peek_type.span),
								async |parser| {
									let ast::Base::Database = base else {
										return Err(parser.with_error(|parser| {
											Level::Error
												.title(format!(
													"Unexpected token `{}`, record access can only be defined on a database",
													parser.slice(peek.span)
												))
												.snippet(parser.snippet().annotate(
													AnnotationKind::Primary.span(peek.span),
												))
												.to_diagnostic()
										}));
									};

									parser.parse().await.map(AccessType::Record)
								},
							)
							.await?;
						}
						T![BEARER] => {
							let _ = parser.next();
							parse_unordered_clause(
								parser,
								&mut ty,
								peek.span.extend(peek_type.span),
								async |parser| {
									let _ = parser.expect(T![FOR])?;
									let expect = if matches!(base, Base::Database) {
										"`USER` or `RECORD`"
									} else {
										"`USER`"
									};
									let peek = parser.peek_expect(expect)?;
									let subject = match peek.token {
										T![USER] => {
											let _ = parser.next();
											ast::BearerAccessSubject::User
										}
										T![RECORD] if matches!(base, Base::Database) => {
											let _ = parser.next();
											ast::BearerAccessSubject::Record
										}
										_ => return Err(parser.unexpected(expect)),
									};

									let jwt = if parser.eat(T![WITH])?.is_some() {
										Some(parser.parse().await?)
									} else {
										None
									};

									Ok(AccessType::Bearer(ast::BearerAccess {
										subject,
										jwt,
									}))
								},
							)
							.await?
						}
						_ => return Err(parser.unexpected(expect)),
					}
				}
				_ => break,
			}
		}

		Ok(ast::DefineAccess {
			kind,
			name,
			base,
			comment: comment.map(|x| x.0),
			duration_grant: duration_grant.map(|x| x.0),
			duration_session: duration_session.map(|x| x.0),
			duration_token: duration_token.map(|x| x.0),
			authenticate: authenticate.map(|x| x.0),
			ty: ty.map(|x| x.0),
			span,
		})
	}
}
