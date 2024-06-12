use geo::Point;
use reblessive::Stk;

use super::{ParseResult, Parser};
use crate::{
	enter_object_recursion, enter_query_recursion,
	sql::{
		Array, Dir, Function, Geometry, Ident, Idiom, Mock, Part, Script, Strand, Subquery, Table,
		Value,
	},
	syn::{
		lexer::Lexer,
		parser::{
			mac::{expected, unexpected},
			ParseError, ParseErrorKind,
		},
		token::{t, NumberKind, Span, TokenKind},
	},
};

impl Parser<'_> {
	/// Parse a what primary.
	///
	/// What's are values which are more restricted in what expressions they can contain.
	pub async fn parse_what_primary(&mut self, ctx: &mut Stk) -> ParseResult<Value> {
		match self.peek_kind() {
			TokenKind::Duration => {
				let duration = self.next_token_value()?;
				Ok(Value::Duration(duration))
			}
			TokenKind::DateTime => {
				let datetime = self.next_token_value()?;
				Ok(Value::Datetime(datetime))
			}
			t!("r\"") => {
				self.pop_peek();
				let thing = self.parse_record_string(ctx, true).await?;
				Ok(Value::Thing(thing))
			}
			t!("r'") => {
				self.pop_peek();
				let thing = self.parse_record_string(ctx, false).await?;
				Ok(Value::Thing(thing))
			}
			t!("$param") => {
				let param = self.next_token_value()?;
				Ok(Value::Param(param))
			}
			t!("FUNCTION") => {
				self.pop_peek();
				let func = self.parse_script(ctx).await?;
				Ok(Value::Function(Box::new(func)))
			}
			t!("IF") => {
				let stmt = ctx.run(|ctx| self.parse_if_stmt(ctx)).await?;
				Ok(Value::Subquery(Box::new(Subquery::Ifelse(stmt))))
			}
			t!("(") => {
				let token = self.pop_peek();
				self.parse_inner_subquery(ctx, Some(token.span))
					.await
					.map(|x| Value::Subquery(Box::new(x)))
			}
			t!("<") => {
				self.pop_peek();
				expected!(self, t!("FUTURE"));
				expected!(self, t!(">"));
				let start = expected!(self, t!("{")).span;
				let block = self.parse_block(ctx, start).await?;
				Ok(Value::Future(Box::new(crate::sql::Future(block))))
			}
			t!("|") => {
				let start = self.pop_peek().span;
				self.parse_mock(start).map(Value::Mock)
			}
			t!("/") => {
				let token = self.pop_peek();
				let regex = self.lexer.relex_regex(token);
				self.token_value(regex).map(Value::Regex)
			}
			t!("RETURN")
			| t!("SELECT")
			| t!("CREATE")
			| t!("UPSERT")
			| t!("UPDATE")
			| t!("DELETE")
			| t!("RELATE")
			| t!("DEFINE")
			| t!("REMOVE")
			| t!("REBUILD") => {
				self.parse_inner_subquery(ctx, None).await.map(|x| Value::Subquery(Box::new(x)))
			}
			t!("fn") => self.parse_custom_function(ctx).await.map(|x| Value::Function(Box::new(x))),
			t!("ml") => self.parse_model(ctx).await.map(|x| Value::Model(Box::new(x))),
			x => {
				if !self.peek_can_be_ident() {
					unexpected!(self, x, "a value")
				}

				let token = self.next();
				match self.peek_kind() {
					t!("::") | t!("(") => self.parse_builtin(ctx, token.span).await,
					t!(":") => {
						let str = self.token_value::<Ident>(token)?.0;
						self.parse_thing_or_range(ctx, str).await
					}
					x => {
						if x.has_data() {
							// x had data and possibly overwrote the data from token, This is
							// always an invalid production so just return error.
							unexpected!(self, x, "a value");
						} else {
							Ok(Value::Table(self.token_value(token)?))
						}
					}
				}
			}
		}
	}

	/// Parse an expressions
	pub async fn parse_idiom_expression(&mut self, ctx: &mut Stk) -> ParseResult<Value> {
		let token = self.peek();
		let value = match token.kind {
			t!("NONE") => {
				self.pop_peek();
				return Ok(Value::None);
			}
			t!("NULL") => {
				self.pop_peek();
				return Ok(Value::Null);
			}
			t!("true") => {
				self.pop_peek();
				return Ok(Value::Bool(true));
			}
			t!("false") => {
				self.pop_peek();
				return Ok(Value::Bool(false));
			}
			t!("<") => {
				self.pop_peek();
				// Casting should already have been parsed.
				expected!(self, t!("FUTURE"));
				self.expect_closing_delimiter(t!(">"), token.span)?;
				let next = expected!(self, t!("{")).span;
				let block = self.parse_block(ctx, next).await?;
				return Ok(Value::Future(Box::new(crate::sql::Future(block))));
			}
			TokenKind::Strand => {
				self.pop_peek();
				if self.legacy_strands {
					return self.parse_legacy_strand(ctx).await;
				} else {
					let strand = self.token_value(token)?;
					return Ok(Value::Strand(strand));
				}
			}
			TokenKind::Duration => {
				self.pop_peek();
				let duration = self.token_value(token)?;
				Value::Duration(duration)
			}
			TokenKind::Number(_) => {
				self.pop_peek();
				let number = self.token_value(token)?;
				Value::Number(number)
			}
			TokenKind::Uuid => {
				self.pop_peek();
				let uuid = self.token_value(token)?;
				Value::Uuid(uuid)
			}
			TokenKind::DateTime => {
				self.pop_peek();
				let datetime = self.token_value(token)?;
				Value::Datetime(datetime)
			}
			t!("r\"") => {
				self.pop_peek();
				let thing = self.parse_record_string(ctx, true).await?;
				Value::Thing(thing)
			}
			t!("r'") => {
				self.pop_peek();
				let thing = self.parse_record_string(ctx, false).await?;
				Value::Thing(thing)
			}
			t!("$param") => {
				self.pop_peek();
				let param = self.token_value(token)?;
				Value::Param(param)
			}
			t!("FUNCTION") => {
				self.pop_peek();
				let script = self.parse_script(ctx).await?;
				Value::Function(Box::new(script))
			}
			t!("->") => {
				self.pop_peek();
				let graph = ctx.run(|ctx| self.parse_graph(ctx, Dir::Out)).await?;
				Value::Idiom(Idiom(vec![Part::Graph(graph)]))
			}
			t!("<->") => {
				self.pop_peek();
				let graph = ctx.run(|ctx| self.parse_graph(ctx, Dir::Both)).await?;
				Value::Idiom(Idiom(vec![Part::Graph(graph)]))
			}
			t!("<-") => {
				self.pop_peek();
				let graph = ctx.run(|ctx| self.parse_graph(ctx, Dir::In)).await?;
				Value::Idiom(Idiom(vec![Part::Graph(graph)]))
			}
			t!("[") => {
				self.pop_peek();
				self.parse_array(ctx, token.span).await.map(Value::Array)?
			}
			t!("{") => {
				self.pop_peek();
				self.parse_object_like(ctx, token.span).await?
			}
			t!("|") => {
				self.pop_peek();
				self.parse_mock(token.span).map(Value::Mock)?
			}
			t!("IF") => {
				enter_query_recursion!(this = self => {
					this.pop_peek();
					let stmt = ctx.run(|ctx| this.parse_if_stmt(ctx)).await?;
					Value::Subquery(Box::new(Subquery::Ifelse(stmt)))
				})
			}
			t!("(") => {
				self.pop_peek();
				self.parse_inner_subquery_or_coordinate(ctx, token.span).await?
			}
			t!("/") => {
				self.pop_peek();
				let regex = self.lexer.relex_regex(token);
				self.token_value(regex).map(Value::Regex)?
			}
			t!("RETURN")
			| t!("SELECT")
			| t!("CREATE")
			| t!("UPSERT")
			| t!("UPDATE")
			| t!("DELETE")
			| t!("RELATE")
			| t!("DEFINE")
			| t!("REMOVE")
			| t!("REBUILD") => {
				self.parse_inner_subquery(ctx, None).await.map(|x| Value::Subquery(Box::new(x)))?
			}
			t!("fn") => {
				self.pop_peek();
				self.parse_custom_function(ctx).await.map(|x| Value::Function(Box::new(x)))?
			}
			t!("ml") => {
				self.pop_peek();
				self.parse_model(ctx).await.map(|x| Value::Model(Box::new(x)))?
			}
			_ => {
				self.pop_peek();
				match self.peek_kind() {
					t!("::") | t!("(") => self.parse_builtin(ctx, token.span).await?,
					t!(":") => {
						let str = self.token_value::<Ident>(token)?.0;
						self.parse_thing_or_range(ctx, str).await?
					}
					x => {
						if x.has_data() {
							unexpected!(self, x, "a value");
						} else if self.table_as_field {
							Value::Idiom(Idiom(vec![Part::Field(self.token_value(token)?)]))
						} else {
							Value::Table(self.token_value(token)?)
						}
					}
				}
			}
		};

		// Parse the rest of the idiom if it is being continued.
		if Self::continues_idiom(self.peek_kind()) {
			match value {
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::Future(_)
				| Value::Strand(_) => unreachable!(),
				Value::Idiom(Idiom(x)) => self.parse_remaining_value_idiom(ctx, x).await,
				Value::Table(Table(x)) => {
					self.parse_remaining_value_idiom(ctx, vec![Part::Field(Ident(x))]).await
				}
				x => self.parse_remaining_value_idiom(ctx, vec![Part::Start(x)]).await,
			}
		} else {
			Ok(value)
		}
	}

	/// Parses an array production
	///
	/// # Parser state
	/// Expects the starting `[` to already be eaten and its span passed as an argument.
	pub async fn parse_array(&mut self, ctx: &mut Stk, start: Span) -> ParseResult<Array> {
		let mut values = Vec::new();
		enter_object_recursion!(this = self => {
			loop {
				if this.eat(t!("]")) {
					break;
				}

				let value = ctx.run(|ctx| this.parse_value_field(ctx)).await?;
				values.push(value);

				if !this.eat(t!(",")) {
					this.expect_closing_delimiter(t!("]"), start)?;
					break;
				}
			}
		});

		Ok(Array(values))
	}

	/// Parse a mock `|foo:1..3|`
	///
	/// # Parser State
	/// Expects the starting `|` already be eaten and its span passed as an argument.
	pub fn parse_mock(&mut self, start: Span) -> ParseResult<Mock> {
		let name = self.next_token_value::<Ident>()?.0;
		expected!(self, t!(":"));
		let from = self.next_token_value()?;
		let to = self.eat(t!("..")).then(|| self.next_token_value()).transpose()?;
		self.expect_closing_delimiter(t!("|"), start)?;
		if let Some(to) = to {
			Ok(Mock::Range(name, from, to))
		} else {
			Ok(Mock::Count(name, from))
		}
	}

	pub async fn parse_full_subquery(&mut self, ctx: &mut Stk) -> ParseResult<Subquery> {
		let peek = self.peek();
		match peek.kind {
			t!("(") => {
				self.pop_peek();
				self.parse_inner_subquery(ctx, Some(peek.span)).await
			}
			t!("IF") => {
				enter_query_recursion!(this = self => {
					this.pop_peek();
					let if_stmt = ctx.run(|ctx| this.parse_if_stmt(ctx)).await?;
					Ok(Subquery::Ifelse(if_stmt))
				})
			}
			_ => self.parse_inner_subquery(ctx, None).await,
		}
	}

	pub async fn parse_inner_subquery_or_coordinate(
		&mut self,
		ctx: &mut Stk,
		start: Span,
	) -> ParseResult<Value> {
		enter_query_recursion!(this = self => {
			this.parse_inner_subquery_or_coordinate_inner(ctx,start).await
		})
	}

	async fn parse_inner_subquery_or_coordinate_inner(
		&mut self,
		ctx: &mut Stk,
		start: Span,
	) -> ParseResult<Value> {
		let peek = self.peek();
		let res = match peek.kind {
			t!("RETURN") => {
				self.pop_peek();
				let stmt = ctx.run(|ctx| self.parse_return_stmt(ctx)).await?;
				Subquery::Output(stmt)
			}
			t!("SELECT") => {
				self.pop_peek();
				let stmt = ctx.run(|ctx| self.parse_select_stmt(ctx)).await?;
				Subquery::Select(stmt)
			}
			t!("CREATE") => {
				self.pop_peek();
				let stmt = ctx.run(|ctx| self.parse_create_stmt(ctx)).await?;
				Subquery::Create(stmt)
			}
			t!("UPSERT") => {
				self.pop_peek();
				let stmt = ctx.run(|ctx| self.parse_upsert_stmt(ctx)).await?;
				Subquery::Upsert(stmt)
			}
			t!("UPDATE") => {
				self.pop_peek();
				let stmt = ctx.run(|ctx| self.parse_update_stmt(ctx)).await?;
				Subquery::Update(stmt)
			}
			t!("DELETE") => {
				self.pop_peek();
				let stmt = ctx.run(|ctx| self.parse_delete_stmt(ctx)).await?;
				Subquery::Delete(stmt)
			}
			t!("RELATE") => {
				self.pop_peek();
				let stmt = ctx.run(|ctx| self.parse_relate_stmt(ctx)).await?;
				Subquery::Relate(stmt)
			}
			t!("DEFINE") => {
				self.pop_peek();
				let stmt = ctx.run(|ctx| self.parse_define_stmt(ctx)).await?;
				Subquery::Define(stmt)
			}
			t!("REMOVE") => {
				self.pop_peek();
				let stmt = self.parse_remove_stmt()?;
				Subquery::Remove(stmt)
			}
			t!("REBUILD") => {
				self.pop_peek();
				let stmt = self.parse_rebuild_stmt()?;
				Subquery::Rebuild(stmt)
			}
			t!("+") | t!("-") => {
				// handle possible coordinate in the shape of ([-+]?number,[-+]?number)
				if let TokenKind::Number(kind) = self.peek_token_at(1).kind {
					// take the value so we don't overwrite it if the next token happens to be an
					// strand or an ident, both of which are invalid syntax.
					let number_value = self.lexer.string.take().unwrap();
					if self.peek_token_at(2).kind == t!(",") {
						match kind {
							NumberKind::Decimal | NumberKind::NaN => {
								return Err(ParseError::new(
									ParseErrorKind::UnexpectedExplain {
										found: TokenKind::Number(kind),
										expected: "a non-decimal, non-nan number",
										explain: "coordinate numbers can't be NaN or a decimal",
									},
									peek.span,
								));
							}
							_ => {}
						}

						self.lexer.string = Some(number_value);
						let a = self.parse_signed_float()?;
						self.next();
						let b = self.parse_signed_float()?;
						self.expect_closing_delimiter(t!(")"), start)?;
						return Ok(Value::Geometry(Geometry::Point(Point::from((a, b)))));
					}
					self.lexer.string = Some(number_value);
				}
				Subquery::Value(ctx.run(|ctx| self.parse_value_field(ctx)).await?)
			}
			TokenKind::Number(kind) => {
				// handle possible coordinate in the shape of ([-+]?number,[-+]?number)
				// take the value so we don't overwrite it if the next token happens to be an
				// strand or an ident, both of which are invalid syntax.
				let number_value = self.lexer.string.take().unwrap();
				if self.peek_token_at(1).kind == t!(",") {
					match kind {
						NumberKind::Decimal | NumberKind::NaN => {
							return Err(ParseError::new(
								ParseErrorKind::UnexpectedExplain {
									found: TokenKind::Number(kind),
									expected: "a non-decimal, non-nan number",
									explain: "coordinate numbers can't be NaN or a decimal",
								},
								peek.span,
							));
						}
						_ => {}
					}
					self.pop_peek();
					// was a semicolon, put the strand back for code reuse.
					self.lexer.string = Some(number_value);
					let a = self.token_value::<f64>(peek)?;
					// eat the semicolon.
					self.next();
					let b = self.parse_signed_float()?;
					self.expect_closing_delimiter(t!(")"), start)?;
					return Ok(Value::Geometry(Geometry::Point(Point::from((a, b)))));
				}
				self.lexer.string = Some(number_value);
				Subquery::Value(ctx.run(|ctx| self.parse_value_field(ctx)).await?)
			}
			_ => {
				let value = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
				Subquery::Value(value)
			}
		};
		if self.peek_kind() != t!(")") && Self::starts_disallowed_subquery_statement(peek.kind) {
			if let Subquery::Value(Value::Idiom(Idiom(ref idiom))) = res {
				if idiom.len() == 1 {
					// we parsed a single idiom and the next token was a dissallowed statement so
					// it is likely that the used meant to use an invalid statement.
					return Err(ParseError::new(
						ParseErrorKind::DisallowedStatement {
							found: self.peek_kind(),
							expected: t!(")"),
							disallowed: peek.span,
						},
						self.recent_span(),
					));
				}
			}
		}
		self.expect_closing_delimiter(t!(")"), start)?;
		Ok(Value::Subquery(Box::new(res)))
	}

	pub async fn parse_inner_subquery(
		&mut self,
		ctx: &mut Stk,
		start: Option<Span>,
	) -> ParseResult<Subquery> {
		enter_query_recursion!(this = self => {
			this.parse_inner_subquery_inner(ctx,start).await
		})
	}

	async fn parse_inner_subquery_inner(
		&mut self,
		ctx: &mut Stk,
		start: Option<Span>,
	) -> ParseResult<Subquery> {
		let peek = self.peek();
		let res = match peek.kind {
			t!("RETURN") => {
				self.pop_peek();
				let stmt = ctx.run(|ctx| self.parse_return_stmt(ctx)).await?;
				Subquery::Output(stmt)
			}
			t!("SELECT") => {
				self.pop_peek();
				let stmt = ctx.run(|ctx| self.parse_select_stmt(ctx)).await?;
				Subquery::Select(stmt)
			}
			t!("CREATE") => {
				self.pop_peek();
				let stmt = ctx.run(|ctx| self.parse_create_stmt(ctx)).await?;
				Subquery::Create(stmt)
			}
			t!("UPSERT") => {
				self.pop_peek();
				let stmt = ctx.run(|ctx| self.parse_upsert_stmt(ctx)).await?;
				Subquery::Upsert(stmt)
			}
			t!("UPDATE") => {
				self.pop_peek();
				let stmt = ctx.run(|ctx| self.parse_update_stmt(ctx)).await?;
				Subquery::Update(stmt)
			}
			t!("DELETE") => {
				self.pop_peek();
				let stmt = ctx.run(|ctx| self.parse_delete_stmt(ctx)).await?;
				Subquery::Delete(stmt)
			}
			t!("RELATE") => {
				self.pop_peek();
				let stmt = ctx.run(|ctx| self.parse_relate_stmt(ctx)).await?;
				Subquery::Relate(stmt)
			}
			t!("DEFINE") => {
				self.pop_peek();
				let stmt = ctx.run(|ctx| self.parse_define_stmt(ctx)).await?;
				Subquery::Define(stmt)
			}
			t!("REMOVE") => {
				self.pop_peek();
				let stmt = self.parse_remove_stmt()?;
				Subquery::Remove(stmt)
			}
			t!("REBUILD") => {
				self.pop_peek();
				let stmt = self.parse_rebuild_stmt()?;
				Subquery::Rebuild(stmt)
			}
			_ => {
				let value = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
				Subquery::Value(value)
			}
		};
		if let Some(start) = start {
			if self.peek_kind() != t!(")") && Self::starts_disallowed_subquery_statement(peek.kind)
			{
				if let Subquery::Value(Value::Idiom(Idiom(ref idiom))) = res {
					if idiom.len() == 1 {
						// we parsed a single idiom and the next token was a dissallowed statement so
						// it is likely that the used meant to use an invalid statement.
						return Err(ParseError::new(
							ParseErrorKind::DisallowedStatement {
								found: self.peek_kind(),
								expected: t!(")"),
								disallowed: peek.span,
							},
							self.recent_span(),
						));
					}
				}
			}

			self.expect_closing_delimiter(t!(")"), start)?;
		}
		Ok(res)
	}

	fn starts_disallowed_subquery_statement(kind: TokenKind) -> bool {
		matches!(
			kind,
			t!("ANALYZE")
				| t!("BEGIN") | t!("BREAK")
				| t!("CANCEL") | t!("COMMIT")
				| t!("CONTINUE") | t!("FOR")
				| t!("INFO") | t!("KILL")
				| t!("LIVE") | t!("OPTION")
				| t!("LET") | t!("SHOW")
				| t!("SLEEP") | t!("THROW")
				| t!("USE")
		)
	}

	/// Parses a strand with legacy rules, parsing to a record id, datetime or uuid if the string
	/// matches.
	pub async fn parse_legacy_strand(&mut self, ctx: &mut Stk) -> ParseResult<Value> {
		let text = self.lexer.string.take().unwrap();
		if let Ok(x) = Parser::new(text.as_bytes()).parse_thing(ctx).await {
			return Ok(Value::Thing(x));
		}
		if let Ok(x) = Lexer::new(text.as_bytes()).lex_only_datetime() {
			return Ok(Value::Datetime(x));
		}
		if let Ok(x) = Lexer::new(text.as_bytes()).lex_only_uuid() {
			return Ok(Value::Uuid(x));
		}
		Ok(Value::Strand(Strand(text)))
	}

	async fn parse_script(&mut self, ctx: &mut Stk) -> ParseResult<Function> {
		let start = expected!(self, t!("(")).span;
		let mut args = Vec::new();
		loop {
			if self.eat(t!(")")) {
				break;
			}

			let arg = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
			args.push(arg);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!(")"), start)?;
				break;
			}
		}
		expected!(self, t!("{"));
		let body = self
			.lexer
			.lex_js_function_body()
			.map_err(|(e, span)| ParseError::new(ParseErrorKind::InvalidToken(e), span))?;
		Ok(Function::Script(Script(body), args))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::syn::Parse;

	#[test]
	fn subquery_expression_statement() {
		let sql = "(1 + 2 + 3)";
		let out = Value::parse(sql);
		assert_eq!("(1 + 2 + 3)", format!("{}", out))
	}

	#[test]
	fn subquery_ifelse_statement() {
		let sql = "IF true THEN false END";
		let out = Value::parse(sql);
		assert_eq!("IF true THEN false END", format!("{}", out))
	}

	#[test]
	fn subquery_select_statement() {
		let sql = "(SELECT * FROM test)";
		let out = Value::parse(sql);
		assert_eq!("(SELECT * FROM test)", format!("{}", out))
	}

	#[test]
	fn subquery_define_statement() {
		let sql = "(DEFINE EVENT foo ON bar WHEN $event = 'CREATE' THEN (CREATE x SET y = 1))";
		let out = Value::parse(sql);
		assert_eq!(
			"(DEFINE EVENT foo ON bar WHEN $event = 'CREATE' THEN (CREATE x SET y = 1))",
			format!("{}", out)
		)
	}

	#[test]
	fn subquery_remove_statement() {
		let sql = "(REMOVE EVENT foo_event ON foo)";
		let out = Value::parse(sql);
		assert_eq!("(REMOVE EVENT foo_event ON foo)", format!("{}", out))
	}

	#[test]
	fn mock_count() {
		let sql = "|test:1000|";
		let out = Value::parse(sql);
		assert_eq!("|test:1000|", format!("{}", out));
		assert_eq!(out, Value::from(Mock::Count(String::from("test"), 1000)));
	}

	#[test]
	fn mock_range() {
		let sql = "|test:1..1000|";
		let out = Value::parse(sql);
		assert_eq!("|test:1..1000|", format!("{}", out));
		assert_eq!(out, Value::from(Mock::Range(String::from("test"), 1, 1000)));
	}

	#[test]
	fn regex_simple() {
		let sql = "/test/";
		let out = Value::parse(sql);
		assert_eq!("/test/", format!("{}", out));
		let Value::Regex(regex) = out else {
			panic!()
		};
		assert_eq!(regex, "test".parse().unwrap());
	}

	#[test]
	fn regex_complex() {
		let sql = r"/(?i)test\/[a-z]+\/\s\d\w{1}.*/";
		let out = Value::parse(sql);
		assert_eq!(r"/(?i)test/[a-z]+/\s\d\w{1}.*/", format!("{}", out));
		let Value::Regex(regex) = out else {
			panic!()
		};
		assert_eq!(regex, r"(?i)test/[a-z]+/\s\d\w{1}.*".parse().unwrap());
	}
}
