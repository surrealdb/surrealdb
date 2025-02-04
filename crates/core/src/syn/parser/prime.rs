use geo::Point;
use reblessive::Stk;

use super::{mac::pop_glued, ParseResult, Parser};
use crate::{
	sql::{
		Array, Closure, Dir, Duration, Function, Geometry, Ident, Idiom, Kind, Mock, Number, Param,
		Part, Script, Strand, Subquery, Table, Value,
	},
	syn::{
		error::bail,
		lexer::compound,
		parser::{
			enter_object_recursion, enter_query_recursion,
			mac::{expected, unexpected},
		},
		token::{t, Glued, Span, TokenKind},
	},
};

impl Parser<'_> {
	/// Parse a what primary.
	///
	/// What's are values which are more restricted in what expressions they can contain.
	pub(super) async fn parse_what_primary(&mut self, ctx: &mut Stk) -> ParseResult<Value> {
		let token = self.peek();
		match token.kind {
			t!("r\"") => {
				self.pop_peek();
				Ok(Value::Thing(self.parse_record_string(ctx, true).await?))
			}
			t!("r'") => {
				self.pop_peek();
				Ok(Value::Thing(self.parse_record_string(ctx, false).await?))
			}
			t!("d\"") | t!("d'") | TokenKind::Glued(Glued::Datetime) => {
				Ok(Value::Datetime(self.next_token_value()?))
			}
			t!("u\"") | t!("u'") | TokenKind::Glued(Glued::Uuid) => {
				Ok(Value::Uuid(self.next_token_value()?))
			}
			t!("$param") => {
				let value = Value::Param(self.next_token_value()?);
				let value = self.try_parse_inline(ctx, &value).await?.unwrap_or(value);
				Ok(value)
			}
			t!("FUNCTION") => {
				self.pop_peek();
				let func = self.parse_script(ctx).await?;
				let value = Value::Function(Box::new(func));
				Ok(self.try_parse_inline(ctx, &value).await?.unwrap_or(value))
			}
			t!("IF") => {
				let stmt = ctx.run(|ctx| self.parse_if_stmt(ctx)).await?;
				Ok(Value::Subquery(Box::new(Subquery::Ifelse(stmt))))
			}
			t!("(") => {
				let token = self.pop_peek();
				let value = self
					.parse_inner_subquery(ctx, Some(token.span))
					.await
					.map(|x| Value::Subquery(Box::new(x)))?;
				Ok(self.try_parse_inline(ctx, &value).await?.unwrap_or(value))
			}
			t!("<") => {
				self.pop_peek();
				expected!(self, t!("FUTURE"));
				expected!(self, t!(">"));
				let start = expected!(self, t!("{")).span;
				let block = self.parse_block(ctx, start).await?;
				Ok(Value::Future(Box::new(super::sql::Future(block))))
			}
			t!("|") => {
				let start = self.pop_peek().span;
				self.parse_closure_or_mock(ctx, start).await
			}
			t!("||") => self.parse_closure_after_args(ctx, Vec::new()).await,
			t!("/") => self.next_token_value().map(Value::Regex),
			t!("RETURN")
			| t!("SELECT")
			| t!("CREATE")
			| t!("INSERT")
			| t!("UPSERT")
			| t!("UPDATE")
			| t!("DELETE")
			| t!("RELATE")
			| t!("DEFINE")
			| t!("REMOVE")
			| t!("REBUILD") => {
				self.parse_inner_subquery(ctx, None).await.map(|x| Value::Subquery(Box::new(x)))
			}
			t!("fn") => {
				self.pop_peek();
				let value =
					self.parse_custom_function(ctx).await.map(|x| Value::Function(Box::new(x)))?;
				Ok(self.try_parse_inline(ctx, &value).await?.unwrap_or(value))
			}
			t!("ml") => {
				self.pop_peek();
				let value = self.parse_model(ctx).await.map(|x| Value::Model(Box::new(x)))?;
				Ok(self.try_parse_inline(ctx, &value).await?.unwrap_or(value))
			}
			x if Self::kind_is_identifier(x) => {
				let peek = self.peek1();
				match peek.kind {
					t!("::") | t!("(") => {
						self.pop_peek();
						self.parse_builtin(ctx, token.span).await
					}
					t!(":") => {
						let str = self.next_token_value::<Ident>()?.0;
						self.parse_thing_or_range(ctx, str).await.map(Value::Thing)
					}
					_ => Ok(Value::Table(self.next_token_value()?)),
				}
			}
			_ => unexpected!(self, token, "an expression"),
		}
	}

	pub(super) async fn try_parse_inline(
		&mut self,
		ctx: &mut Stk,
		subject: &Value,
	) -> ParseResult<Option<Value>> {
		if self.eat_whitespace(t!("(")) {
			let start = self.last_span();
			let mut args = Vec::new();
			loop {
				if self.eat(t!(")")) {
					break;
				}

				let arg = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;
				args.push(arg);

				if !self.eat(t!(",")) {
					self.expect_closing_delimiter(t!(")"), start)?;
					break;
				}
			}

			let value =
				Value::Function(Box::new(Function::Anonymous(subject.clone(), args, false)));
			let value = ctx.run(|ctx| self.try_parse_inline(ctx, &value)).await?.unwrap_or(value);
			Ok(Some(value))
		} else {
			Ok(None)
		}
	}

	pub(super) fn parse_number_like_prime(&mut self) -> ParseResult<Value> {
		let token = self.peek();
		match token.kind {
			TokenKind::Glued(Glued::Duration) => {
				let duration = pop_glued!(self, Duration);
				Ok(Value::Duration(duration))
			}
			TokenKind::Glued(Glued::Number) => {
				let v = self.next_token_value()?;
				Ok(Value::Number(v))
			}
			_ => {
				self.pop_peek();
				let value = self.lexer.lex_compound(token, compound::numeric)?;
				let v = match value.value {
					compound::Numeric::Number(x) => Value::Number(x),
					compound::Numeric::Duration(x) => Value::Duration(Duration(x)),
				};
				Ok(v)
			}
		}
	}

	/// Parse an expressions
	pub(super) async fn parse_idiom_expression(&mut self, ctx: &mut Stk) -> ParseResult<Value> {
		let token = self.peek();
		let value = match token.kind {
			t!("@") => {
				self.pop_peek();
				let mut res = vec![Part::Doc];
				if !self.peek_continues_idiom() {
					res.push(self.parse_dot_part(ctx).await?);
				}

				Value::Idiom(Idiom(res))
			}
			t!("NONE") => {
				self.pop_peek();
				Value::None
			}
			t!("NULL") => {
				self.pop_peek();
				Value::Null
			}
			t!("true") => {
				self.pop_peek();
				Value::Bool(true)
			}
			t!("false") => {
				self.pop_peek();
				Value::Bool(false)
			}
			t!("<") => {
				self.pop_peek();
				let peek = self.peek_whitespace();
				if peek.kind == t!("-") {
					self.pop_peek();
					let graph = ctx.run(|ctx| self.parse_graph(ctx, Dir::In)).await?;
					Value::Idiom(Idiom(vec![Part::Graph(graph)]))
				} else if peek.kind == t!("->") {
					self.pop_peek();
					let graph = ctx.run(|ctx| self.parse_graph(ctx, Dir::Both)).await?;
					Value::Idiom(Idiom(vec![Part::Graph(graph)]))
				} else if self.eat(t!("FUTURE")) {
					// Casting should already have been parsed.
					self.expect_closing_delimiter(t!(">"), token.span)?;
					let next = expected!(self, t!("{")).span;
					let block = self.parse_block(ctx, next).await?;
					Value::Future(Box::new(super::sql::Future(block)))
				} else {
					unexpected!(self, token, "expected either a `<-` or a future")
				}
			}
			t!("r\"") => {
				self.pop_peek();
				Value::Thing(self.parse_record_string(ctx, true).await?)
			}
			t!("r'") => {
				self.pop_peek();
				Value::Thing(self.parse_record_string(ctx, false).await?)
			}
			t!("d\"") | t!("d'") | TokenKind::Glued(Glued::Datetime) => {
				Value::Datetime(self.next_token_value()?)
			}
			t!("u\"") | t!("u'") | TokenKind::Glued(Glued::Uuid) => {
				Value::Uuid(self.next_token_value()?)
			}
			t!("'") | t!("\"") | TokenKind::Glued(Glued::Strand) => {
				let s = self.next_token_value::<Strand>()?;
				if self.settings.legacy_strands {
					if let Some(x) = self.reparse_legacy_strand(ctx, &s.0).await {
						return Ok(x);
					}
				}
				Value::Strand(s)
			}
			t!("+")
			| t!("-")
			| TokenKind::Digits
			| TokenKind::Glued(Glued::Number | Glued::Duration) => self.parse_number_like_prime()?,
			TokenKind::NaN => {
				self.pop_peek();
				Value::Number(Number::Float(f64::NAN))
			}
			t!("$param") => {
				let value = Value::Param(self.next_token_value()?);
				self.try_parse_inline(ctx, &value).await?.unwrap_or(value)
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
			t!("[") => {
				self.pop_peek();
				self.parse_array(ctx, token.span).await.map(Value::Array)?
			}
			t!("{") => {
				self.pop_peek();
				let value = self.parse_object_like(ctx, token.span).await?;
				self.try_parse_inline(ctx, &value).await?.unwrap_or(value)
			}
			t!("|") => {
				self.pop_peek();
				self.parse_closure_or_mock(ctx, token.span).await?
			}
			t!("||") => {
				self.pop_peek();
				ctx.run(|ctx| self.parse_closure_after_args(ctx, Vec::new())).await?
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
				let value = self.parse_inner_subquery_or_coordinate(ctx, token.span).await?;
				self.try_parse_inline(ctx, &value).await?.unwrap_or(value)
			}
			t!("/") => self.next_token_value().map(Value::Regex)?,
			t!("RETURN")
			| t!("SELECT")
			| t!("CREATE")
			| t!("INSERT")
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
			x if Self::kind_is_identifier(x) => {
				let peek = self.peek1();
				match peek.kind {
					t!("::") | t!("(") => {
						self.pop_peek();
						self.parse_builtin(ctx, token.span).await?
					}
					t!(":") => {
						let str = self.next_token_value::<Ident>()?.0;
						self.parse_thing_or_range(ctx, str).await?.into()
					}
					_ => {
						if self.table_as_field {
							Value::Idiom(Idiom(vec![Part::Field(self.next_token_value()?)]))
						} else {
							Value::Table(self.next_token_value()?)
						}
					}
				}
			}
			_ => {
				unexpected!(self, token, "an expression")
			}
		};

		// Parse the rest of the idiom if it is being continued.
		if self.peek_continues_idiom() {
			let value = match value {
				Value::Idiom(Idiom(x)) => self.parse_remaining_value_idiom(ctx, x).await,
				Value::Table(Table(x)) => {
					self.parse_remaining_value_idiom(ctx, vec![Part::Field(Ident(x))]).await
				}
				x => self.parse_remaining_value_idiom(ctx, vec![Part::Start(x)]).await,
			}?;
			Ok(self.try_parse_inline(ctx, &value).await?.unwrap_or(value))
		} else {
			Ok(value)
		}
	}

	/// Parses an array production
	///
	/// # Parser state
	/// Expects the starting `[` to already be eaten and its span passed as an argument.
	pub(crate) async fn parse_array(&mut self, ctx: &mut Stk, start: Span) -> ParseResult<Array> {
		let mut values = Vec::new();
		enter_object_recursion!(this = self => {
			loop {
				if this.eat(t!("]")) {
					break;
				}

				let value = ctx.run(|ctx| this.parse_value_inherit(ctx)).await?;
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
	pub(super) fn parse_mock(&mut self, start: Span) -> ParseResult<Mock> {
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

	pub(super) async fn parse_closure_or_mock(
		&mut self,
		ctx: &mut Stk,
		start: Span,
	) -> ParseResult<Value> {
		match self.peek_kind() {
			t!("$param") => ctx.run(|ctx| self.parse_closure(ctx, start)).await,
			_ => self.parse_mock(start).map(Value::Mock),
		}
	}

	pub(super) async fn parse_closure(&mut self, ctx: &mut Stk, start: Span) -> ParseResult<Value> {
		let mut args = Vec::new();
		loop {
			if self.eat(t!("|")) {
				break;
			}

			let param = self.next_token_value::<Param>()?.0;
			let kind = if self.eat(t!(":")) {
				if self.eat(t!("<")) {
					let delim = self.last_span();
					ctx.run(|ctx| self.parse_kind(ctx, delim)).await?
				} else {
					ctx.run(|ctx| self.parse_inner_single_kind(ctx)).await?
				}
			} else {
				Kind::Any
			};

			args.push((param, kind));

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("|"), start)?;
				break;
			}
		}

		self.parse_closure_after_args(ctx, args).await
	}

	pub(super) async fn parse_closure_after_args(
		&mut self,
		ctx: &mut Stk,
		args: Vec<(Ident, Kind)>,
	) -> ParseResult<Value> {
		let (returns, body) = if self.eat(t!("->")) {
			let returns = Some(ctx.run(|ctx| self.parse_inner_kind(ctx)).await?);
			let start = expected!(self, t!("{")).span;
			let body = Value::Block(Box::new(ctx.run(|ctx| self.parse_block(ctx, start)).await?));
			(returns, body)
		} else {
			let body = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;
			(None, body)
		};

		Ok(Value::Closure(Box::new(Closure {
			args,
			returns,
			body,
		})))
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

	pub(super) async fn parse_inner_subquery_or_coordinate(
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
			t!("INSERT") => {
				self.pop_peek();
				let stmt = ctx.run(|ctx| self.parse_insert_stmt(ctx)).await?;
				Subquery::Insert(stmt)
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
				let stmt = self.parse_remove_stmt(ctx).await?;
				Subquery::Remove(stmt)
			}
			t!("REBUILD") => {
				self.pop_peek();
				let stmt = self.parse_rebuild_stmt()?;
				Subquery::Rebuild(stmt)
			}
			TokenKind::Digits | TokenKind::Glued(Glued::Number) | t!("+") | t!("-") => {
				if self.glue_and_peek1()?.kind == t!(",") {
					let number_span = self.peek().span;
					let number = self.next_token_value::<Number>()?;
					// eat ','
					self.next();

					if matches!(number, Number::Decimal(_))
						|| matches!(number, Number::Float(x) if x.is_nan())
					{
						bail!("Unexpected token, expected a non-decimal, non-NaN, number",
								@number_span => "Coordinate numbers can't be NaN or a decimal");
					}

					let x = number.as_float();
					let y = self.next_token_value::<f64>()?;
					self.expect_closing_delimiter(t!(")"), start)?;
					return Ok(Value::Geometry(Geometry::Point(Point::from((x, y)))));
				} else {
					let value = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;
					Subquery::Value(value)
				}
			}
			_ => {
				let value = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;
				Subquery::Value(value)
			}
		};
		let token = self.peek();
		if token.kind != t!(")") && Self::starts_disallowed_subquery_statement(peek.kind) {
			if let Subquery::Value(Value::Idiom(Idiom(ref idiom))) = res {
				if idiom.len() == 1 {
					bail!("Unexpected token `{}` expected `)`",peek.kind,
						@token.span,
						@peek.span => "This is a reserved keyword here and can't be an identifier");
				}
			}
		}
		self.expect_closing_delimiter(t!(")"), start)?;
		Ok(Value::Subquery(Box::new(res)))
	}

	pub(super) async fn parse_inner_subquery(
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
			t!("INSERT") => {
				self.pop_peek();
				let stmt = ctx.run(|ctx| self.parse_insert_stmt(ctx)).await?;
				Subquery::Insert(stmt)
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
				let stmt = self.parse_remove_stmt(ctx).await?;
				Subquery::Remove(stmt)
			}
			t!("REBUILD") => {
				self.pop_peek();
				let stmt = self.parse_rebuild_stmt()?;
				Subquery::Rebuild(stmt)
			}
			_ => {
				let value = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;
				Subquery::Value(value)
			}
		};
		if let Some(start) = start {
			let token = self.peek();
			if token.kind != t!(")") && Self::starts_disallowed_subquery_statement(peek.kind) {
				if let Subquery::Value(Value::Idiom(Idiom(ref idiom))) = res {
					if idiom.len() == 1 {
						// we parsed a single idiom and the next token was a dissallowed statement so
						// it is likely that the used meant to use an invalid statement.
						bail!("Unexpected token `{}` expected `)`",peek.kind,
							@token.span,
							@peek.span => "This is a reserved keyword here and can't be an identifier");
					}
				}
			}

			self.expect_closing_delimiter(t!(")"), start)?;
		}
		Ok(res)
	}

	/// Parses a strand with legacy rules, parsing to a record id, datetime or uuid if the string
	/// matches.
	pub(super) async fn reparse_legacy_strand(
		&mut self,
		ctx: &mut Stk,
		text: &str,
	) -> Option<Value> {
		if let Ok(x) = Parser::new(text.as_bytes()).parse_thing(ctx).await {
			return Some(Value::Thing(x));
		}
		if let Ok(x) = Parser::new(text.as_bytes()).next_token_value() {
			return Some(Value::Datetime(x));
		}
		if let Ok(x) = Parser::new(text.as_bytes()).next_token_value() {
			return Some(Value::Uuid(x));
		}
		None
	}

	async fn parse_script(&mut self, ctx: &mut Stk) -> ParseResult<Function> {
		let start = expected!(self, t!("(")).span;
		let mut args = Vec::new();
		loop {
			if self.eat(t!(")")) {
				break;
			}

			let arg = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;
			args.push(arg);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!(")"), start)?;
				break;
			}
		}
		let token = expected!(self, t!("{"));
		let mut span = self.lexer.lex_compound(token, compound::javascript)?.span;
		// remove the starting `{` and ending `}`.
		span.offset += 1;
		span.len -= 2;
		let body = self.lexer.span_str(span);
		Ok(Function::Script(Script(body.to_string()), args))
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
	fn subquery_insert_statment() {
		let sql = "(INSERT INTO test [])";
		let out = Value::parse(sql);
		assert_eq!("(INSERT INTO test [])", format!("{}", out))
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
		assert_eq!(r"/(?i)test\/[a-z]+\/\s\d\w{1}.*/", format!("{}", out));
		let Value::Regex(regex) = out else {
			panic!()
		};
		assert_eq!(regex, r"(?i)test/[a-z]+/\s\d\w{1}.*".parse().unwrap());
	}

	#[test]
	fn plain_string() {
		let sql = r#""hello""#;
		let out = Value::parse(sql);
		assert_eq!(r#"'hello'"#, format!("{}", out));

		let sql = r#"s"hello""#;
		let out = Value::parse(sql);
		assert_eq!(r#"'hello'"#, format!("{}", out));

		let sql = r#"s'hello'"#;
		let out = Value::parse(sql);
		assert_eq!(r#"'hello'"#, format!("{}", out));
	}

	#[test]
	fn params() {
		let sql = "$hello";
		let out = Value::parse(sql);
		assert_eq!("$hello", format!("{}", out));

		let sql = "$__hello";
		let out = Value::parse(sql);
		assert_eq!("$__hello", format!("{}", out));
	}
}
