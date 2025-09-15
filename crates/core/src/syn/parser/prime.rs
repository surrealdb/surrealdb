use reblessive::Stk;

use super::basic::NumberToken;
use super::mac::pop_glued;
use super::{ParseResult, Parser};
use crate::sql::lookup::LookupKind;
use crate::sql::{
	Closure, Dir, Expr, Function, FunctionCall, Ident, Idiom, Kind, Literal, Mock, Param, Part,
	Script,
};
use crate::syn::error::bail;
use crate::syn::lexer::compound::{self, Numeric};
use crate::syn::parser::enter_object_recursion;
use crate::syn::parser::mac::{expected, unexpected};
use crate::syn::token::{Glued, Span, TokenKind, t};
use crate::val::{Duration, Strand};

impl Parser<'_> {
	pub(super) fn parse_number_like_prime(&mut self) -> ParseResult<Expr> {
		let token = self.peek();
		match token.kind {
			TokenKind::Glued(Glued::Duration) => {
				let duration = pop_glued!(self, Duration);
				Ok(Expr::Literal(Literal::Duration(duration)))
			}
			TokenKind::Glued(Glued::Number) => {
				let v = self.next_token_value()?;
				match v {
					NumberToken::Float(f) => Ok(Expr::Literal(Literal::Float(f))),
					NumberToken::Integer(i) => Ok(Expr::Literal(Literal::Integer(i))),
					NumberToken::Decimal(d) => Ok(Expr::Literal(Literal::Decimal(d))),
				}
			}
			_ => {
				self.pop_peek();
				let value = self.lexer.lex_compound(token, compound::numeric)?;
				let v = match value.value {
					compound::Numeric::Float(x) => Expr::Literal(Literal::Float(x)),
					compound::Numeric::Integer(x) => Expr::Literal(Literal::Integer(x)),
					compound::Numeric::Decimal(x) => Expr::Literal(Literal::Decimal(x)),
					compound::Numeric::Duration(x) => Expr::Literal(Literal::Duration(Duration(x))),
				};
				Ok(v)
			}
		}
	}

	/// Parse an expressions
	pub(super) async fn parse_prime_expr(&mut self, stk: &mut Stk) -> ParseResult<Expr> {
		let token = self.peek();
		let value = match token.kind {
			t!("@") => {
				self.pop_peek();
				let mut res = vec![Part::Doc];
				if !self.peek_continues_idiom() {
					res.push(self.parse_dot_part(stk).await?);
				}

				Expr::Idiom(Idiom(res))
			}
			t!("NONE") => {
				self.pop_peek();
				Expr::Literal(Literal::None)
			}
			t!("NULL") => {
				self.pop_peek();
				Expr::Literal(Literal::Null)
			}
			t!("true") => {
				self.pop_peek();
				Expr::Literal(Literal::Bool(true))
			}
			t!("false") => {
				self.pop_peek();
				Expr::Literal(Literal::Bool(false))
			}
			t!("<") => {
				self.pop_peek();
				let peek = self.peek_whitespace();
				if peek.kind == t!("~") {
					self.pop_peek();
					if !self.settings.references_enabled {
						bail!(
							"Experimental capability `record_references` is not enabled",
							@self.last_span() => "Use of `<~` reference lookup is still experimental"
						)
					}

					let lookup =
						stk.run(|ctx| self.parse_lookup(ctx, LookupKind::Reference)).await?;
					Expr::Idiom(Idiom(vec![Part::Graph(lookup)]))
				} else if peek.kind == t!("-") {
					self.pop_peek();
					let lookup =
						stk.run(|ctx| self.parse_lookup(ctx, LookupKind::Graph(Dir::In))).await?;
					Expr::Idiom(Idiom(vec![Part::Graph(lookup)]))
				} else if peek.kind == t!("->") {
					self.pop_peek();
					let lookup =
						stk.run(|ctx| self.parse_lookup(ctx, LookupKind::Graph(Dir::Both))).await?;
					Expr::Idiom(Idiom(vec![Part::Graph(lookup)]))
				} else {
					unexpected!(self, token, "expected either a `<-` or a future")
				}
			}
			t!("r\"") => {
				self.pop_peek();
				let record_id = self.parse_record_string(stk, true).await?;
				Expr::Literal(Literal::RecordId(record_id))
			}
			t!("r'") => {
				self.pop_peek();
				let record_id = self.parse_record_string(stk, false).await?;
				Expr::Literal(Literal::RecordId(record_id))
			}
			t!("d\"") | t!("d'") | TokenKind::Glued(Glued::Datetime) => {
				let datetime = self.next_token_value()?;
				Expr::Literal(Literal::Datetime(datetime))
			}
			t!("u\"") | t!("u'") | TokenKind::Glued(Glued::Uuid) => {
				let datetime = self.next_token_value()?;
				Expr::Literal(Literal::Uuid(datetime))
			}
			t!("b\"") | t!("b'") | TokenKind::Glued(Glued::Bytes) => {
				let bytes = self.next_token_value()?;
				Expr::Literal(Literal::Bytes(bytes))
			}
			t!("f\"") | t!("f'") | TokenKind::Glued(Glued::File) => {
				if !self.settings.files_enabled {
					unexpected!(self, token, "the experimental files feature to be enabled");
				}

				let file = self.next_token_value()?;
				Expr::Literal(Literal::File(file))
			}
			t!("'") | t!("\"") | TokenKind::Glued(Glued::Strand) => {
				let s = self.next_token_value::<Strand>()?;
				if self.settings.legacy_strands {
					Expr::Literal(self.reparse_legacy_strand(stk, s).await)
				} else {
					Expr::Literal(Literal::Strand(s))
				}
			}
			t!("+")
			| t!("-")
			| TokenKind::Digits
			| TokenKind::Glued(Glued::Number | Glued::Duration) => self.parse_number_like_prime()?,
			TokenKind::NaN => {
				self.pop_peek();
				Expr::Literal(Literal::Float(f64::NAN))
			}
			t!("$param") => Expr::Param(self.next_token_value()?),
			t!("FUNCTION") => {
				self.pop_peek();
				let script = self.parse_script(stk).await?;
				Expr::FunctionCall(Box::new(script))
			}
			t!("->") => {
				self.pop_peek();
				let lookup =
					stk.run(|ctx| self.parse_lookup(ctx, LookupKind::Graph(Dir::Out))).await?;
				Expr::Idiom(Idiom(vec![Part::Graph(lookup)]))
			}
			t!("[") => {
				self.pop_peek();
				self.parse_array(stk, token.span).await.map(|a| Expr::Literal(Literal::Array(a)))?
			}
			t!("{") => {
				self.pop_peek();
				self.parse_object_like(stk, token.span).await?
			}
			t!("|") => {
				self.pop_peek();
				self.parse_closure_or_mock(stk, token.span).await?
			}
			t!("||") => {
				self.pop_peek();
				stk.run(|ctx| self.parse_closure_after_args(ctx, Vec::new())).await?
			}
			t!("(") => {
				self.pop_peek();
				self.parse_covered_expr_or_coordinate(stk, token.span).await?
			}
			t!("/") => {
				let regex = self.next_token_value()?;
				Expr::Literal(Literal::Regex(regex))
			}
			t!("fn") => {
				self.pop_peek();
				self.parse_custom_function(stk).await.map(|x| Expr::FunctionCall(Box::new(x)))?
			}
			t!("ml") => {
				self.pop_peek();
				self.parse_model(stk).await.map(|x| Expr::FunctionCall(Box::new(x)))?
			}
			t!("IF") => {
				self.pop_peek();
				let stmt = stk.run(|ctx| self.parse_if_stmt(ctx)).await?;
				Expr::If(Box::new(stmt))
			}
			t!("SELECT") => {
				self.pop_peek();
				let stmt = self.parse_select_stmt(stk).await?;
				Expr::Select(Box::new(stmt))
			}
			t!("CREATE") => {
				self.pop_peek();
				let stmt = self.parse_create_stmt(stk).await?;
				Expr::Create(Box::new(stmt))
			}
			t!("UPDATE") => {
				self.pop_peek();
				let stmt = self.parse_update_stmt(stk).await?;
				Expr::Update(Box::new(stmt))
			}
			t!("UPSERT") => {
				self.pop_peek();
				let stmt = self.parse_upsert_stmt(stk).await?;
				Expr::Upsert(Box::new(stmt))
			}
			t!("DELETE") => {
				self.pop_peek();
				let stmt = self.parse_delete_stmt(stk).await?;
				Expr::Delete(Box::new(stmt))
			}
			t!("RELATE") => {
				self.pop_peek();
				let stmt = self.parse_relate_stmt(stk).await?;
				Expr::Relate(Box::new(stmt))
			}
			t!("INSERT") => {
				self.pop_peek();
				let stmt = self.parse_insert_stmt(stk).await?;
				Expr::Insert(Box::new(stmt))
			}
			t!("DEFINE") => {
				self.pop_peek();
				let stmt = self.parse_define_stmt(stk).await?;
				Expr::Define(Box::new(stmt))
			}
			t!("REMOVE") => {
				self.pop_peek();
				let stmt = self.parse_remove_stmt(stk).await?;
				Expr::Remove(Box::new(stmt))
			}
			t!("REBUILD") => {
				self.pop_peek();
				let stmt = self.parse_rebuild_stmt()?;
				Expr::Rebuild(Box::new(stmt))
			}
			t!("ALTER") => {
				self.pop_peek();
				let stmt = self.parse_alter_stmt(stk).await?;
				Expr::Alter(Box::new(stmt))
			}
			t!("INFO") => {
				self.pop_peek();
				let stmt = self.parse_info_stmt(stk).await?;
				Expr::Info(Box::new(stmt))
			}
			t!("FOR") => {
				self.pop_peek();
				let stmt = self.parse_for_stmt(stk).await?;
				Expr::Foreach(Box::new(stmt))
			}
			t!("LET") => {
				self.pop_peek();
				let stmt = self.parse_let_stmt(stk).await?;
				Expr::Let(Box::new(stmt))
			}
			t!("SLEEP") if self.peek1().kind != t!("(") => {
				self.pop_peek();
				let stmt = self.parse_sleep_stmt()?;
				Expr::Sleep(Box::new(stmt))
			}
			t!("RETURN") => {
				self.pop_peek();
				let stmt = self.parse_return_stmt(stk).await?;
				Expr::Return(Box::new(stmt))
			}
			t!("THROW") => {
				self.pop_peek();
				let expr = stk.run(|stk| self.parse_expr_inherit(stk)).await?;
				Expr::Throw(Box::new(expr))
			}
			t!("CONTINUE") => {
				self.pop_peek();
				Expr::Continue
			}
			t!("BREAK") => {
				self.pop_peek();
				Expr::Break
			}
			x if Self::kind_is_identifier(x) => {
				let peek = self.peek1();
				match peek.kind {
					t!("::") | t!("(") => {
						self.pop_peek();
						self.parse_builtin(stk, token.span).await?
					}
					t!(":") => {
						let str = self.next_token_value::<Ident>()?;
						self.parse_record_id_or_range(stk, str)
							.await
							.map(|x| Expr::Literal(Literal::RecordId(x)))?
					}
					_ => {
						if self.table_as_field {
							Expr::Idiom(Idiom(vec![Part::Field(self.next_token_value()?)]))
						} else {
							Expr::Table(self.next_token_value()?)
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
			match value {
				Expr::Idiom(Idiom(x)) => self.parse_remaining_value_idiom(stk, x).await,
				Expr::Table(x) => self.parse_remaining_value_idiom(stk, vec![Part::Field(x)]).await,
				x => self.parse_remaining_value_idiom(stk, vec![Part::Start(x)]).await,
			}
		} else {
			Ok(value)
		}
	}

	/// Parses an array production
	///
	/// # Parser state
	/// Expects the starting `[` to already be eaten and its span passed as an
	/// argument.
	pub(crate) async fn parse_array(
		&mut self,
		stk: &mut Stk,
		start: Span,
	) -> ParseResult<Vec<Expr>> {
		let mut exprs = Vec::new();
		enter_object_recursion!(this = self => {
			loop {
				if this.eat(t!("]")) {
					break;
				}

				let value = stk.run(|ctx| this.parse_expr_inherit(ctx)).await?;
				exprs.push(value);

				if !this.eat(t!(",")) {
					this.expect_closing_delimiter(t!("]"), start)?;
					break;
				}
			}
		});

		Ok(exprs)
	}

	/// Parse a mock `|foo:1..3|`
	///
	/// # Parser State
	/// Expects the starting `|` already be eaten and its span passed as an
	/// argument.
	pub(super) fn parse_mock(&mut self, start: Span) -> ParseResult<Mock> {
		let name = self.next_token_value::<Ident>()?.into_string();
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
		stk: &mut Stk,
		start: Span,
	) -> ParseResult<Expr> {
		match self.peek_kind() {
			t!("$param") => stk.run(|ctx| self.parse_closure(ctx, start)).await,
			_ => self.parse_mock(start).map(Expr::Mock),
		}
	}

	pub(super) async fn parse_closure(&mut self, stk: &mut Stk, start: Span) -> ParseResult<Expr> {
		let mut args = Vec::new();
		loop {
			if self.eat(t!("|")) {
				break;
			}

			let param = self.next_token_value::<Param>()?.ident();
			let kind = if self.eat(t!(":")) {
				if self.eat(t!("<")) {
					let delim = self.last_span();
					stk.run(|stk| self.parse_kind(stk, delim)).await?
				} else {
					stk.run(|stk| self.parse_inner_single_kind(stk)).await?
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

		self.parse_closure_after_args(stk, args).await
	}

	pub(super) async fn parse_closure_after_args(
		&mut self,
		stk: &mut Stk,
		args: Vec<(Ident, Kind)>,
	) -> ParseResult<Expr> {
		let (returns, body) = if self.eat(t!("->")) {
			let returns = Some(stk.run(|ctx| self.parse_inner_kind(ctx)).await?);
			let start = expected!(self, t!("{")).span;
			let body = Expr::Block(Box::new(stk.run(|ctx| self.parse_block(ctx, start)).await?));
			(returns, body)
		} else {
			let body = stk.run(|stk| self.parse_expr_inherit(stk)).await?;
			(None, body)
		};

		Ok(Expr::Closure(Box::new(Closure {
			args,
			returns,
			body,
		})))
	}

	async fn parse_covered_expr_or_coordinate(
		&mut self,
		stk: &mut Stk,
		start: Span,
	) -> ParseResult<Expr> {
		let peek = self.peek();
		let res = match peek.kind {
			TokenKind::Digits | TokenKind::Glued(Glued::Number) | t!("+") | t!("-") => {
				if self.glue_and_peek1()?.kind == t!(",") {
					let number_span = self.peek().span;
					let number = self.next_token_value::<Numeric>()?;
					// eat ','
					self.next();

					let x = match number {
						Numeric::Duration(_) | Numeric::Decimal(_) => {
							bail!("Unexpected token, expected a non-decimal, non-NaN, number",
								@number_span => "Coordinate numbers can't be NaN or a decimal");
						}
						Numeric::Float(x) if x.is_nan() => {
							bail!("Unexpected token, expected a non-decimal, non-NaN, number",
								@number_span => "Coordinate numbers can't be NaN or a decimal");
						}
						Numeric::Float(x) => x,
						Numeric::Integer(x) => x as f64,
					};

					let y = self.next_token_value::<f64>()?;
					self.expect_closing_delimiter(t!(")"), start)?;
					return Ok(Expr::Literal(Literal::Geometry(crate::val::Geometry::Point(
						geo::Point::new(x, y),
					))));
				} else {
					stk.run(|ctx| self.parse_expr_inherit(ctx)).await?
				}
			}
			_ => stk.run(|ctx| self.parse_expr_inherit(ctx)).await?,
		};
		let token = self.peek();
		if token.kind != t!(")") && Self::starts_disallowed_subquery_statement(peek.kind) {
			if let Expr::Idiom(Idiom(ref idiom)) = res {
				if idiom.len() == 1 {
					bail!("Unexpected token `{}` expected `)`",peek.kind,
						@token.span,
						@peek.span => "This is a reserved keyword here and can't be an identifier");
				}
			}
		}
		self.expect_closing_delimiter(t!(")"), start)?;
		Ok(res)
	}

	/// Parses a strand with legacy rules, parsing to a record id, datetime or
	/// uuid if the string matches.
	pub(super) async fn reparse_legacy_strand(&mut self, stk: &mut Stk, text: Strand) -> Literal {
		if let Ok(x) = Parser::new(text.as_bytes()).parse_record_id(stk).await {
			return Literal::RecordId(x);
		}
		if let Ok(x) = Parser::new(text.as_bytes()).next_token_value() {
			return Literal::Datetime(x);
		}
		if let Ok(x) = Parser::new(text.as_bytes()).next_token_value() {
			return Literal::Uuid(x);
		}
		Literal::Strand(text)
	}

	async fn parse_script(&mut self, stk: &mut Stk) -> ParseResult<FunctionCall> {
		let start = expected!(self, t!("(")).span;
		let mut args = Vec::new();
		loop {
			if self.eat(t!(")")) {
				break;
			}

			let arg = stk.run(|ctx| self.parse_expr_inherit(ctx)).await?;
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
		let receiver = Function::Script(Script(body.to_string()));
		Ok(FunctionCall {
			receiver,
			arguments: args,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::syn;

	#[test]
	fn subquery_expression_statement() {
		let sql = "(1 + 2 + 3)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("1 + 2 + 3", format!("{}", out))
	}

	#[test]
	fn subquery_ifelse_statement() {
		let sql = "IF true THEN false END";
		let out = syn::expr(sql).unwrap();
		assert_eq!("IF true THEN false END", format!("{}", out))
	}

	#[test]
	fn subquery_select_statement() {
		let sql = "(SELECT * FROM test)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("SELECT * FROM test", format!("{}", out))
	}

	#[test]
	fn subquery_define_statement() {
		let sql = "(DEFINE EVENT foo ON bar WHEN $event = 'CREATE' THEN (CREATE x SET y = 1))";
		let out = syn::expr(sql).unwrap();
		assert_eq!(
			"DEFINE EVENT foo ON bar WHEN $event = 'CREATE' THEN CREATE x SET y = 1",
			format!("{}", out)
		)
	}

	#[test]
	fn subquery_remove_statement() {
		let sql = "(REMOVE EVENT foo_event ON foo)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("REMOVE EVENT foo_event ON foo", format!("{}", out))
	}

	#[test]
	fn subquery_insert_statment() {
		let sql = "(INSERT INTO test [])";
		let out = syn::expr(sql).unwrap();
		assert_eq!("INSERT INTO test []", format!("{}", out))
	}

	#[test]
	fn mock_count() {
		let sql = "|test:1000|";
		let out = syn::expr(sql).unwrap();
		assert_eq!("|test:1000|", format!("{}", out));
		assert_eq!(out, Expr::Mock(Mock::Count(String::from("test"), 1000)));
	}

	#[test]
	fn mock_range() {
		let sql = "|test:1..1000|";
		let out = syn::expr(sql).unwrap();
		assert_eq!("|test:1..1000|", format!("{}", out));
		assert_eq!(out, Expr::Mock(Mock::Range(String::from("test"), 1, 1000)));
	}

	#[test]
	fn regex_simple() {
		let sql = "/test/";
		let out = syn::expr(sql).unwrap();
		assert_eq!("/test/", format!("{}", out));
		let Expr::Literal(Literal::Regex(regex)) = out else {
			panic!()
		};
		assert_eq!(regex, "test".parse().unwrap());
	}

	#[test]
	fn regex_complex() {
		let sql = r"/(?i)test\/[a-z]+\/\s\d\w{1}.*/";
		let out = syn::expr(sql).unwrap();
		assert_eq!(r"/(?i)test\/[a-z]+\/\s\d\w{1}.*/", format!("{}", out));
		let Expr::Literal(Literal::Regex(regex)) = out else {
			panic!()
		};
		assert_eq!(regex, r"(?i)test/[a-z]+/\s\d\w{1}.*".parse().unwrap());
	}

	#[test]
	fn plain_string() {
		let sql = r#""hello""#;
		let out = syn::expr(sql).unwrap();
		assert_eq!(r#"'hello'"#, format!("{}", out));

		let sql = r#"s"hello""#;
		let out = syn::expr(sql).unwrap();
		assert_eq!(r#"'hello'"#, format!("{}", out));

		let sql = r#"s'hello'"#;
		let out = syn::expr(sql).unwrap();
		assert_eq!(r#"'hello'"#, format!("{}", out));
	}

	#[test]
	fn params() {
		let sql = "$hello";
		let out = syn::expr(sql).unwrap();
		assert_eq!("$hello", format!("{}", out));

		let sql = "$__hello";
		let out = syn::expr(sql).unwrap();
		assert_eq!("$__hello", format!("{}", out));
	}
}
