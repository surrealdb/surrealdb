use std::ops::Bound;

use common::range::TypedRange;
use surrealdb_sql::lookup::LookupKind;
use surrealdb_sql::{
	Closure, Dir, Expr, Function, FunctionCall, Idiom, Kind, Literal, Mock, Param, Part, Script,
};

use super::{ParseResult, Parser};
use crate::error::{SyntaxError, bail};
use crate::lexer::Lexer;
use crate::lexer::compound::{self};
use crate::parser::enter_object_recursion;
use crate::parser::mac::{expected, unexpected};
use crate::token::{Span, TokenKind, t};

impl Parser<'_> {
	pub(super) fn parse_number_like_prime(&mut self) -> ParseResult<Expr> {
		let token = self.peek();
		match token.kind {
			TokenKind::Infinity => Ok(Expr::Literal(Literal::Float(f64::INFINITY))),
			t!("+") | t!("-") | TokenKind::Digits => {
				self.pop_peek();
				let value = self.lex_compound(token, compound::numeric)?;
				self.last_span = value.span;
				let v = match value.value {
					compound::Numeric::Float(x) => Expr::Literal(Literal::Float(x)),
					compound::Numeric::Integer(x) => {
						Expr::Literal(Literal::Integer(x.into_int(value.span)?))
					}
					compound::Numeric::Decimal(x) => Expr::Literal(Literal::Decimal(x)),
					compound::Numeric::Duration(x) => Expr::Literal(Literal::Duration(x)),
				};
				Ok(v)
			}
			_ => unexpected!(self, token, "a number"),
		}
	}

	/// Parses a statement that appears in expression position.
	///
	/// Statements are large values, so each one is parsed in a frame of its
	/// own rather than in [`Self::parse_prime_expr`], whose frame every
	/// nested expression pays for.
	#[inline(never)]
	fn parse_boxed_stmt_prime<T>(
		&mut self,
		parse: fn(&mut Self) -> ParseResult<T>,
		wrap: fn(Box<T>) -> Expr,
	) -> ParseResult<Expr> {
		self.pop_peek();
		Ok(wrap(Box::new(parse(self)?)))
	}

	/// Parses a `<-`, `<->` or `<~` lookup at the start of an expression.
	#[inline(never)]
	fn parse_backward_lookup_prime(&mut self, token: crate::token::Token) -> ParseResult<Expr> {
		self.pop_peek();
		if let Some(peek) = self.peek_whitespace() {
			if peek.kind == t!("~") {
				self.pop_peek();
				Ok(Expr::Idiom(Idiom(vec![self.parse_graph_part(LookupKind::Reference)?])))
			} else if peek.kind == t!("-") {
				self.pop_peek();
				Ok(Expr::Idiom(Idiom(vec![self.parse_graph_part(LookupKind::Graph(Dir::In))?])))
			} else if peek.kind == t!("->") {
				self.pop_peek();
				Ok(Expr::Idiom(Idiom(vec![self.parse_graph_part(LookupKind::Graph(Dir::Both))?])))
			} else {
				unexpected!(self, token, "`<-`, `<->` or `<~`")
			}
		} else {
			unexpected!(self, token, "`<-`, `<->` or `<~`")
		}
	}

	/// Parses an `r"..."` record-id string.
	#[inline(never)]
	fn parse_record_id_string_prime(&mut self, token: crate::token::Token) -> ParseResult<Expr> {
		self.pop_peek();
		let str = self.unescape_string_span(token.span)?;
		let mut inner_parser = Parser::new(str.as_bytes());
		let record_id = match inner_parser.parse_record_id() {
			Ok(x) => x,
			Err(e) => {
				let e = e.update_spans(|span| {
					let range = span.to_range();
					let start =
						Lexer::escaped_string_offset(self.span_str(token.span), range.start);
					let end = Lexer::escaped_string_offset(self.span_str(token.span), range.end);
					*span = Span::from_range((token.span.offset + start)..(token.span.offset + end))
				});
				return Err(e);
			}
		};
		Ok(Expr::Literal(Literal::RecordId(record_id)))
	}

	/// Parse an expressions
	///
	/// Every nested expression passes through this function, so its frame is
	/// paid once per nesting level and is kept small: each arm that builds
	/// anything larger than a literal delegates to an `#[inline(never)]`
	/// helper and yields its `ParseResult` without unwrapping it here.
	pub(super) fn parse_prime_expr(&mut self) -> ParseResult<Expr> {
		let token = self.peek();
		let value = match token.kind {
			t!("NONE") => {
				self.pop_peek();
				Ok(Expr::Literal(Literal::None))
			}
			t!("NULL") => {
				self.pop_peek();
				Ok(Expr::Literal(Literal::Null))
			}
			t!("true") => {
				self.pop_peek();
				Ok(Expr::Literal(Literal::Bool(true)))
			}
			t!("false") => {
				self.pop_peek();
				Ok(Expr::Literal(Literal::Bool(false)))
			}
			TokenKind::Infinity => {
				self.pop_peek();
				Ok(Expr::Literal(Literal::Float(f64::INFINITY)))
			}
			TokenKind::NaN => {
				self.pop_peek();
				Ok(Expr::Literal(Literal::Float(f64::NAN)))
			}
			t!("CONTINUE") => {
				self.pop_peek();
				Ok(Expr::Continue)
			}
			t!("BREAK") => {
				self.pop_peek();
				Ok(Expr::Break)
			}
			t!("@") => self.parse_doc_prime(),
			t!("<") => self.parse_backward_lookup_prime(token),
			t!("r\"") | t!("r'") => self.parse_record_id_string_prime(token),
			t!("d\"")
			| t!("d'")
			| t!("u\"")
			| t!("u'")
			| t!("b\"")
			| t!("b'")
			| t!("f\"")
			| t!("f'")
			| t!("'")
			| t!("\"")
			| t!("/")
			| t!("$param") => self.parse_token_literal_prime(token),
			t!("+") | t!("-") | TokenKind::Digits => self.parse_number_like_prime(),
			t!("FUNCTION") => {
				self.pop_peek();
				self.parse_script().map(|x| Expr::FunctionCall(Box::new(x)))
			}
			t!("->") => self.parse_forward_lookup_prime(),
			t!("[") => {
				self.pop_peek();
				self.parse_array(token.span).map(|a| Expr::Literal(Literal::Array(a)))
			}
			t!("{") => {
				self.pop_peek();
				self.parse_object_like(token.span)
			}
			t!("|") => {
				self.pop_peek();
				self.parse_closure_or_mock(token.span)
			}
			t!("||") => {
				self.pop_peek();
				self.parse_closure_after_args(Vec::new())
			}
			t!("(") => {
				self.pop_peek();
				self.parse_covered_expr_or_coordinate(token.span)
			}
			t!("fn") | t!("mod") | t!("silo") | t!("ml") if self.peek1().kind == t!("::") => {
				self.parse_function_call_prime(token.kind)
			}
			t!("IF") => self.parse_boxed_stmt_prime(Self::parse_if_stmt, Expr::IfElse),
			t!("SELECT") => self.parse_boxed_stmt_prime(Self::parse_select_stmt, Expr::Select),
			t!("CREATE") => self.parse_boxed_stmt_prime(Self::parse_create_stmt, Expr::Create),
			t!("EXPLAIN") => {
				self.pop_peek();
				self.parse_explain_expr()
			}
			t!("UPDATE") => self.parse_boxed_stmt_prime(Self::parse_update_stmt, Expr::Update),
			t!("UPSERT") => self.parse_boxed_stmt_prime(Self::parse_upsert_stmt, Expr::Upsert),
			t!("DELETE") => self.parse_boxed_stmt_prime(Self::parse_delete_stmt, Expr::Delete),
			t!("RELATE") => self.parse_boxed_stmt_prime(Self::parse_relate_stmt, Expr::Relate),
			t!("INSERT") => self.parse_boxed_stmt_prime(Self::parse_insert_stmt, Expr::Insert),
			t!("DEFINE") => self.parse_boxed_stmt_prime(Self::parse_define_stmt, Expr::Define),
			t!("REMOVE") => self.parse_boxed_stmt_prime(Self::parse_remove_stmt, Expr::Remove),
			t!("REBUILD") => self.parse_boxed_stmt_prime(Self::parse_rebuild_stmt, Expr::Rebuild),
			t!("ALTER") => self.parse_boxed_stmt_prime(Self::parse_alter_stmt, Expr::Alter),
			t!("INFO") => self.parse_boxed_stmt_prime(Self::parse_info_stmt, Expr::Info),
			t!("FOR") => self.parse_boxed_stmt_prime(Self::parse_for_stmt, Expr::Foreach),
			t!("LET") => self.parse_boxed_stmt_prime(Self::parse_let_stmt, Expr::Let),
			t!("SLEEP") if self.peek1().kind != t!("(") => {
				self.parse_boxed_stmt_prime(Self::parse_sleep_stmt, Expr::Sleep)
			}
			t!("RETURN") => self.parse_boxed_stmt_prime(Self::parse_return_stmt, Expr::Return),
			t!("THROW") => self.parse_boxed_stmt_prime(Self::parse_expr_inherit, Expr::Throw),
			x if Self::kind_is_identifier(x) => self.parse_ident_prime(token),
			_ => {
				unexpected!(self, token, "an expression")
			}
		}?;

		if self.peek_continues_idiom() {
			self.parse_prime_idiom_continuation(value)
		} else {
			Ok(value)
		}
	}

	/// Parses an expression starting with `@`, the current document.
	#[inline(never)]
	fn parse_doc_prime(&mut self) -> ParseResult<Expr> {
		self.pop_peek();
		let mut res = vec![Part::Doc];
		if !self.peek_continues_idiom() {
			res.push(self.parse_dot_part()?);
		}
		Ok(Expr::Idiom(Idiom(res)))
	}

	/// Parses a literal whose value a single token carries: strings, typed
	/// strings, regexes and parameters.
	#[inline(never)]
	fn parse_token_literal_prime(&mut self, token: crate::token::Token) -> ParseResult<Expr> {
		let expr = match token.kind {
			t!("d\"") | t!("d'") => Expr::Literal(Literal::Datetime(self.next_token_value()?)),
			t!("u\"") | t!("u'") => Expr::Literal(Literal::Uuid(self.next_token_value()?)),
			t!("b\"") | t!("b'") => Expr::Literal(Literal::Bytes(self.next_token_value()?)),
			t!("f\"") | t!("f'") => {
				if !self.settings.files_enabled {
					unexpected!(self, token, "the experimental files feature to be enabled");
				}
				let file: surrealdb_types::File = self.next_token_value()?;
				Expr::Literal(Literal::File(file.into()))
			}
			t!("/") => Expr::Literal(Literal::Regex(self.next_token_value()?)),
			t!("$param") => Expr::Param(self.next_token_value()?),
			_ => {
				if self.settings.legacy_strands {
					Expr::Literal(self.reparse_legacy_strand()?)
				} else {
					let s = self.parse_string_lit()?;
					Expr::Literal(Literal::String(s.into()))
				}
			}
		};
		Ok(expr)
	}

	/// Parses a `->` lookup at the start of an expression.
	#[inline(never)]
	fn parse_forward_lookup_prime(&mut self) -> ParseResult<Expr> {
		self.pop_peek();
		Ok(Expr::Idiom(Idiom(vec![self.parse_graph_part(LookupKind::Graph(Dir::Out))?])))
	}

	/// Parses a `fn::`, `mod::`, `silo::` or `ml::` call; `kind` is the
	/// still-unconsumed leading keyword.
	#[inline(never)]
	fn parse_function_call_prime(&mut self, kind: TokenKind) -> ParseResult<Expr> {
		self.pop_peek();
		let call = match kind {
			t!("fn") => self.parse_custom_function()?,
			t!("mod") => self.parse_module_function()?,
			t!("silo") => self.parse_silo_function()?,
			_ => self.parse_model()?,
		};
		Ok(Expr::FunctionCall(Box::new(call)))
	}

	/// Parses an expression starting with an identifier: a builtin function
	/// call, a record id or range, or a table (or field) name.
	#[inline(never)]
	fn parse_ident_prime(&mut self, token: crate::token::Token) -> ParseResult<Expr> {
		match self.peek1().kind {
			t!("::") | t!("(") => {
				self.pop_peek();
				self.parse_builtin(token.span)
			}
			t!(":") => {
				let str = self.parse_ident()?;
				self.parse_record_id_or_range(str).map(|x| Expr::Literal(Literal::RecordId(x)))
			}
			_ => {
				if self.table_as_field {
					Ok(Expr::Idiom(Idiom(vec![Part::Field(self.parse_ident_str()?.into())])))
				} else {
					Ok(Expr::Table(self.parse_ident_str()?.into()))
				}
			}
		}
	}

	/// Parses the rest of an idiom that continues the prime `value`.
	#[inline(never)]
	fn parse_prime_idiom_continuation(&mut self, value: Expr) -> ParseResult<Expr> {
		match value {
			Expr::Idiom(Idiom(x)) => self.parse_remaining_value_idiom(x),
			Expr::Table(x) => self.parse_remaining_value_idiom(vec![Part::Field(x.into())]),
			x => self.parse_remaining_value_idiom(vec![Part::Start(x)]),
		}
	}

	/// Parses an array production
	///
	/// # Parser state
	/// Expects the starting `[` to already be eaten and its span passed as an
	/// argument.
	pub fn parse_array(&mut self, start: Span) -> ParseResult<Vec<Expr>> {
		let mut exprs = Vec::new();
		enter_object_recursion!(this = self => {
			loop {
				if this.eat(t!("]")) {
					break;
				}

				let value = this.parse_expr_collection_element()?;
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
	pub(super) fn parse_mock(&mut self, start_span: Span) -> ParseResult<Mock> {
		let name: surrealdb_sql::TableName = self.parse_ident_str()?.into();
		expected!(self, t!(":"));
		let start = match self.peek_kind() {
			t!("..") => {
				self.pop_peek();
				Bound::Unbounded
			}
			_ => {
				let from = self.next_token_value::<i64>()?;

				match self.peek_kind() {
					t!("..") => {
						self.pop_peek();
						Bound::Included(from)
					}
					t!(">") => {
						self.pop_peek();
						expected!(self, t!(".."));
						Bound::Excluded(from)
					}
					_ => {
						self.expect_closing_delimiter(t!("|"), start_span)?;
						return Ok(Mock::Count(name, from));
					}
				}
			}
		};

		let end = match self.peek_kind() {
			t!("|") => {
				self.pop_peek();
				Bound::Unbounded
			}
			t!("=") => {
				self.pop_peek();
				let to = self.next_token_value()?;
				self.expect_closing_delimiter(t!("|"), start_span)?;
				Bound::Included(to)
			}
			_ => {
				let to = self.next_token_value()?;
				self.expect_closing_delimiter(t!("|"), start_span)?;
				Bound::Excluded(to)
			}
		};
		Ok(Mock::Range(
			name,
			TypedRange {
				start,
				end,
			},
		))
	}

	pub(super) fn parse_closure_or_mock(&mut self, start: Span) -> ParseResult<Expr> {
		match self.peek_kind() {
			t!("$param") => self.parse_closure(start),
			_ => self.parse_mock(start).map(Expr::Mock),
		}
	}

	pub(super) fn parse_closure(&mut self, start: Span) -> ParseResult<Expr> {
		let mut args = Vec::new();
		loop {
			if self.eat(t!("|")) {
				break;
			}

			let param = self.next_token_value::<Param>()?;
			let kind = if self.eat(t!(":")) {
				if self.eat(t!("<")) {
					let delim = self.last_span();
					self.parse_kind(delim)?
				} else {
					self.parse_inner_single_kind()?
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

		self.parse_closure_after_args(args)
	}

	pub(super) fn parse_closure_after_args(
		&mut self,
		args: Vec<(Param, Kind)>,
	) -> ParseResult<Expr> {
		let (returns, body) = if self.eat(t!("->")) {
			let returns = Some(self.parse_inner_kind()?);
			let start = expected!(self, t!("{")).span;
			let body = Expr::Block(Box::new(self.parse_block(start)?));
			(returns, body)
		} else {
			let body = self.parse_expr_inherit()?;
			(None, body)
		};

		Ok(Expr::Closure(Box::new(Closure {
			args,
			returns,
			body,
		})))
	}

	fn parse_covered_expr_or_coordinate(&mut self, start: Span) -> ParseResult<Expr> {
		let peek = self.peek();
		match peek.kind {
			t!("+") | t!("-") | TokenKind::NaN | TokenKind::Infinity | TokenKind::Digits => {
				if let Some(x) = self.speculate(|this| {
					let Ok(x) = this.next_token_value::<f64>() else {
						return Ok(None);
					};

					if !this.eat(t!(",")) {
						return Ok(None);
					}

					let y = this.next_token_value::<f64>()?;
					this.expect_closing_delimiter(t!(")"), start)?;
					Ok(Some(Expr::Literal(Literal::Geometry(geo_types::Geometry::Point(
						geo_types::Point::new(x, y),
					)))))
				})? {
					return Ok(x);
				};
			}
			_ => {}
		};

		let res = self.parse_expr_inherit()?;
		let token = self.peek();
		if token.kind != t!(")")
			&& Self::starts_disallowed_subquery_statement(peek.kind)
			&& let Expr::Idiom(Idiom(ref idiom)) = res
			&& idiom.len() == 1
		{
			bail!("Unexpected token `{}` expected `)`",peek.kind,
			@token.span,
			@peek.span => "This is a reserved keyword here and can't be an identifier");
		}
		self.expect_closing_delimiter(t!(")"), start)?;

		// Ensure that `((..).a).b` is broken up into separate idioms.
		if self.peek_continues_idiom() {
			self.parse_remaining_value_idiom(vec![Part::Start(res)])
		} else {
			Ok(res)
		}
	}

	/// Parses a strand with legacy rules, parsing to a record id, datetime or
	/// uuid if the string matches.
	pub(super) fn reparse_legacy_strand(&mut self) -> Result<Literal, SyntaxError> {
		let token = self.next();
		assert!(matches!(token.kind, t!("'") | t!("\"")));

		let str = self.unescape_string_span(token.span)?;

		if let Ok(x) = Lexer::lex_uuid(str) {
			return Ok(Literal::Uuid(x.into_inner()));
		}
		if let Ok(x) = Lexer::lex_datetime(str) {
			return Ok(Literal::Datetime(x.into_inner()));
		}

		let mut record_id_parser = Parser::new(str.as_bytes());
		if let Ok(x) = record_id_parser.parse_record_id()
			&& record_id_parser.peek().is_eof()
		{
			return Ok(Literal::RecordId(x));
		}

		Ok(Literal::String(str.into()))
	}

	fn parse_script(&mut self) -> ParseResult<FunctionCall> {
		let start = expected!(self, t!("(")).span;
		let mut args = Vec::new();
		loop {
			if self.eat(t!(")")) {
				break;
			}

			let arg = self.parse_expr_inherit()?;
			args.push(arg);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!(")"), start)?;
				break;
			}
		}
		let token = expected!(self, t!("{"));
		let mut span = self.lex_compound(token, compound::javascript)?.span;
		// remove the starting `{` and ending `}`.
		span.offset += 1;
		span.len -= 2;
		let body = self.span_str(span);
		let receiver = Function::Script(Script(body.to_string()));
		Ok(FunctionCall {
			receiver,
			arguments: args,
		})
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_types::ToSql;

	use super::*;

	#[test]
	fn subquery_expression_statement() {
		let sql = "(1 + 2 + 3)";
		let out = crate::expr(sql).unwrap();
		assert_eq!("1 + 2 + 3", out.to_sql())
	}

	#[test]
	fn subquery_ifelse_statement() {
		let sql = "IF true THEN false END";
		let out = crate::expr(sql).unwrap();
		assert_eq!("IF true THEN false END", out.to_sql())
	}

	#[test]
	fn subquery_select_statement() {
		let sql = "(SELECT * FROM test)";
		let out = crate::expr(sql).unwrap();
		assert_eq!("SELECT * FROM test", out.to_sql())
	}

	#[test]
	fn subquery_define_statement() {
		let sql = "(DEFINE EVENT foo ON bar WHEN $event = 'CREATE' THEN (CREATE x SET y = 1))";
		let out = crate::expr(sql).unwrap();
		assert_eq!(
			"DEFINE EVENT foo ON bar WHEN $event = 'CREATE' THEN (CREATE x SET y = 1)",
			out.to_sql()
		)
	}

	#[test]
	fn subquery_remove_statement() {
		let sql = "(REMOVE EVENT foo_event ON foo)";
		let out = crate::expr(sql).unwrap();
		assert_eq!("REMOVE EVENT foo_event ON foo", out.to_sql())
	}

	#[test]
	fn subquery_insert_statment() {
		let sql = "(INSERT INTO test [])";
		let out = crate::expr(sql).unwrap();
		assert_eq!("INSERT INTO test []", out.to_sql())
	}

	#[test]
	fn mock_count() {
		let sql = "|test:1000|";
		let out = crate::expr(sql).unwrap();
		assert_eq!("|test:1000|", out.to_sql());
		assert_eq!(out, Expr::Mock(Mock::Count("test".into(), 1000)));
	}

	#[test]
	fn mock_range() {
		let sql = "|test:1..1000|";
		let out = crate::expr(sql).unwrap();
		assert_eq!("|test:1..1000|", out.to_sql());
		assert_eq!(out, Expr::Mock(Mock::Range("test".into(), TypedRange::from_range(1..1000))));
	}

	#[test]
	fn regex_simple() {
		let sql = "/test/";
		let out = crate::expr(sql).unwrap();
		assert_eq!("/test/", out.to_sql());
		let Expr::Literal(Literal::Regex(regex)) = out else {
			panic!()
		};
		assert_eq!(regex.as_str(), "test");
	}

	#[test]
	fn regex_complex() {
		let sql = r"/(?i)test\/[a-z]+\/\s\d\w{1}.*/";
		let out = crate::expr(sql).unwrap();
		assert_eq!(r"/(?i)test\/[a-z]+\/\s\d\w{1}.*/", out.to_sql());
		let Expr::Literal(Literal::Regex(regex)) = out else {
			panic!()
		};
		assert_eq!(regex.as_str(), r"(?i)test/[a-z]+/\s\d\w{1}.*");
	}

	#[test]
	fn plain_string() {
		let sql = r#""hello""#;
		let out = crate::expr(sql).unwrap();
		assert_eq!(r#"'hello'"#, out.to_sql());

		let sql = r#"s"hello""#;
		let out = crate::expr(sql).unwrap();
		assert_eq!(r#"'hello'"#, out.to_sql());

		let sql = r#"s'hello'"#;
		let out = crate::expr(sql).unwrap();
		assert_eq!(r#"'hello'"#, out.to_sql());
	}

	#[test]
	fn params() {
		let sql = "$hello";
		let out = crate::expr(sql).unwrap();
		assert_eq!("$hello", out.to_sql());

		let sql = "$__hello";
		let out = crate::expr(sql).unwrap();
		assert_eq!("$__hello", out.to_sql());
	}

	#[test]
	fn legacy_strand_partial_record_id_stays_string() {
		let sql = "'openai:gpt-5-mini'";
		let out = crate::expr_legacy_strand(sql).unwrap();
		assert!(
			matches!(out, Expr::Literal(Literal::String(_))),
			"expected Literal::String, got: {out:?}"
		);
		assert_eq!("'openai:gpt-5-mini'", out.to_sql());
	}

	#[test]
	fn legacy_strand_full_record_id_is_converted() {
		let sql = "'person:test'";
		let out = crate::expr_legacy_strand(sql).unwrap();
		assert!(
			matches!(out, Expr::Literal(Literal::RecordId(_))),
			"expected Literal::RecordId, got: {out:?}"
		);
	}
}
