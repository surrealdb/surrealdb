use geo::Point;

use super::{ParseResult, Parser};
use crate::{
	sql::{
		Array, Dir, Function, Geometry, Ident, Idiom, Mock, Part, Script, Strand, Subquery, Table,
		Value,
	},
	syn::v2::{
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
	pub fn parse_what_primary(&mut self) -> ParseResult<Value> {
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
				Ok(Value::Thing(self.parse_record_string(true)?))
			}
			t!("r'") => {
				self.pop_peek();
				Ok(Value::Thing(self.parse_record_string(false)?))
			}
			t!("$param") => {
				let param = self.next_token_value()?;
				Ok(Value::Param(param))
			}
			t!("FUNCTION") => {
				self.pop_peek();
				Ok(Value::Function(Box::new(self.parse_script()?)))
			}
			t!("IF") => {
				let stmt = self.parse_if_stmt()?;
				Ok(Value::Subquery(Box::new(Subquery::Ifelse(stmt))))
			}
			t!("(") => {
				let token = self.pop_peek();
				self.parse_inner_subquery(Some(token.span)).map(|x| Value::Subquery(Box::new(x)))
			}
			t!("<") => {
				self.pop_peek();
				expected!(self, t!("FUTURE"));
				expected!(self, t!(">"));
				let start = expected!(self, t!("{")).span;
				let block = self.parse_block(start)?;
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
			| t!("UPDATE")
			| t!("DELETE")
			| t!("RELATE")
			| t!("DEFINE")
			| t!("REMOVE") => self.parse_inner_subquery(None).map(|x| Value::Subquery(Box::new(x))),
			t!("fn") => self.parse_custom_function().map(|x| Value::Function(Box::new(x))),
			t!("ml") => self.parse_model().map(|x| Value::Model(Box::new(x))),
			x => {
				if !self.peek_can_be_ident() {
					unexpected!(self, x, "a value")
				}

				let token = self.next();
				match self.peek_kind() {
					t!("::") | t!("(") => self.parse_builtin(token.span),
					t!(":") => {
						let str = self.token_value::<Ident>(token)?.0;
						self.parse_thing_or_range(str)
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
	pub fn parse_idiom_expression(&mut self) -> ParseResult<Value> {
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
				let block = self.parse_block(next)?;
				return Ok(Value::Future(Box::new(crate::sql::Future(block))));
			}
			TokenKind::Strand => {
				self.pop_peek();
				if self.legacy_strands {
					return self.parse_legacy_strand();
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
				Value::Thing(self.parse_record_string(true)?)
			}
			t!("r'") => {
				self.pop_peek();
				Value::Thing(self.parse_record_string(false)?)
			}
			t!("$param") => {
				self.pop_peek();
				let param = self.token_value(token)?;
				Value::Param(param)
			}
			t!("FUNCTION") => {
				self.pop_peek();
				Value::Function(Box::new(self.parse_script()?))
			}
			t!("->") => {
				self.pop_peek();
				let graph = self.parse_graph(Dir::Out)?;
				Value::Idiom(Idiom(vec![Part::Graph(graph)]))
			}
			t!("<->") => {
				self.pop_peek();
				let graph = self.parse_graph(Dir::Both)?;
				Value::Idiom(Idiom(vec![Part::Graph(graph)]))
			}
			t!("<-") => {
				self.pop_peek();
				let graph = self.parse_graph(Dir::In)?;
				Value::Idiom(Idiom(vec![Part::Graph(graph)]))
			}
			t!("[") => {
				self.pop_peek();
				self.parse_array(token.span).map(Value::Array)?
			}
			t!("{") => {
				self.pop_peek();
				self.parse_object_like(token.span)?
			}
			t!("|") => {
				self.pop_peek();
				self.parse_mock(token.span).map(Value::Mock)?
			}
			t!("IF") => {
				self.pop_peek();
				let stmt = self.parse_if_stmt()?;
				Value::Subquery(Box::new(Subquery::Ifelse(stmt)))
			}
			t!("(") => {
				self.pop_peek();
				self.parse_inner_subquery_or_coordinate(token.span)?
			}
			t!("/") => {
				self.pop_peek();
				let regex = self.lexer.relex_regex(token);
				self.token_value(regex).map(Value::Regex)?
			}
			t!("RETURN")
			| t!("SELECT")
			| t!("CREATE")
			| t!("UPDATE")
			| t!("DELETE")
			| t!("RELATE")
			| t!("DEFINE")
			| t!("REMOVE") => self.parse_inner_subquery(None).map(|x| Value::Subquery(Box::new(x)))?,
			t!("fn") => {
				self.pop_peek();
				self.parse_custom_function().map(|x| Value::Function(Box::new(x)))?
			}
			t!("ml") => {
				self.pop_peek();
				self.parse_model().map(|x| Value::Model(Box::new(x)))?
			}
			_ => {
				self.pop_peek();
				match self.peek_kind() {
					t!("::") | t!("(") => self.parse_builtin(token.span)?,
					t!(":") => {
						let str = self.token_value::<Ident>(token)?.0;
						self.parse_thing_or_range(str)?
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
				Value::Idiom(Idiom(x)) => self.parse_remaining_value_idiom(x),
				Value::Table(Table(x)) => {
					self.parse_remaining_value_idiom(vec![Part::Field(Ident(x))])
				}
				x => self.parse_remaining_value_idiom(vec![Part::Start(x)]),
			}
		} else {
			Ok(value)
		}
	}

	/// Parses an array production
	///
	/// # Parser state
	/// Expects the starting `[` to already be eaten and its span passed as an argument.
	pub fn parse_array(&mut self, start: Span) -> ParseResult<Array> {
		let mut values = Vec::new();
		loop {
			if self.eat(t!("]")) {
				break;
			}
			values.push(self.parse_value_field()?);

			if !self.eat(t!(",")) {
				self.expect_closing_delimiter(t!("]"), start)?;
				break;
			}
		}

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

	pub fn parse_full_subquery(&mut self) -> ParseResult<Subquery> {
		let peek = self.peek();
		match peek.kind {
			t!("(") => {
				self.pop_peek();
				dbg!("called");
				self.parse_inner_subquery(Some(peek.span))
			}
			t!("IF") => {
				self.pop_peek();
				let if_stmt = self.parse_if_stmt()?;
				Ok(Subquery::Ifelse(if_stmt))
			}
			_ => self.parse_inner_subquery(None),
		}
	}

	pub fn parse_inner_subquery_or_coordinate(&mut self, start: Span) -> ParseResult<Value> {
		let peek = self.peek();
		let res = match peek.kind {
			t!("RETURN") => {
				self.pop_peek();
				let stmt = self.parse_return_stmt()?;
				Subquery::Output(stmt)
			}
			t!("SELECT") => {
				self.pop_peek();
				let stmt = self.parse_select_stmt()?;
				Subquery::Select(stmt)
			}
			t!("CREATE") => {
				self.pop_peek();
				let stmt = self.parse_create_stmt()?;
				Subquery::Create(stmt)
			}
			t!("UPDATE") => {
				self.pop_peek();
				let stmt = self.parse_update_stmt()?;
				Subquery::Update(stmt)
			}
			t!("DELETE") => {
				self.pop_peek();
				let stmt = self.parse_delete_stmt()?;
				Subquery::Delete(stmt)
			}
			t!("RELATE") => {
				self.pop_peek();
				let stmt = self.parse_relate_stmt()?;
				Subquery::Relate(stmt)
			}
			t!("DEFINE") => {
				self.pop_peek();
				let stmt = self.parse_define_stmt()?;
				Subquery::Define(stmt)
			}
			t!("REMOVE") => {
				self.pop_peek();
				let stmt = self.parse_remove_stmt()?;
				Subquery::Remove(stmt)
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
				Subquery::Value(self.parse_value_field()?)
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
				Subquery::Value(self.parse_value_field()?)
			}
			_ => {
				let value = self.parse_value_field()?;
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

	pub fn parse_inner_subquery(&mut self, start: Option<Span>) -> ParseResult<Subquery> {
		let peek = self.peek();
		let res = match peek.kind {
			t!("RETURN") => {
				self.pop_peek();
				let stmt = self.parse_return_stmt()?;
				Subquery::Output(stmt)
			}
			t!("SELECT") => {
				self.pop_peek();
				let stmt = self.parse_select_stmt()?;
				Subquery::Select(stmt)
			}
			t!("CREATE") => {
				self.pop_peek();
				let stmt = self.parse_create_stmt()?;
				Subquery::Create(stmt)
			}
			t!("UPDATE") => {
				self.pop_peek();
				let stmt = self.parse_update_stmt()?;
				Subquery::Update(stmt)
			}
			t!("DELETE") => {
				self.pop_peek();
				let stmt = self.parse_delete_stmt()?;
				Subquery::Delete(stmt)
			}
			t!("RELATE") => {
				self.pop_peek();
				let stmt = self.parse_relate_stmt()?;
				Subquery::Relate(stmt)
			}
			t!("DEFINE") => {
				self.pop_peek();
				let stmt = self.parse_define_stmt()?;
				Subquery::Define(stmt)
			}
			t!("REMOVE") => {
				self.pop_peek();
				let stmt = self.parse_remove_stmt()?;
				Subquery::Remove(stmt)
			}
			_ => {
				let value = self.parse_value_field()?;
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
				| t!("BEGIN")
				| t!("BREAK")
				| t!("CANCEL")
				| t!("COMMIT")
				| t!("CONTINUE")
				| t!("FOR") | t!("INFO")
				| t!("KILL") | t!("LIVE")
				| t!("OPTION")
				| t!("LET") | t!("SHOW")
				| t!("SLEEP")
				| t!("THROW")
				| t!("USE")
		)
	}

	/// Parses a strand with legacy rules, parsing to a record id, datetime or uuid if the string
	/// matches.
	pub fn parse_legacy_strand(&mut self) -> ParseResult<Value> {
		let text = self.lexer.string.take().unwrap();
		if let Ok(x) = Parser::new(text.as_bytes()).parse_thing() {
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

	fn parse_script(&mut self) -> ParseResult<Function> {
		let start = expected!(self, t!("(")).span;
		let mut args = Vec::new();
		loop {
			if self.eat(t!(")")) {
				break;
			}

			args.push(self.parse_value_field()?);

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
