use reblessive::Stk;

use crate::{
	sql::{
		part::{DestructurePart, Recurse},
		Dir, Edges, Field, Fields, Graph, Ident, Idiom, Part, Table, Tables, Value,
	},
	syn::{
		error::bail,
		token::{t, Glued, Span, TokenKind},
	},
};

use super::{mac::unexpected, ParseResult, Parser};

impl Parser<'_> {
	pub(super) fn peek_continues_idiom(&mut self) -> bool {
		let peek = self.peek().kind;
		if matches!(peek, t!("->") | t!("[") | t!(".") | t!("...") | t!("?")) {
			return true;
		}
		peek == t!("<") && matches!(self.peek1().kind, t!("-") | t!("->"))
	}

	/// Parse fields of a selecting query: `foo, bar` in `SELECT foo, bar FROM baz`.
	///
	/// # Parser State
	/// Expects the next tokens to be of a field set.
	pub(super) async fn parse_fields(&mut self, ctx: &mut Stk) -> ParseResult<Fields> {
		if self.eat(t!("VALUE")) {
			let expr = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
			let alias = if self.eat(t!("AS")) {
				Some(self.parse_plain_idiom(ctx).await?)
			} else {
				None
			};
			Ok(Fields(
				vec![Field::Single {
					expr,
					alias,
				}],
				true,
			))
		} else {
			let mut fields = Vec::new();
			loop {
				let field = if self.eat(t!("*")) {
					Field::All
				} else {
					let expr = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
					let alias = if self.eat(t!("AS")) {
						Some(self.parse_plain_idiom(ctx).await?)
					} else {
						None
					};
					Field::Single {
						expr,
						alias,
					}
				};
				fields.push(field);
				if !self.eat(t!(",")) {
					break;
				}
			}
			Ok(Fields(fields, false))
		}
	}

	/// Parses a list of idioms separated by a `,`
	pub(super) async fn parse_idiom_list(&mut self, ctx: &mut Stk) -> ParseResult<Vec<Idiom>> {
		let mut res = vec![self.parse_plain_idiom(ctx).await?];
		while self.eat(t!(",")) {
			res.push(self.parse_plain_idiom(ctx).await?);
		}
		Ok(res)
	}

	/// Parses the remaining idiom parts after the start: Any part like `...`, `.foo` and `->foo`
	///
	/// This function differes from [`Parser::parse_remaining_value_idiom`] in how it handles graph
	/// parsing. Graphs inside a plain idioms will remain a normal graph production.
	pub(super) async fn parse_remaining_idiom(
		&mut self,
		stk: &mut Stk,
		start: Vec<Part>,
	) -> ParseResult<Idiom> {
		let mut res = start;
		loop {
			match self.peek_kind() {
				t!("?") => {
					self.pop_peek();
					res.push(Part::Optional);
				}
				t!("...") => {
					self.pop_peek();
					res.push(Part::Flatten);
				}
				t!(".") => {
					self.pop_peek();
					res.push(self.parse_dot_part(stk).await?)
				}
				t!("[") => {
					let span = self.pop_peek().span;
					let part = self.parse_bracket_part(stk, span).await?;
					res.push(part)
				}
				t!("->") => {
					self.pop_peek();
					let graph = stk.run(|stk| self.parse_graph(stk, Dir::Out)).await?;
					res.push(Part::Graph(graph))
				}
				t!("<") => {
					let peek = self.peek_whitespace1();
					if peek.kind == t!("-") {
						self.pop_peek();
						self.pop_peek();
						let graph = stk.run(|stk| self.parse_graph(stk, Dir::In)).await?;
						res.push(Part::Graph(graph))
					} else if peek.kind == t!("->") {
						self.pop_peek();
						self.pop_peek();
						let graph = stk.run(|stk| self.parse_graph(stk, Dir::Both)).await?;
						res.push(Part::Graph(graph))
					} else {
						break;
					}
				}
				t!("..") => {
					bail!("Unexpected token `{}` expected and idiom",t!(".."),
						@self.last_span() => "Did you maybe intent to use the flatten operator `...`");
				}
				_ => break,
			}
		}
		Ok(Idiom(res))
	}

	/// Parses the remaining idiom parts after the start: Any part like `...`, `.foo` and `->foo`
	///
	///
	/// This function differes from [`Parser::parse_remaining_value_idiom`] in how it handles graph
	/// parsing. When parsing a idiom like production which can be a value, the initial start value
	/// might need to be changed to a Edge depending on what is parsed next.
	pub(super) async fn parse_remaining_value_idiom(
		&mut self,
		ctx: &mut Stk,
		start: Vec<Part>,
	) -> ParseResult<Value> {
		let mut res = start;
		loop {
			match self.peek_kind() {
				t!("?") => {
					self.pop_peek();
					res.push(Part::Optional);
				}
				t!("...") => {
					self.pop_peek();
					res.push(Part::Flatten);
				}
				t!(".") => {
					self.pop_peek();
					res.push(self.parse_dot_part(ctx).await?)
				}
				t!("[") => {
					let span = self.pop_peek().span;
					let part = self.parse_bracket_part(ctx, span).await?;
					res.push(part)
				}
				t!("->") => {
					self.pop_peek();
					if let Some(x) = self.parse_graph_idiom(ctx, &mut res, Dir::Out).await? {
						return Ok(x);
					}
				}
				t!("<") => {
					let peek = self.peek_whitespace1();
					if peek.kind == t!("-") {
						self.pop_peek();
						self.pop_peek();

						if let Some(x) = self.parse_graph_idiom(ctx, &mut res, Dir::In).await? {
							return Ok(x);
						}
					} else if peek.kind == t!("->") {
						self.pop_peek();
						self.pop_peek();

						if let Some(x) = self.parse_graph_idiom(ctx, &mut res, Dir::Both).await? {
							return Ok(x);
						}
					} else {
						break;
					}
				}
				t!("..") => {
					bail!("Unexpected token `{}` expected and idiom",t!(".."),
						@self.last_span() => "Did you maybe intent to use the flatten operator `...`");
				}
				_ => break,
			}
		}
		Ok(Value::Idiom(Idiom(res)))
	}

	/// Parse a graph idiom and possibly rewrite the starting value to be an edge whenever the
	/// parsed production matches `Thing -> Ident`.
	async fn parse_graph_idiom(
		&mut self,
		ctx: &mut Stk,
		res: &mut Vec<Part>,
		dir: Dir,
	) -> ParseResult<Option<Value>> {
		let graph = ctx.run(|ctx| self.parse_graph(ctx, dir)).await?;
		// the production `Thing Graph` is reparsed as an edge if the graph does not contain an
		// alias or a condition.
		if res.len() == 1 && graph.alias.is_none() && graph.cond.is_none() {
			match std::mem::replace(&mut res[0], Part::All) {
				Part::Value(Value::Thing(t)) | Part::Start(Value::Thing(t)) => {
					let edge = Edges {
						dir: graph.dir,
						from: t,
						what: graph.what,
					};
					let value = Value::Edges(Box::new(edge));

					if !self.peek_continues_idiom() {
						return Ok(Some(value));
					}
					res[0] = Part::Start(value);
					return Ok(None);
				}
				x => {
					res[0] = x;
				}
			}
		}
		res.push(Part::Graph(graph));
		Ok(None)
	}

	/// Parse a idiom which can only start with a graph or an identifier.
	/// Other expressions are not allowed as start of this idiom
	pub async fn parse_plain_idiom(&mut self, ctx: &mut Stk) -> ParseResult<Idiom> {
		let start = match self.peek_kind() {
			t!("->") => {
				self.pop_peek();
				let graph = ctx.run(|ctx| self.parse_graph(ctx, Dir::Out)).await?;
				Part::Graph(graph)
			}
			t!("<") => {
				let t = self.pop_peek();
				let graph = if self.eat_whitespace(t!("-")) {
					ctx.run(|ctx| self.parse_graph(ctx, Dir::In)).await?
				} else if self.eat_whitespace(t!("->")) {
					ctx.run(|ctx| self.parse_graph(ctx, Dir::Both)).await?
				} else {
					unexpected!(self, t, "either `<-` `<->` or `->`")
				};
				Part::Graph(graph)
			}
			_ => Part::Field(self.next_token_value()?),
		};
		let start = vec![start];
		self.parse_remaining_idiom(ctx, start).await
	}

	/// Parse the part after the `.` in a idiom
	pub(super) async fn parse_dot_part(&mut self, ctx: &mut Stk) -> ParseResult<Part> {
		let res = match self.peek_kind() {
			t!("*") => {
				self.pop_peek();
				Part::All
			}
			t!("@") => {
				self.pop_peek();
				Part::RepeatRecurse
			}
			t!("{") => {
				self.pop_peek();
				ctx.run(|ctx| self.parse_curly_part(ctx)).await?
			}
			_ => {
				let ident: Ident = self.next_token_value()?;
				if self.eat(t!("(")) {
					self.parse_function_part(ctx, ident).await?
				} else {
					Part::Field(ident)
				}
			}
		};
		Ok(res)
	}
	pub(super) async fn parse_function_part(
		&mut self,
		ctx: &mut Stk,
		name: Ident,
	) -> ParseResult<Part> {
		let args = self.parse_function_args(ctx).await?;
		Ok(Part::Method(name.0, args))
	}
	/// Parse the part after the `.{` in an idiom
	pub(super) async fn parse_curly_part(&mut self, ctx: &mut Stk) -> ParseResult<Part> {
		match self.peek_kind() {
			t!("*") | t!("..") | TokenKind::Digits => self.parse_recurse_part(ctx).await,
			_ => self.parse_destructure_part(ctx).await,
		}
	}
	/// Parse a destructure part, expects `.{` to already be parsed
	pub(super) async fn parse_destructure_part(&mut self, ctx: &mut Stk) -> ParseResult<Part> {
		let start = self.last_span();
		let mut destructured: Vec<DestructurePart> = Vec::new();
		loop {
			if self.eat(t!("}")) {
				// We've reached the end of the destructure
				break;
			}

			let field: Ident = self.next_token_value()?;
			let part = match self.peek_kind() {
				t!(":") => {
					self.pop_peek();
					let start = match self.peek_kind() {
						x if Parser::kind_is_identifier(x) => Part::Field(self.next_token_value()?),
						t!("->") => {
							self.pop_peek();
							let graph = ctx.run(|ctx| self.parse_graph(ctx, Dir::Out)).await?;
							Part::Graph(graph)
						}
						found @ t!("<") => match self.peek_whitespace1().kind {
							t!("-") => {
								self.pop_peek();
								self.pop_peek();
								let graph = ctx.run(|ctx| self.parse_graph(ctx, Dir::In)).await?;
								Part::Graph(graph)
							}
							t!("->") => {
								self.pop_peek();
								self.pop_peek();
								let graph = ctx.run(|ctx| self.parse_graph(ctx, Dir::Both)).await?;
								Part::Graph(graph)
							}
							_ => {
								bail!("Unexpected token `{}` expected an identifier, `->`, `<-` or `<->`", found, @self.recent_span());
							}
						},
						found => {
							bail!("Unexpected token `{}` expected an identifier, `->`, `<-` or `<->`", found, @self.recent_span());
						}
					};
					DestructurePart::Aliased(
						field,
						self.parse_remaining_idiom(ctx, vec![start]).await?,
					)
				}
				t!(".") => {
					self.pop_peek();
					let found = self.peek_kind();
					match self.parse_dot_part(ctx).await? {
						Part::All => DestructurePart::All(field),
						Part::Destructure(v) => DestructurePart::Destructure(field, v),
						_ => {
							bail!("Unexpected token `{}` expected a `*` or a destructuring", found, @self.last_span());
						}
					}
				}
				_ => DestructurePart::Field(field),
			};

			destructured.push(part);

			if !self.eat(t!(",")) {
				// We've reached the end of the destructure
				self.expect_closing_delimiter(t!("}"), start)?;
				break;
			}
		}

		Ok(Part::Destructure(destructured))
	}
	/// Parse the inner part of a recurse, expects a valid recurse value in the current position
	pub(super) fn parse_recurse_inner(&mut self) -> ParseResult<Recurse> {
		let min = if matches!(self.peek().kind, TokenKind::Digits) {
			Some(self.next_token_value::<u32>()?)
		} else {
			None
		};

		match (self.eat_whitespace(t!("..")), min) {
			(true, _) => (),
			(false, Some(v)) => {
				return Ok(Recurse::Fixed(v));
			}
			_ => {
				let found = self.next().kind;
				bail!("Unexpected token `{}` expected an integer or ..", found, @self.last_span());
			}
		}

		// parse ending id.
		let max = if matches!(self.peek_whitespace().kind, TokenKind::Digits) {
			Some(self.next_token_value::<u32>()?)
		} else {
			None
		};

		Ok(Recurse::Range(min, max))
	}
	/// Parse a recurse part, expects `.{` to already be parsed
	pub(super) async fn parse_recurse_part(&mut self, ctx: &mut Stk) -> ParseResult<Part> {
		let start = self.last_span();
		let recurse = self.parse_recurse_inner()?;
		self.expect_closing_delimiter(t!("}"), start)?;

		let nest = if self.eat(t!("(")) {
			let start = self.last_span();
			let idiom = self.parse_remaining_idiom(ctx, vec![]).await?;
			self.expect_closing_delimiter(t!(")"), start)?;
			Some(idiom)
		} else {
			None
		};

		Ok(Part::Recurse(recurse, nest))
	}
	/// Parse the part after the `[` in a idiom
	pub(super) async fn parse_bracket_part(
		&mut self,
		ctx: &mut Stk,
		start: Span,
	) -> ParseResult<Part> {
		let peek = self.peek();
		let res = match peek.kind {
			t!("*") => {
				self.pop_peek();
				Part::All
			}
			t!("$") => {
				self.pop_peek();
				Part::Last
			}
			t!("?") | t!("WHERE") => {
				self.pop_peek();
				let value = ctx.run(|ctx| self.parse_value_field(ctx)).await?;
				Part::Where(value)
			}
			_ => {
				let value = ctx.run(|ctx| self.parse_value_inherit(ctx)).await?;
				if let Value::Number(x) = value {
					Part::Index(x)
				} else {
					Part::Value(value)
				}
			}
		};
		self.expect_closing_delimiter(t!("]"), start)?;
		Ok(res)
	}

	/// Parse a basic idiom.
	///
	/// Basic idioms differ from normal idioms in that they are more restrictive.
	/// Flatten, graphs, conditions and indexing by param is not allowed.
	pub(super) async fn parse_basic_idiom(&mut self, ctx: &mut Stk) -> ParseResult<Idiom> {
		let start = self.next_token_value::<Ident>()?;
		let mut parts = vec![Part::Field(start)];
		loop {
			let token = self.peek();
			let part = match token.kind {
				t!(".") => {
					self.pop_peek();
					self.parse_dot_part(ctx).await?
				}
				t!("[") => {
					self.pop_peek();
					let peek = self.peek();
					let res = match peek.kind {
						t!("*") => {
							self.pop_peek();
							Part::All
						}
						t!("$") => {
							self.pop_peek();
							Part::Last
						}
						TokenKind::Digits | t!("+") | TokenKind::Glued(Glued::Number) => {
							let number = self.next_token_value()?;
							Part::Index(number)
						}
						t!("-") => {
							let peek_digit = self.peek_whitespace1();
							if let TokenKind::Digits = peek_digit.kind {
								let span = self.recent_span().covers(peek_digit.span);
								bail!("Unexpected token `-` expected $, *, or a number", @span => "an index can't be negative");
							}
							unexpected!(self, peek, "$, * or a number");
						}
						_ => unexpected!(self, peek, "$, * or a number"),
					};
					self.expect_closing_delimiter(t!("]"), token.span)?;
					res
				}
				_ => break,
			};
			parts.push(part);
		}
		Ok(Idiom(parts))
	}

	/// Parse a local idiom.
	///
	/// Basic idioms differ from local idioms in that they are more restrictive.
	/// Only field, all and number indexing is allowed. Flatten is also allowed but only at the
	/// end.
	pub(super) async fn parse_local_idiom(&mut self, ctx: &mut Stk) -> ParseResult<Idiom> {
		let start = self.next_token_value()?;
		let mut parts = vec![Part::Field(start)];
		loop {
			let token = self.peek();
			let part = match token.kind {
				t!(".") => {
					self.pop_peek();
					self.parse_dot_part(ctx).await?
				}
				t!("[") => {
					self.pop_peek();
					let token = self.peek();
					let res = match token.kind {
						t!("*") => {
							self.pop_peek();
							Part::All
						}
						TokenKind::Digits | t!("+") | TokenKind::Glued(Glued::Number) => {
							let number = self.next_token_value()?;
							Part::Index(number)
						}
						t!("-") => {
							let peek_digit = self.peek_whitespace1();
							if let TokenKind::Digits = peek_digit.kind {
								let span = self.recent_span().covers(peek_digit.span);
								bail!("Unexpected token `-` expected $, *, or a number", @span => "an index can't be negative");
							}
							unexpected!(self, token, "$, * or a number");
						}
						_ => unexpected!(self, token, "$, * or a number"),
					};
					self.expect_closing_delimiter(t!("]"), token.span)?;
					res
				}
				_ => break,
			};

			parts.push(part);
		}

		if self.eat(t!("...")) {
			let token = self.peek();
			if let t!(".") | t!("[") = token.kind {
				bail!("Unexpected token `...` expected a local idiom to end.",
					@token.span => "Flattening can only be done at the end of a local idiom")
			}
			parts.push(Part::Flatten);
		}

		Ok(Idiom(parts))
	}

	/// Parses a list of what values seperated by comma's
	///
	/// # Parser state
	/// Expects to be at the start of a what list.
	pub(super) async fn parse_what_list(&mut self, ctx: &mut Stk) -> ParseResult<Vec<Value>> {
		let mut res = vec![self.parse_what_value(ctx).await?];
		while self.eat(t!(",")) {
			res.push(self.parse_what_value(ctx).await?)
		}
		Ok(res)
	}

	/// Parses a single what value,
	///
	/// # Parser state
	/// Expects to be at the start of a what value
	pub(super) async fn parse_what_value(&mut self, ctx: &mut Stk) -> ParseResult<Value> {
		let start = self.parse_what_primary(ctx).await?;
		if start.can_start_idiom() && self.peek_continues_idiom() {
			let start = match start {
				Value::Table(Table(x)) => vec![Part::Field(Ident(x))],
				Value::Idiom(Idiom(x)) => x,
				x => vec![Part::Start(x)],
			};

			let idiom = self.parse_remaining_value_idiom(ctx, start).await?;
			Ok(idiom)
		} else {
			Ok(start)
		}
	}

	/// Parses a graph value
	///
	/// # Parser state
	/// Expects to just have eaten a direction (e.g. <-, <->, or ->) and be at the field like part
	/// of the graph
	pub(super) async fn parse_graph(&mut self, ctx: &mut Stk, dir: Dir) -> ParseResult<Graph> {
		let token = self.peek();
		match token.kind {
			t!("?") => {
				self.pop_peek();
				Ok(Graph {
					dir,
					..Default::default()
				})
			}
			t!("(") => {
				let span = self.pop_peek().span;
				let token = self.peek();
				let what = match token.kind {
					t!("?") => {
						self.pop_peek();
						Tables::default()
					}
					x if Self::kind_is_identifier(x) => {
						// The following function should always succeed here,
						// returning an error here would be a bug, so unwrap.
						let table = self.next_token_value().unwrap();
						let mut tables = Tables(vec![table]);
						while self.eat(t!(",")) {
							tables.0.push(self.next_token_value()?);
						}
						tables
					}
					_ => unexpected!(self, token, "`?` or an identifier"),
				};

				let cond = self.try_parse_condition(ctx).await?;
				let alias = if self.eat(t!("AS")) {
					Some(self.parse_plain_idiom(ctx).await?)
				} else {
					None
				};

				self.expect_closing_delimiter(t!(")"), span)?;

				Ok(Graph {
					dir,
					what,
					cond,
					alias,
					expr: Fields::all(),
					..Default::default()
				})
			}
			x if Self::kind_is_identifier(x) => {
				// The following function should always succeed here,
				// returning an error here would be a bug, so unwrap.
				let table = self.next_token_value().unwrap();
				Ok(Graph {
					dir,
					expr: Fields::all(),
					what: Tables(vec![table]),
					..Default::default()
				})
			}
			_ => unexpected!(self, token, "`?`, `(` or an identifier"),
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::sql::{Expression, Id, Number, Object, Operator, Param, Strand, Thing};
	use crate::syn::Parse;

	use super::*;

	#[test]
	fn graph_in() {
		let sql = "<-likes";
		let out = Value::parse(sql);
		assert_eq!("<-likes", format!("{}", out));
	}

	#[test]
	fn graph_out() {
		let sql = "->likes";
		let out = Value::parse(sql);
		assert_eq!("->likes", format!("{}", out));
	}

	#[test]
	fn graph_both() {
		let sql = "<->likes";
		let out = Value::parse(sql);
		assert_eq!("<->likes", format!("{}", out));
	}

	#[test]
	fn graph_multiple() {
		let sql = "->(likes, follows)";
		let out = Value::parse(sql);
		assert_eq!("->(likes, follows)", format!("{}", out));
	}

	#[test]
	fn graph_aliases() {
		let sql = "->(likes, follows AS connections)";
		let out = Value::parse(sql);
		assert_eq!("->(likes, follows AS connections)", format!("{}", out));
	}

	#[test]
	fn graph_conditions() {
		let sql = "->(likes, follows WHERE influencer = true)";
		let out = Value::parse(sql);
		assert_eq!("->(likes, follows WHERE influencer = true)", format!("{}", out));
	}

	#[test]
	fn graph_conditions_aliases() {
		let sql = "->(likes, follows WHERE influencer = true AS connections)";
		let out = Value::parse(sql);
		assert_eq!("->(likes, follows WHERE influencer = true AS connections)", format!("{}", out));
	}

	#[test]
	fn idiom_normal() {
		let sql = "test";
		let out = Value::parse(sql);
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Value::from(Idiom(vec![Part::from("test")])));
	}

	#[test]
	fn idiom_quoted_backtick() {
		let sql = "`test`";
		let out = Value::parse(sql);
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Value::from(Idiom(vec![Part::from("test")])));
	}

	#[test]
	fn idiom_quoted_brackets() {
		let sql = "⟨test⟩";
		let out = Value::parse(sql);
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, Value::from(Idiom(vec![Part::from("test")])));
	}

	#[test]
	fn idiom_nested() {
		let sql = "test.temp";
		let out = Value::parse(sql);
		assert_eq!("test.temp", format!("{}", out));
		assert_eq!(out, Value::from(Idiom(vec![Part::from("test"), Part::from("temp")])));
	}

	#[test]
	fn idiom_nested_quoted() {
		let sql = "test.`some key`";
		let out = Value::parse(sql);
		assert_eq!("test.`some key`", format!("{}", out));
		assert_eq!(out, Value::from(Idiom(vec![Part::from("test"), Part::from("some key")])));
	}

	#[test]
	fn idiom_nested_array_all() {
		let sql = "test.temp[*]";
		let out = Value::parse(sql);
		assert_eq!("test.temp[*]", format!("{}", out));
		assert_eq!(
			out,
			Value::from(Idiom(vec![Part::from("test"), Part::from("temp"), Part::All]))
		);
	}

	#[test]
	fn idiom_nested_array_last() {
		let sql = "test.temp[$]";
		let out = Value::parse(sql);
		assert_eq!("test.temp[$]", format!("{}", out));
		assert_eq!(
			out,
			Value::from(Idiom(vec![Part::from("test"), Part::from("temp"), Part::Last]))
		);
	}

	#[test]
	fn idiom_nested_array_value() {
		let sql = "test.temp[*].text";
		let out = Value::parse(sql);
		assert_eq!("test.temp[*].text", format!("{}", out));
		assert_eq!(
			out,
			Value::from(Idiom(vec![
				Part::from("test"),
				Part::from("temp"),
				Part::All,
				Part::from("text")
			]))
		);
	}

	#[test]
	fn idiom_nested_array_question() {
		let sql = "test.temp[? test = true].text";
		let out = Value::parse(sql);
		assert_eq!("test.temp[WHERE test = true].text", format!("{}", out));
		assert_eq!(
			out,
			Value::from(Idiom(vec![
				Part::from("test"),
				Part::from("temp"),
				Part::Where(Value::Expression(Box::new(Expression::Binary {
					l: Value::Idiom(Idiom(vec![Part::Field(Ident("test".to_string()))])),
					o: Operator::Equal,
					r: Value::Bool(true)
				}))),
				Part::from("text")
			]))
		);
	}

	#[test]
	fn idiom_nested_array_condition() {
		let sql = "test.temp[WHERE test = true].text";
		let out = Value::parse(sql);
		assert_eq!("test.temp[WHERE test = true].text", format!("{}", out));
		assert_eq!(
			out,
			Value::from(Idiom(vec![
				Part::from("test"),
				Part::from("temp"),
				Part::Where(Value::Expression(Box::new(Expression::Binary {
					l: Value::Idiom(Idiom(vec![Part::Field(Ident("test".to_string()))])),
					o: Operator::Equal,
					r: Value::Bool(true)
				}))),
				Part::from("text")
			]))
		);
	}

	#[test]
	fn idiom_start_param_local_field() {
		let sql = "$test.temporary[0].embedded…";
		let out = Value::parse(sql);
		assert_eq!("$test.temporary[0].embedded…", format!("{}", out));
		assert_eq!(
			out,
			Value::from(Idiom(vec![
				Part::Start(Param::from("test").into()),
				Part::from("temporary"),
				Part::Index(Number::Int(0)),
				Part::from("embedded"),
				Part::Flatten,
			]))
		);
	}

	#[test]
	fn idiom_start_thing_remote_traversal() {
		let sql = "person:test.friend->like->person";
		let out = Value::parse(sql);
		assert_eq!("person:test.friend->like->person", format!("{}", out));
		assert_eq!(
			out,
			Value::from(Idiom(vec![
				Part::Start(Thing::from(("person", "test")).into()),
				Part::from("friend"),
				Part::Graph(Graph {
					dir: Dir::Out,
					expr: Fields::all(),
					what: Table::from("like").into(),
					cond: None,
					alias: None,
					split: None,
					group: None,
					order: None,
					limit: None,
					start: None,
				}),
				Part::Graph(Graph {
					dir: Dir::Out,
					expr: Fields::all(),
					what: Table::from("person").into(),
					cond: None,
					alias: None,
					split: None,
					group: None,
					order: None,
					limit: None,
					start: None,
				}),
			]))
		);
	}

	#[test]
	fn part_all() {
		let sql = "{}[*]";
		let out = Value::parse(sql);
		assert_eq!("{  }[*]", format!("{}", out));
		assert_eq!(
			out,
			Value::from(Idiom(vec![Part::Start(Value::from(Object::default())), Part::All]))
		);
	}

	#[test]
	fn part_last() {
		let sql = "{}[$]";
		let out = Value::parse(sql);
		assert_eq!("{  }[$]", format!("{}", out));
		assert_eq!(
			out,
			Value::from(Idiom(vec![Part::Start(Value::from(Object::default())), Part::Last]))
		);
	}

	#[test]
	fn part_param() {
		let sql = "{}[$param]";
		let out = Value::parse(sql);
		assert_eq!("{  }[$param]", format!("{}", out));
		assert_eq!(
			out,
			Value::from(Idiom(vec![
				Part::Start(Value::from(Object::default())),
				Part::Value(Value::Param(Param::from("param")))
			]))
		);
	}

	#[test]
	fn part_flatten() {
		let sql = "{}...";
		let out = Value::parse(sql);
		assert_eq!("{  }…", format!("{}", out));
		assert_eq!(
			out,
			Value::from(Idiom(vec![Part::Start(Value::from(Object::default())), Part::Flatten]))
		);
	}

	#[test]
	fn part_flatten_ellipsis() {
		let sql = "{}…";
		let out = Value::parse(sql);
		assert_eq!("{  }…", format!("{}", out));
		assert_eq!(
			out,
			Value::from(Idiom(vec![Part::Start(Value::from(Object::default())), Part::Flatten]))
		);
	}

	#[test]
	fn part_number() {
		let sql = "{}[0]";
		let out = Value::parse(sql);
		assert_eq!("{  }[0]", format!("{}", out));
		assert_eq!(
			out,
			Value::from(Idiom(vec![
				Part::Start(Value::from(Object::default())),
				Part::Index(Number::from(0))
			]))
		);
	}

	#[test]
	fn part_expression_question() {
		let sql = "{}[?test = true]";
		let out = Value::parse(sql);
		assert_eq!("{  }[WHERE test = true]", format!("{}", out));
		assert_eq!(
			out,
			Value::from(Idiom(vec![
				Part::Start(Value::from(Object::default())),
				Part::Where(Value::Expression(Box::new(Expression::Binary {
					l: Value::Idiom(Idiom(vec![Part::Field(Ident("test".to_string()))])),
					o: Operator::Equal,
					r: Value::Bool(true)
				}))),
			]))
		);
	}

	#[test]
	fn part_expression_condition() {
		let sql = "{}[WHERE test = true]";
		let out = Value::parse(sql);
		assert_eq!("{  }[WHERE test = true]", format!("{}", out));
		assert_eq!(
			out,
			Value::from(Idiom(vec![
				Part::Start(Value::from(Object::default())),
				Part::Where(Value::Expression(Box::new(Expression::Binary {
					l: Value::Idiom(Idiom(vec![Part::Field(Ident("test".to_string()))])),
					o: Operator::Equal,
					r: Value::Bool(true)
				}))),
			]))
		);
	}

	#[test]
	fn idiom_thing_number() {
		let sql = "test:1.foo";
		let out = Value::parse(sql);
		assert_eq!(
			out,
			Value::from(Idiom(vec![
				Part::Start(Value::Thing(Thing {
					tb: "test".to_owned(),
					id: Id::from(1),
				})),
				Part::from("foo"),
			]))
		);
	}

	#[test]
	fn idiom_thing_index() {
		let sql = "test:1['foo']";
		let out = Value::parse(sql);
		assert_eq!(
			out,
			Value::from(Idiom(vec![
				Part::Start(Value::Thing(Thing {
					tb: "test".to_owned(),
					id: Id::from(1),
				})),
				Part::Value(Value::Strand(Strand("foo".to_owned()))),
			]))
		);
	}

	#[test]
	fn idiom_thing_all() {
		let sql = "test:1.*";
		let out = Value::parse(sql);
		assert_eq!(
			out,
			Value::from(Idiom(vec![
				Part::Start(Value::Thing(Thing {
					tb: "test".to_owned(),
					id: Id::from(1),
				})),
				Part::All
			]))
		);
	}
}
