use reblessive::Stk;

use super::basic::NumberToken;
use super::mac::{expected, unexpected};
use super::{ParseResult, Parser};
use crate::sql::lookup::LookupKind;
use crate::sql::part::{DestructurePart, Recurse, RecurseInstruction};
use crate::sql::{Dir, Expr, Field, Fields, Ident, Idiom, Literal, Lookup, Param, Part};
use crate::syn::error::bail;
use crate::syn::lexer::compound::{self, Numeric};
use crate::syn::token::{Glued, Span, TokenKind, t};

impl Parser<'_> {
	pub(super) fn peek_continues_idiom(&mut self) -> bool {
		let peek = self.peek().kind;
		if matches!(peek, t!("->") | t!("[") | t!(".") | t!("...") | t!("?")) {
			return true;
		}
		peek == t!("<") && matches!(self.peek1().kind, t!("-") | t!("~") | t!("->"))
	}

	/// Parse fields of a selecting query: `foo, bar` in `SELECT foo, bar FROM
	/// baz`.
	///
	/// # Parser State
	/// Expects the next tokens to be of a field set.
	pub(crate) async fn parse_fields(&mut self, stk: &mut Stk) -> ParseResult<Fields> {
		if self.eat(t!("VALUE")) {
			let expr = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
			let alias = if self.eat(t!("AS")) {
				Some(self.parse_plain_idiom(stk).await?)
			} else {
				None
			};
			Ok(Fields::Value(Box::new(Field::Single {
				expr,
				alias,
			})))
		} else {
			let mut fields = Vec::new();
			loop {
				let field = if self.eat(t!("*")) {
					Field::All
				} else {
					let expr = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
					let alias = if self.eat(t!("AS")) {
						Some(self.parse_plain_idiom(stk).await?)
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
			Ok(Fields::Select(fields))
		}
	}

	/// Parses a list of idioms separated by a `,`
	pub(super) async fn parse_idiom_list(&mut self, stk: &mut Stk) -> ParseResult<Vec<Idiom>> {
		let mut res = vec![self.parse_plain_idiom(stk).await?];
		while self.eat(t!(",")) {
			res.push(self.parse_plain_idiom(stk).await?);
		}
		Ok(res)
	}

	/// Parses the remaining idiom parts after the start: Any part like `...`,
	/// `.foo` and `->foo`
	///
	/// This function differes from [`Parser::parse_remaining_value_idiom`] in
	/// how it handles graph parsing. Graphs inside a plain idioms will remain
	/// a normal graph production.
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
					let lookup =
						stk.run(|stk| self.parse_lookup(stk, LookupKind::Graph(Dir::Out))).await?;
					res.push(Part::Graph(lookup))
				}
				t!("<") => {
					let peek = self.peek_whitespace1();
					if peek.kind == t!("~") {
						self.pop_peek();
						self.pop_peek();
						if !self.settings.references_enabled {
							bail!(
								"Experimental capability `record_references` is not enabled",
								@self.last_span() => "Use of `<~` reference lookup is still experimental"
							)
						}

						let lookup =
							stk.run(|stk| self.parse_lookup(stk, LookupKind::Reference)).await?;
						res.push(Part::Graph(lookup))
					} else if peek.kind == t!("-") {
						self.pop_peek();
						self.pop_peek();
						let lookup = stk
							.run(|stk| self.parse_lookup(stk, LookupKind::Graph(Dir::In)))
							.await?;
						res.push(Part::Graph(lookup))
					} else if peek.kind == t!("->") {
						self.pop_peek();
						self.pop_peek();
						let lookup = stk
							.run(|stk| self.parse_lookup(stk, LookupKind::Graph(Dir::Both)))
							.await?;
						res.push(Part::Graph(lookup))
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

	/// Parses the remaining idiom parts after the start: Any part like `...`,
	/// `.foo` and `->foo`
	///
	///
	/// This function differes from [`Parser::parse_remaining_value_idiom`] in
	/// how it handles graph parsing. When parsing a idiom like production
	/// which can be a value, the initial start value might need to be changed
	/// to a Edge depending on what is parsed next.
	pub(super) async fn parse_remaining_value_idiom(
		&mut self,
		stk: &mut Stk,
		start: Vec<Part>,
	) -> ParseResult<Expr> {
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
					let x = self.parse_lookup(stk, LookupKind::Graph(Dir::Out)).await?;
					res.push(Part::Graph(x))
				}
				t!("<") => {
					let peek = self.peek_whitespace1();
					if peek.kind == t!("~") {
						self.pop_peek();
						self.pop_peek();
						if !self.settings.references_enabled {
							bail!(
								"Experimental capability `record_references` is not enabled",
								@self.last_span() => "Use of `<~` reference lookup is still experimental"
							)
						}

						let lookup = self.parse_lookup(stk, LookupKind::Reference).await?;
						res.push(Part::Graph(lookup))
					} else if peek.kind == t!("-") {
						self.pop_peek();
						self.pop_peek();
						let lookup = self.parse_lookup(stk, LookupKind::Graph(Dir::In)).await?;
						res.push(Part::Graph(lookup))
					} else if peek.kind == t!("->") {
						self.pop_peek();
						self.pop_peek();
						let lookup = self.parse_lookup(stk, LookupKind::Graph(Dir::Both)).await?;
						res.push(Part::Graph(lookup))
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
		Ok(Expr::Idiom(Idiom(res)))
	}

	/// Parse a idiom which can only start with a graph or an identifier.
	/// Other expressions are not allowed as start of this idiom
	pub async fn parse_plain_idiom(&mut self, stk: &mut Stk) -> ParseResult<Idiom> {
		let start = match self.peek_kind() {
			t!("->") => {
				self.pop_peek();
				let lookup =
					stk.run(|ctx| self.parse_lookup(ctx, LookupKind::Graph(Dir::Out))).await?;
				Part::Graph(lookup)
			}
			t!("<") => {
				let t = self.pop_peek();
				let lookup = if self.eat_whitespace(t!("~")) {
					if !self.settings.references_enabled {
						bail!(
							"Experimental capability `record_references` is not enabled",
							@self.last_span() => "Use of `<~` reference lookup is still experimental"
						)
					}

					stk.run(|ctx| self.parse_lookup(ctx, LookupKind::Reference)).await?
				} else if self.eat_whitespace(t!("-")) {
					stk.run(|ctx| self.parse_lookup(ctx, LookupKind::Graph(Dir::In))).await?
				} else if self.eat_whitespace(t!("->")) {
					stk.run(|ctx| self.parse_lookup(ctx, LookupKind::Graph(Dir::Both))).await?
				} else {
					unexpected!(self, t, "either `<-` `<->` or `->`")
				};
				Part::Graph(lookup)
			}
			_ => Part::Field(self.next_token_value()?),
		};
		let start = vec![start];
		self.parse_remaining_idiom(stk, start).await
	}

	/// Parse the part after the `.` in a idiom
	pub(super) async fn parse_dot_part(&mut self, stk: &mut Stk) -> ParseResult<Part> {
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
				stk.run(|ctx| self.parse_curly_part(ctx)).await?
			}
			_ => {
				let ident: Ident = self.next_token_value()?;
				if self.eat(t!("(")) {
					self.parse_function_part(stk, ident).await?
				} else {
					Part::Field(ident)
				}
			}
		};
		Ok(res)
	}
	pub(super) async fn parse_function_part(
		&mut self,
		stk: &mut Stk,
		name: Ident,
	) -> ParseResult<Part> {
		let args = self.parse_function_args(stk).await?;
		Ok(Part::Method(name.into_string(), args))
	}
	/// Parse the part after the `.{` in an idiom
	pub(super) async fn parse_curly_part(&mut self, stk: &mut Stk) -> ParseResult<Part> {
		match self.peek_kind() {
			t!("*") | t!("..") | TokenKind::Digits => self.parse_recurse_part(stk).await,
			_ => self.parse_destructure_part(stk).await,
		}
	}
	/// Parse a destructure part, expects `.{` to already be parsed
	pub(super) async fn parse_destructure_part(&mut self, stk: &mut Stk) -> ParseResult<Part> {
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
					let idiom = match self.parse_expr_field(stk).await? {
						Expr::Idiom(x) => x,
						v => Idiom(vec![Part::Start(v)]),
					};
					DestructurePart::Aliased(field, idiom)
				}
				t!(".") => {
					self.pop_peek();
					let found = self.peek_kind();
					match self.parse_dot_part(stk).await? {
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
	/// Parse the inner part of a recurse, expects a valid recurse value in the
	/// current position
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
	/// Parse a recursion instruction following the inner recurse part, if any
	pub(super) async fn parse_recurse_instruction(
		&mut self,
		stk: &mut Stk,
	) -> ParseResult<Option<RecurseInstruction>> {
		let instruction = if self.eat(t!("+")) {
			let kind = self.next_token_value::<Ident>()?;
			if kind.eq_ignore_ascii_case("path") {
				let mut inclusive = false;
				loop {
					if self.eat(t!("+")) {
						let kind = self.next_token_value::<Ident>()?;
						if kind.eq_ignore_ascii_case("inclusive") {
							inclusive = true
						} else {
							bail!("Unexpected option `{}` expected `inclusive`",kind, @self.last_span());
						}
					} else {
						break;
					};
				}
				Some(RecurseInstruction::Path {
					inclusive,
				})
			} else if kind.eq_ignore_ascii_case("collect") {
				let mut inclusive = false;
				loop {
					if self.eat(t!("+")) {
						let kind = self.next_token_value::<Ident>()?;
						if kind.eq_ignore_ascii_case("inclusive") {
							inclusive = true
						} else {
							bail!("Unexpected option `{}` expected `inclusive`",kind, @self.last_span());
						}
					} else {
						break;
					};
				}
				Some(RecurseInstruction::Collect {
					inclusive,
				})
			} else if kind.eq_ignore_ascii_case("shortest") {
				expected!(self, t!("="));
				let token = self.peek();
				let expects = match token.kind {
					TokenKind::Parameter => Expr::Param(self.next_token_value::<Param>()?),
					x if Parser::kind_is_identifier(x) => {
						Expr::Literal(Literal::RecordId(self.parse_record_id(stk).await?))
					}
					_ => {
						unexpected!(self, token, "a param or record-id");
					}
				};
				let mut inclusive = false;
				loop {
					if self.eat(t!("+")) {
						let kind = self.next_token_value::<Ident>()?;
						if kind.eq_ignore_ascii_case("inclusive") {
							inclusive = true
						} else {
							bail!("Unexpected option `{}` expected `inclusive`",kind, @self.last_span());
						}
					} else {
						break;
					};
				}
				Some(RecurseInstruction::Shortest {
					expects,
					inclusive,
				})
			} else {
				bail!("Unexpected instruction `{}` expected `path`, `collect`, or `shortest`",kind, @self.last_span());
			}
		} else {
			None
		};

		Ok(instruction)
	}
	/// Parse a recurse part, expects `.{` to already be parsed
	pub(super) async fn parse_recurse_part(&mut self, stk: &mut Stk) -> ParseResult<Part> {
		let start = self.last_span();
		let recurse = self.parse_recurse_inner()?;
		let instruction = self.parse_recurse_instruction(stk).await?;
		self.expect_closing_delimiter(t!("}"), start)?;

		let nest = if self.eat(t!("(")) {
			let start = self.last_span();
			let idiom = self.parse_remaining_idiom(stk, vec![]).await?;
			self.expect_closing_delimiter(t!(")"), start)?;
			Some(idiom)
		} else {
			None
		};

		Ok(Part::Recurse(recurse, nest, instruction))
	}

	/// Parse the part after the `[` in a idiom
	pub(super) async fn parse_bracket_part(
		&mut self,
		stk: &mut Stk,
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
				let value = stk.run(|ctx| self.parse_expr_field(ctx)).await?;
				Part::Where(value)
			}
			_ => {
				let value = stk.run(|ctx| self.parse_expr_inherit(ctx)).await?;
				Part::Value(value)
			}
		};
		self.expect_closing_delimiter(t!("]"), start)?;
		Ok(res)
	}

	/// Parse a basic idiom.
	///
	/// Basic idioms differ from normal idioms in that they are more
	/// restrictive. Flatten, graphs, conditions and indexing by param is not
	/// allowed.
	pub(super) async fn parse_basic_idiom(&mut self, stk: &mut Stk) -> ParseResult<Idiom> {
		let start = self.next_token_value::<Ident>()?;
		let mut parts = vec![Part::Field(start)];
		loop {
			let token = self.peek();
			let part = match token.kind {
				t!(".") => {
					self.pop_peek();
					self.parse_dot_part(stk).await?
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
							let number = self.next_token_value::<NumberToken>()?;
							let expr = match number {
								NumberToken::Float(x) => Expr::Literal(Literal::Float(x)),
								NumberToken::Integer(x) => Expr::Literal(Literal::Integer(x)),
								NumberToken::Decimal(x) => Expr::Literal(Literal::Decimal(x)),
							};
							Part::Value(expr)
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
	/// Only field, all and number indexing is allowed. Flatten is also allowed
	/// but only at the end.
	pub(super) async fn parse_local_idiom(&mut self, stk: &mut Stk) -> ParseResult<Idiom> {
		let start = self.next_token_value()?;
		let mut parts = vec![Part::Field(start)];
		loop {
			let token = self.peek();
			let part = match token.kind {
				t!(".") => {
					self.pop_peek();
					self.parse_dot_part(stk).await?
				}
				t!("[") => {
					self.pop_peek();
					let token = self.peek();
					let res = match token.kind {
						t!("*") => {
							self.pop_peek();
							Part::All
						}
						TokenKind::Digits | t!("+") => {
							let next = self.next();
							let number = self.lexer.lex_compound(next, compound::numeric)?;
							let number = match number.value {
								Numeric::Duration(_) => {
									bail!("Unexpected token `duration` expected a number", @number.span );
								}
								Numeric::Integer(x) => Expr::Literal(Literal::Integer(x)),
								Numeric::Float(x) => Expr::Literal(Literal::Float(x)),
								Numeric::Decimal(x) => Expr::Literal(Literal::Decimal(x)),
							};
							Part::Value(number)
						}
						TokenKind::Glued(Glued::Number) => {
							let number = self.next_token_value::<NumberToken>()?;
							let number = match number {
								NumberToken::Float(f) => Expr::Literal(Literal::Float(f)),
								NumberToken::Integer(i) => Expr::Literal(Literal::Integer(i)),
								NumberToken::Decimal(decimal) => {
									Expr::Literal(Literal::Decimal(decimal))
								}
							};
							Part::Value(number)
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
	pub(super) async fn parse_what_list(&mut self, stk: &mut Stk) -> ParseResult<Vec<Expr>> {
		let mut res = vec![stk.run(|ctx| self.parse_expr_table(ctx)).await?];
		while self.eat(t!(",")) {
			res.push(stk.run(|ctx| self.parse_expr_table(ctx)).await?)
		}
		Ok(res)
	}

	/// Parses a graph value
	///
	/// # Parser state
	/// Expects to just have eaten a direction (e.g. <-, <->, or ->) and be at
	/// the field like part of the graph
	pub(super) async fn parse_lookup(
		&mut self,
		stk: &mut Stk,
		kind: LookupKind,
	) -> ParseResult<Lookup> {
		let token = self.peek();
		match token.kind {
			t!("?") => {
				self.pop_peek();
				Ok(Lookup {
					kind,
					..Default::default()
				})
			}
			t!("(") => {
				let span = self.pop_peek().span;
				let expr = if self.eat(t!("SELECT")) {
					let before = self.peek().span;
					let expr = self.parse_fields(stk).await?;
					let fields_span = before.covers(self.last_span());
					expected!(self, t!("FROM"));
					Some((expr, fields_span))
				} else {
					None
				};

				let token = self.peek();
				let what = match token.kind {
					t!("?") => {
						self.pop_peek();
						Vec::new()
					}
					x if Self::kind_is_identifier(x) => {
						let subject = self.parse_lookup_subject(stk).await?;
						let mut subjects = vec![subject];
						while self.eat(t!(",")) {
							subjects.push(self.parse_lookup_subject(stk).await?);
						}
						subjects
					}
					_ => unexpected!(self, token, "`?`, an identifier or a range"),
				};

				let cond = self.try_parse_condition(stk).await?;
				let (split, group, order) = if let Some((ref expr, fields_span)) = expr {
					let split = self.try_parse_split(stk, expr, fields_span).await?;
					let group = self.try_parse_group(stk, expr, fields_span).await?;
					let order = self.try_parse_orders(stk, expr, fields_span).await?;
					(split, group, order)
				} else {
					(None, None, None)
				};

				let (limit, start) = if let t!("START") = self.peek_kind() {
					let start = self.try_parse_start(stk).await?;
					let limit = self.try_parse_limit(stk).await?;
					(limit, start)
				} else {
					let limit = self.try_parse_limit(stk).await?;
					let start = self.try_parse_start(stk).await?;
					(limit, start)
				};

				let alias = if self.eat(t!("AS")) {
					Some(self.parse_plain_idiom(stk).await?)
				} else {
					None
				};

				self.expect_closing_delimiter(t!(")"), span)?;

				Ok(Lookup {
					kind,
					what,
					cond,
					alias,
					expr: expr.map(|(x, _)| x),
					split,
					group,
					order,
					limit,
					start,
				})
			}
			x if Self::kind_is_identifier(x) => {
				// The following function should always succeed here,
				// returning an error here would be a bug, so unwrap.
				let subject = self.parse_lookup_subject(stk).await?;
				Ok(Lookup {
					kind,
					what: vec![subject],
					..Default::default()
				})
			}
			_ => unexpected!(self, token, "`?`, `(` or an identifier"),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::lookup::LookupSubject;
	use crate::sql::{self, BinaryOperator, RecordIdKeyLit, RecordIdLit};
	use crate::syn;

	#[test]
	fn graph_in() {
		let sql = "<-likes";
		let out = syn::expr(sql).unwrap();
		assert_eq!("<-likes", format!("{}", out));
	}

	#[test]
	fn graph_out() {
		let sql = "->likes";
		let out = syn::expr(sql).unwrap();
		assert_eq!("->likes", format!("{}", out));
	}

	#[test]
	fn graph_both() {
		let sql = "<->likes";
		let out = syn::expr(sql).unwrap();
		assert_eq!("<->likes", format!("{}", out));
	}

	#[test]
	fn graph_multiple() {
		let sql = "->(likes, follows)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("->(likes, follows)", format!("{}", out));
	}

	#[test]
	fn graph_aliases() {
		let sql = "->(likes, follows AS connections)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("->(likes, follows AS connections)", format!("{}", out));
	}

	#[test]
	fn graph_conditions() {
		let sql = "->(likes, follows WHERE influencer = true)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("->(likes, follows WHERE influencer = true)", format!("{}", out));
	}

	#[test]
	fn graph_conditions_aliases() {
		let sql = "->(likes, follows WHERE influencer = true AS connections)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("->(likes, follows WHERE influencer = true AS connections)", format!("{}", out));
	}

	#[test]
	fn graph_select() {
		let sql = "->(SELECT amount FROM likes WHERE amount > 10)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("->(SELECT amount FROM likes WHERE amount > 10)", format!("{}", out));
	}

	#[test]
	fn graph_select_wildcard() {
		let sql = "->(SELECT * FROM likes WHERE amount > 10)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("->(SELECT * FROM likes WHERE amount > 10)", format!("{}", out));
	}

	#[test]
	fn graph_select_where_order() {
		let sql = "->(SELECT amount FROM likes WHERE amount > 10 ORDER BY amount)";
		let out = syn::expr(sql).unwrap();
		assert_eq!(
			"->(SELECT amount FROM likes WHERE amount > 10 ORDER BY amount\n)",
			format!("{}", out)
		);
	}

	#[test]
	fn graph_select_where_order_limit() {
		let sql = "->(SELECT amount FROM likes WHERE amount > 10 ORDER BY amount LIMIT 1)";
		let out = syn::expr(sql).unwrap();
		assert_eq!(
			"->(SELECT amount FROM likes WHERE amount > 10 ORDER BY amount\n LIMIT 1)",
			format!("{}", out)
		);
	}

	#[test]
	fn graph_select_limit() {
		let sql = "->(SELECT amount FROM likes LIMIT 1)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("->(SELECT amount FROM likes LIMIT 1)", format!("{}", out));
	}

	#[test]
	fn graph_select_order() {
		let sql = "->(SELECT amount FROM likes ORDER BY amount)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("->(SELECT amount FROM likes ORDER BY amount\n)", format!("{}", out));
	}

	#[test]
	fn graph_select_order_limit() {
		let sql = "->(SELECT amount FROM likes ORDER BY amount LIMIT 1)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("->(SELECT amount FROM likes ORDER BY amount\n LIMIT 1)", format!("{}", out));
	}

	/// creates a field part
	fn f(s: &str) -> Part {
		Part::Field(Ident::new(s.to_owned()).unwrap())
	}

	/// creates a field part
	fn b(v: bool) -> Expr {
		Expr::Literal(Literal::Bool(v))
	}

	#[test]
	fn idiom_normal() {
		let sql = "test";
		let out = syn::expr(sql).unwrap();
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, sql::Expr::Idiom(Idiom(vec![f("test")])));
	}

	#[test]
	fn idiom_quoted_backtick() {
		let sql = "`test`";
		let out = syn::expr(sql).unwrap();
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, sql::Expr::Idiom(Idiom(vec![f("test")])));
	}

	#[test]
	fn idiom_quoted_brackets() {
		let sql = "⟨test⟩";
		let out = syn::expr(sql).unwrap();
		assert_eq!("test", format!("{}", out));
		assert_eq!(out, sql::Expr::Idiom(Idiom(vec![f("test")])));
	}

	#[test]
	fn idiom_nested() {
		let sql = "test.temp";
		let out = syn::expr(sql).unwrap();
		assert_eq!("test.temp", format!("{}", out));
		assert_eq!(out, sql::Expr::Idiom(Idiom(vec![f("test"), f("temp")])));
	}

	#[test]
	fn idiom_nested_quoted() {
		let sql = "test.`some key`";
		let out = syn::expr(sql).unwrap();
		assert_eq!("test.`some key`", format!("{}", out));
		assert_eq!(out, sql::Expr::Idiom(Idiom(vec![f("test"), f("some key")])));
	}

	#[test]
	fn idiom_nested_array_all() {
		let sql = "test.temp[*]";
		let out = syn::expr(sql).unwrap();
		assert_eq!("test.temp[*]", format!("{}", out));
		assert_eq!(out, sql::Expr::Idiom(Idiom(vec![f("test"), f("temp"), Part::All])));
	}

	#[test]
	fn idiom_nested_array_last() {
		let sql = "test.temp[$]";
		let out = syn::expr(sql).unwrap();
		assert_eq!("test.temp[$]", format!("{}", out));
		assert_eq!(out, sql::Expr::Idiom(Idiom(vec![f("test"), f("temp"), Part::Last])));
	}

	#[test]
	fn idiom_nested_array_value() {
		let sql = "test.temp[*].text";
		let out = syn::expr(sql).unwrap();
		assert_eq!("test.temp[*].text", format!("{}", out));
		assert_eq!(out, sql::Expr::Idiom(Idiom(vec![f("test"), f("temp"), Part::All, f("text")])));
	}

	#[test]
	fn idiom_nested_array_question() {
		let sql = "test.temp[? test = true].text";
		let out = syn::expr(sql).unwrap();
		assert_eq!("test.temp[WHERE test = true].text", format!("{}", out));
		assert_eq!(
			out,
			sql::Expr::Idiom(Idiom(vec![
				f("test"),
				f("temp"),
				Part::Where(sql::Expr::Binary {
					left: Box::new(sql::Expr::Idiom(Idiom(vec![f("test")]))),
					op: sql::BinaryOperator::Equal,
					right: Box::new(b(true))
				}),
				f("text")
			]))
		);
	}

	#[test]
	fn idiom_nested_array_condition() {
		let sql = "test.temp[WHERE test = true].text";
		let out = syn::expr(sql).unwrap();
		assert_eq!("test.temp[WHERE test = true].text", format!("{}", out));
		assert_eq!(
			out,
			sql::Expr::Idiom(Idiom(vec![
				f("test"),
				f("temp"),
				Part::Where(Expr::Binary {
					left: Box::new(Expr::Idiom(Idiom(vec![f("test")]))),
					op: BinaryOperator::Equal,
					right: Box::new(b(true)),
				}),
				f("text")
			]))
		);
	}

	#[test]
	fn idiom_start_param_local_field() {
		let sql = "$test.temporary[0].embedded…";
		let out = syn::expr(sql).unwrap();
		assert_eq!("$test.temporary[0].embedded…", format!("{}", out));
		assert_eq!(
			out,
			sql::Expr::Idiom(Idiom(vec![
				Part::Start(Expr::Param(Param::new("test".to_owned()).unwrap())),
				f("temporary"),
				Part::Value(Expr::Literal(sql::Literal::Integer(0))),
				f("embedded"),
				Part::Flatten,
			]))
		);
	}

	#[test]
	fn idiom_start_thing_remote_traversal() {
		let sql = "person:test.friend->like->person";
		let out = syn::expr(sql).unwrap();
		assert_eq!("person:test.friend->like->person", format!("{}", out));
		assert_eq!(
			out,
			Expr::Idiom(Idiom(vec![
				Part::Start(Expr::Literal(Literal::RecordId(RecordIdLit {
					table: "person".to_owned(),
					key: RecordIdKeyLit::String(strand!("test").to_owned())
				}))),
				f("friend"),
				Part::Graph(Lookup {
					kind: LookupKind::Graph(Dir::Out),
					what: vec![LookupSubject::Table(Ident::from_strand(
						strand!("like").to_owned()
					))],
					..Default::default()
				}),
				Part::Graph(Lookup {
					kind: LookupKind::Graph(Dir::Out),
					what: vec![LookupSubject::Table(Ident::from_strand(
						strand!("person").to_owned()
					))],
					..Default::default()
				}),
			]))
		);
	}

	#[test]
	fn part_all() {
		let sql = "{}[*]";
		let out = syn::expr(sql).unwrap();
		assert_eq!("{  }[*]", format!("{}", out));
		assert_eq!(
			out,
			Expr::Idiom(Idiom(vec![
				Part::Start(Expr::Literal(Literal::Object(Vec::new()))),
				Part::All
			]))
		);
	}

	#[test]
	fn part_last() {
		let sql = "{}[$]";
		let out = syn::expr(sql).unwrap();
		assert_eq!("{  }[$]", format!("{}", out));
		assert_eq!(
			out,
			Expr::Idiom(Idiom(vec![
				Part::Start(Expr::Literal(Literal::Object(Vec::new()))),
				Part::Last
			]))
		);
	}

	#[test]
	fn part_param() {
		let sql = "{}[$param]";
		let out = syn::expr(sql).unwrap();
		assert_eq!("{  }[$param]", format!("{}", out));
		assert_eq!(
			out,
			Expr::Idiom(Idiom(vec![
				Part::Start(Expr::Literal(Literal::Object(Vec::new()))),
				Part::Value(Expr::Param(Param::from_strand(strand!("param").to_owned())))
			]))
		);
	}

	#[test]
	fn part_flatten() {
		let sql = "{}...";
		let out = syn::expr(sql).unwrap();
		assert_eq!("{  }…", format!("{}", out));
		assert_eq!(
			out,
			Expr::Idiom(Idiom(vec![
				Part::Start(Expr::Literal(Literal::Object(Vec::new()))),
				Part::Flatten
			]))
		);
	}

	#[test]
	fn part_flatten_ellipsis() {
		let sql = "{}…";
		let out = syn::expr(sql).unwrap();
		assert_eq!("{  }…", format!("{}", out));
		assert_eq!(
			out,
			Expr::Idiom(Idiom(vec![
				Part::Start(Expr::Literal(Literal::Object(Vec::new()))),
				Part::Flatten
			]))
		);
	}

	#[test]
	fn part_number() {
		let sql = "{}[0]";
		let out = syn::expr(sql).unwrap();
		assert_eq!("{  }[0]", format!("{}", out));
		assert_eq!(
			out,
			Expr::Idiom(Idiom(vec![
				Part::Start(Expr::Literal(Literal::Object(Vec::new()))),
				Part::Value(Expr::Literal(Literal::Integer(0)))
			]))
		);
	}

	#[test]
	fn part_expression_question() {
		let sql = "{}[?test = true]";
		let out = syn::expr(sql).unwrap();
		assert_eq!("{  }[WHERE test = true]", format!("{}", out));
		assert_eq!(
			out,
			Expr::Idiom(Idiom(vec![
				Part::Start(Expr::Literal(Literal::Object(Vec::new()))),
				Part::Where(Expr::Binary {
					left: Box::new(Expr::Idiom(Idiom(vec![f("test")]))),
					op: BinaryOperator::Equal,
					right: Box::new(b(true)),
				})
			]))
		);
	}

	#[test]
	fn part_expression_condition() {
		let sql = "{}[WHERE test = true]";
		let out = syn::expr(sql).unwrap();
		assert_eq!("{  }[WHERE test = true]", format!("{}", out));
		assert_eq!(
			out,
			Expr::Idiom(Idiom(vec![
				Part::Start(Expr::Literal(Literal::Object(Vec::new()))),
				Part::Where(Expr::Binary {
					left: Box::new(Expr::Idiom(Idiom(vec![f("test")]))),
					op: BinaryOperator::Equal,
					right: Box::new(b(true)),
				})
			]))
		);
	}

	#[test]
	fn idiom_thing_number() {
		let sql = "test:1.foo";
		let out = syn::expr(sql).unwrap();
		assert_eq!(
			out,
			Expr::Idiom(Idiom(vec![
				Part::Start(Expr::Literal(Literal::RecordId(RecordIdLit {
					table: "test".to_owned(),
					key: RecordIdKeyLit::Number(1),
				}))),
				f("foo"),
			]))
		);
	}

	#[test]
	fn idiom_thing_index() {
		let sql = "test:1['foo']";
		let out = syn::expr(sql).unwrap();
		assert_eq!(
			out,
			Expr::Idiom(Idiom(vec![
				Part::Start(Expr::Literal(Literal::RecordId(RecordIdLit {
					table: "test".to_owned(),
					key: RecordIdKeyLit::Number(1),
				}))),
				Part::Value(Expr::Literal(Literal::Strand(strand!("foo").to_owned()))),
			]))
		);
	}

	#[test]
	fn idiom_thing_all() {
		let sql = "test:1.*";
		let out = syn::expr(sql).unwrap();
		assert_eq!(
			out,
			Expr::Idiom(Idiom(vec![
				Part::Start(Expr::Literal(Literal::RecordId(RecordIdLit {
					table: "test".to_owned(),
					key: RecordIdKeyLit::Number(1),
				}))),
				Part::All
			]))
		);
	}
}
