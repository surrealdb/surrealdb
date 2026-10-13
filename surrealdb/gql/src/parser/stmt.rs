//! Statement level parsing: the linear query statement, `MATCH` clauses and
//! the `RETURN` clause with its trailing `ORDER BY`/`OFFSET`/`LIMIT` page
//! statement.
//!
//! From `ambientLinearQueryStatement` (GQL.g4:554), `matchStatement` (14.4,
//! GQL.g4:578-599), `primitiveResultStatement`/`returnStatement` (14.10-14.11,
//! GQL.g4:660-685) and `orderByAndPageStatement` (14.9, GQL.g4:652). All
//! constructs which share grammar real estate with this subset but are not
//! supported are recognised and rejected with precise spans.

use reblessive::Stk;

use crate::gql::ast::{
	DetachMode, GqlExpr, GqlGroupItem, GqlLiteral, GqlStep, LinearQuery, MatchClause, MatchItem,
	MutationStatement, OptionalBlock, OrderItem, ReturnClause, ReturnItem, ReturnItems,
	SetQuantifier,
};
use crate::gql::parser::mac::{enter_object_recursion, expected, unexpected};
use crate::gql::parser::{ParseResult, Parser};
use crate::gql::token::{NumberKind, Span, TokenKind, t};
use crate::syn::error::bail;

impl Parser<'_> {
	/// Parse a top level linear query: a sequence of `MATCH`/`OPTIONAL` reads and
	/// `INSERT`/`SET`/`REMOVE`/`DELETE` mutations — in any textual order — and an
	/// optional trailing `RETURN` clause.
	pub(super) async fn parse_statement(&mut self, stk: &mut Stk) -> ParseResult<LinearQuery> {
		let start = self.peek().span;

		// Steps in textual order: reads and mutations may interleave freely.
		let mut steps = Vec::new();
		loop {
			let token = self.peek();
			match token.kind {
				t!("MATCH") => {
					self.pop_peek();
					let clause = self.parse_match_clause(stk, token.span).await?;
					steps.push(GqlStep::Read(MatchItem::Match(clause)));
				}
				t!("OPTIONAL") => {
					self.pop_peek();
					let block = stk.run(|stk| self.parse_optional_operand(stk, token.span)).await?;
					steps.push(GqlStep::Read(MatchItem::Optional(block)));
				}
				t!("INSERT") => {
					self.pop_peek();
					let stmt = self.parse_insert_statement(stk, token.span).await?;
					steps.push(GqlStep::Mutate(MutationStatement::Insert(stmt)));
				}
				t!("SET") => {
					self.pop_peek();
					let stmt = self.parse_set_statement(stk, token.span).await?;
					steps.push(GqlStep::Mutate(MutationStatement::Set(stmt)));
				}
				t!("REMOVE") => {
					self.pop_peek();
					let stmt = self.parse_remove_statement(token.span)?;
					steps.push(GqlStep::Mutate(MutationStatement::Remove(stmt)));
				}
				t!("DELETE") => {
					self.pop_peek();
					let stmt =
						self.parse_delete_statement(stk, DetachMode::NoDetach, token.span).await?;
					steps.push(GqlStep::Mutate(MutationStatement::Delete(stmt)));
				}
				t!("DETACH") | t!("NODETACH") => {
					let detach = if matches!(token.kind, t!("DETACH")) {
						DetachMode::Detach
					} else {
						DetachMode::NoDetach
					};
					self.pop_peek();
					expected!(self, t!("DELETE"));
					let stmt = self.parse_delete_statement(stk, detach, token.span).await?;
					steps.push(GqlStep::Mutate(MutationStatement::Delete(stmt)));
				}
				_ => break,
			}
		}

		// --- Optional trailing RETURN, or a precise rejection. ---
		let token = self.peek();
		let ret = match token.kind {
			t!("RETURN") => Some(self.parse_return_clause(stk).await?),
			_ if token.is_eof() => None,
			t!("FINISH") => {
				bail!(
					"FINISH statements are not supported yet",
					@token.span => "queries must end with a RETURN clause"
				);
			}
			t!("LET") | t!("FOR") | t!("FILTER") | t!("USE") | t!("SELECT") | t!("CALL") => {
				bail!(
					"`{}` statements are not supported yet",
					token.kind,
					@token.span
				);
			}
			t!("ORDER") | t!("OFFSET") | t!("SKIP") | t!("LIMIT") => {
				bail!(
					"Standalone `{}` statements are not supported yet",
					token.kind,
					@token.span => "ORDER BY, OFFSET/SKIP and LIMIT may only follow the RETURN clause"
				);
			}
			t!("CREATE") | t!("DROP") => {
				bail!(
					"`{}` statements are not supported yet",
					token.kind,
					@token.span
				);
			}
			_ => {
				unexpected!(self, token, "a MATCH, INSERT, SET, REMOVE, DELETE or RETURN statement")
			}
		};
		self.check_trailing_clauses()?;
		Ok(LinearQuery {
			steps,
			ret,
			span: start.covers(self.last_span()),
		})
	}

	/// Parse an `OPTIONAL` operand (`optionalOperand`, GQL.g4:591), all three
	/// grammar forms. The `OPTIONAL` keyword must already be consumed; `start`
	/// is its span.
	///
	/// - plain `OPTIONAL MATCH <pattern…>` ⇒ a block of exactly one item;
	/// - block `OPTIONAL { matchStatement+ }`;
	/// - paren `OPTIONAL ( matchStatement+ )`.
	///
	/// The brace/paren forms hold a `matchStatementBlock` whose statements may
	/// themselves be further `OPTIONAL`s; each block delimiter charges the
	/// object-recursion budget (mirroring nested label/expr parens) so that
	/// `OPTIONAL { OPTIONAL { … } }` cannot overflow the machine stack.
	async fn parse_optional_operand(
		&mut self,
		stk: &mut Stk,
		start: Span,
	) -> ParseResult<OptionalBlock> {
		let token = self.peek();
		match token.kind {
			t!("MATCH") => {
				self.pop_peek();
				let clause = self.parse_match_clause(stk, token.span).await?;
				Ok(OptionalBlock {
					span: start.covers(self.last_span()),
					items: vec![MatchItem::Match(clause)],
				})
			}
			// The `{`/`(` block forms (GQL.g4:593-594). `expect_closing_delimiter`
			// pairs the close with the recorded open span on a mismatch.
			t!("{") => {
				enter_object_recursion!(this = self => {
					let open = this.pop_peek().span;
					let items = stk.run(|stk| this.parse_match_statement_block(stk)).await?;
					this.expect_closing_delimiter(t!("}"), open)?;
					Ok(OptionalBlock {
						span: start.covers(this.last_span()),
						items,
					})
				})
			}
			t!("(") => {
				enter_object_recursion!(this = self => {
					let open = this.pop_peek().span;
					let items = stk.run(|stk| this.parse_match_statement_block(stk)).await?;
					this.expect_closing_delimiter(t!(")"), open)?;
					Ok(OptionalBlock {
						span: start.covers(this.last_span()),
						items,
					})
				})
			}
			_ => unexpected!(self, token, "`MATCH`, `{` or `(`"),
		}
	}

	/// Parse a `matchStatementBlock`: one or more `matchStatement`s
	/// (GQL.g4:597), each a plain `MATCH` clause or a nested `OPTIONAL`
	/// operand. At least one statement is required.
	async fn parse_match_statement_block(&mut self, stk: &mut Stk) -> ParseResult<Vec<MatchItem>> {
		let mut items = Vec::new();
		loop {
			let token = self.peek();
			match token.kind {
				t!("MATCH") => {
					self.pop_peek();
					let clause = self.parse_match_clause(stk, token.span).await?;
					items.push(MatchItem::Match(clause));
				}
				t!("OPTIONAL") => {
					self.pop_peek();
					let block = stk.run(|stk| self.parse_optional_operand(stk, token.span)).await?;
					items.push(MatchItem::Optional(block));
				}
				_ if items.is_empty() => unexpected!(self, token, "`MATCH` or `OPTIONAL`"),
				_ => break,
			}
		}
		Ok(items)
	}

	/// Parse a `MATCH` clause graph pattern: `matchMode? pathPatternList
	/// keepClause? graphPatternWhereClause?` (GQL.g4:803). The `MATCH` keyword
	/// must already be consumed; `start` is its span.
	async fn parse_match_clause(&mut self, stk: &mut Stk, start: Span) -> ParseResult<MatchClause> {
		// `REPEATABLE`/`DIFFERENT` are non-reserved words, so they are only a
		// match mode when followed by their element/edge synonym; otherwise
		// they can begin a path pattern as a path variable.
		let token = self.peek();
		if let t!("REPEATABLE") = token.kind
			&& matches!(self.peek1().kind, t!("ELEMENT") | t!("ELEMENTS"))
		{
			bail!(
				"Match modes (`REPEATABLE ELEMENTS`) are not supported yet",
				@token.span.covers(self.peek1().span)
			);
		}
		if let t!("DIFFERENT") = token.kind
			&& matches!(
				self.peek1().kind,
				t!("EDGE") | t!("EDGES") | t!("RELATIONSHIP") | t!("RELATIONSHIPS")
			) {
			bail!(
				"Match modes (`DIFFERENT EDGES`) are not supported yet",
				@token.span.covers(self.peek1().span)
			);
		}

		let mut patterns = Vec::new();
		loop {
			let pattern = self.parse_path_pattern(stk).await?;
			patterns.push(pattern);
			let token = self.peek();
			match token.kind {
				t!(",") => {
					self.pop_peek();
				}
				// `pathTerm |+| pathTerm` and `pathTerm | pathTerm`
				// alternations (GQL.g4:966-970).
				t!("|+|") => {
					bail!(
						"Multiset alternation (`|+|`) between path patterns is not supported yet",
						@token.span
					);
				}
				t!("|") => {
					bail!(
						"Pattern unions (`|`) between path patterns are not supported yet",
						@token.span
					);
				}
				_ => break,
			}
		}

		let token = self.peek();
		if let t!("KEEP") = token.kind {
			bail!("KEEP clauses are not supported yet", @token.span);
		}

		let where_clause = if self.eat(t!("WHERE")) {
			Some(stk.run(|stk| self.parse_expr(stk)).await?)
		} else {
			None
		};

		// `graphPatternYieldClause` (GQL.g4:597).
		let token = self.peek();
		if let t!("YIELD") = token.kind {
			bail!("YIELD clauses are not supported yet", @token.span);
		}

		Ok(MatchClause {
			patterns,
			where_clause,
			span: start.covers(self.last_span()),
		})
	}

	/// Parse the `RETURN` clause and its optional trailing `ORDER BY`,
	/// `OFFSET`/`SKIP` and `LIMIT` clauses, which must appear in that fixed
	/// order.
	async fn parse_return_clause(&mut self, stk: &mut Stk) -> ParseResult<ReturnClause> {
		let start = expected!(self, t!("RETURN")).span;

		let quantifier = if self.eat(t!("DISTINCT")) {
			Some(SetQuantifier::Distinct)
		} else if self.eat(t!("ALL")) {
			Some(SetQuantifier::All)
		} else {
			None
		};

		let items = if self.eat(t!("*")) {
			ReturnItems::Star
		} else {
			let mut items = Vec::new();
			loop {
				items.push(self.parse_return_item(stk).await?);
				if !self.eat(t!(",")) {
					break;
				}
			}
			ReturnItems::Items(items)
		};

		// An attached `GROUP BY` (`groupByClause`, GQL.g4:1313) sits between the
		// return items and the trailing `ORDER BY`/`OFFSET`/`LIMIT`.
		let group_by = self.parse_group_by_clause(stk).await?;

		let mut order_by = Vec::new();
		if self.eat(t!("ORDER")) {
			expected!(self, t!("BY"));
			loop {
				order_by.push(self.parse_order_item(stk).await?);
				if !self.eat(t!(",")) {
					break;
				}
			}
		}

		// `OFFSET` and `SKIP` are synonyms (`offsetSynonym`, GQL.g4:1374).
		let skip = if self.eat(t!("OFFSET")) || self.eat(t!("SKIP")) {
			Some(self.parse_count_specification()?)
		} else {
			None
		};

		let limit = if self.eat(t!("LIMIT")) {
			Some(self.parse_count_specification()?)
		} else {
			None
		};

		Ok(ReturnClause {
			quantifier,
			items,
			group_by,
			order_by,
			skip,
			limit,
			span: start.covers(self.last_span()),
		})
	}

	/// Parse an optional `GROUP BY` clause (`groupByClause`, GQL.g4:1313): the
	/// `GROUP BY` keywords followed by a comma-separated list of grouping
	/// elements. Each element is a full value expression (matching the return
	/// items and `ORDER BY` keys); the lowering enforces the GQL shape.
	async fn parse_group_by_clause(&mut self, stk: &mut Stk) -> ParseResult<Vec<GqlGroupItem>> {
		if !self.eat(t!("GROUP")) {
			return Ok(Vec::new());
		}
		expected!(self, t!("BY"));
		let mut items = Vec::new();
		loop {
			let start = self.peek().span;
			let expr = stk.run(|stk| self.parse_expr(stk)).await?;
			items.push(GqlGroupItem {
				expr,
				span: start.covers(self.last_span()),
			});
			if !self.eat(t!(",")) {
				break;
			}
		}
		Ok(items)
	}

	/// Parse a single `RETURN` item: an expression with an optional `AS`
	/// alias, capturing the verbatim source text of the expression for use as
	/// the default column name.
	async fn parse_return_item(&mut self, stk: &mut Stk) -> ParseResult<ReturnItem> {
		let start = self.peek().span;
		let expr = stk.run(|stk| self.parse_expr(stk)).await?;
		let text_span = start.covers(self.last_span());
		let text = self.span_str(text_span).to_owned();
		let alias = if self.eat(t!("AS")) {
			Some(self.parse_ident()?)
		} else {
			None
		};
		Ok(ReturnItem {
			expr,
			alias,
			text,
		})
	}

	/// Parse a single `ORDER BY` sort specification: `sortKey
	/// orderingSpecification? nullOrdering?` (GQL.g4:1341), where the sort
	/// key is a full value expression.
	async fn parse_order_item(&mut self, stk: &mut Stk) -> ParseResult<OrderItem> {
		let start = self.peek().span;
		let expr = stk.run(|stk| self.parse_expr(stk)).await?;
		let ascending = if self.eat(t!("ASC")) || self.eat(t!("ASCENDING")) {
			Some(true)
		} else if self.eat(t!("DESC")) || self.eat(t!("DESCENDING")) {
			Some(false)
		} else {
			None
		};
		let nulls_first = if self.eat(t!("NULLS")) {
			let token = self.next();
			match token.kind {
				t!("FIRST") => Some(true),
				t!("LAST") => Some(false),
				_ => unexpected!(self, token, "`FIRST` or `LAST`"),
			}
		} else {
			None
		};
		Ok(OrderItem {
			expr,
			ascending,
			nulls_first,
			span: start.covers(self.last_span()),
		})
	}

	/// Parse a `LIMIT`/`OFFSET` count: an unsigned integer or a parameter
	/// (`nonNegativeIntegerSpecification`, GQL.g4:2268).
	fn parse_count_specification(&mut self) -> ParseResult<GqlExpr> {
		let token = self.next();
		match token.kind {
			TokenKind::Number {
				kind:
					kind @ (NumberKind::Integer
					| NumberKind::Hex
					| NumberKind::Octal
					| NumberKind::Binary),
				suffix: None,
			} => {
				let value = self.parse_integer_token(token, kind)?;
				Ok(GqlExpr::Literal(GqlLiteral::Integer(value), token.span))
			}
			TokenKind::Parameter => {
				let name = self.parse_parameter_name(token)?;
				Ok(GqlExpr::Param {
					name,
					span: token.span,
				})
			}
			TokenKind::SubstitutedParameter => {
				bail!(
					"Substituted parameters (`$$name`) are not supported yet",
					@token.span => "use a general `$name` parameter"
				);
			}
			_ => unexpected!(self, token, "an unsigned integer or a parameter"),
		}
	}

	/// Rejects out-of-order, duplicated or composite trailing clauses with a
	/// targeted error before the generic end-of-query check runs.
	fn check_trailing_clauses(&mut self) -> ParseResult<()> {
		let token = self.peek();
		match token.kind {
			t!("ORDER") | t!("OFFSET") | t!("SKIP") | t!("LIMIT") => {
				bail!(
					"Unexpected `{}` clause",
					token.kind,
					@token.span => "ORDER BY, OFFSET/SKIP and LIMIT may appear at most once each, in that order"
				);
			}
			t!("UNION") | t!("EXCEPT") | t!("INTERSECT") | t!("OTHERWISE") => {
				bail!(
					"Composite queries (`UNION`, `EXCEPT`, `INTERSECT`, `OTHERWISE`) are not supported yet",
					@token.span
				);
			}
			t!("GROUP") => {
				bail!(
					"Unexpected `GROUP` clause",
					@token.span => "GROUP BY must directly follow the RETURN items, before ORDER BY"
				);
			}
			_ => Ok(()),
		}
	}
}
