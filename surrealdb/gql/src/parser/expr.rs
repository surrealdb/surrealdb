//! Value expression parsing.
//!
//! Implements the precedence table of `doc/gql/REFERENCE.md` section (e),
//! derived from the consolidated left-recursive `valueExpression`
//! (GQL.g4:2137-2163), `valueExpressionPrimary` (GQL.g4:2220) and `predicate`
//! (GQL.g4:2008). Lowest to highest: `OR`/`XOR`; `AND`; `IS [NOT]
//! TRUE|FALSE|UNKNOWN`; `NOT`; comparisons (non-chaining); `||`; `+`/`-`;
//! `*`/`/`; unary `+`/`-`; primary.
//!
//! `IS [NOT] NULL` is not a precedence level: it is a `nullPredicate`
//! (GQL.g4:2042) whose operand must be a *primary*, so it is parsed as a
//! primary postfix; `a.x + 1 IS NULL` is a syntax error which must be written
//! `(a.x + 1) IS NULL`.

use reblessive::Stk;

use crate::gql::ast::{BinaryOp, GqlExpr, GqlLiteral, Ident, SetQuantifier, TruthValue, UnaryOp};
use crate::gql::lexer::Lexer;
use crate::gql::parser::mac::{enter_object_recursion, unexpected};
use crate::gql::parser::{ParseResult, Parser};
use crate::gql::token::{Keyword, NumberKind, NumberSuffix, Span, Token, TokenKind, t};
use crate::syn::error::{bail, syntax_error};

impl Parser<'_> {
	/// Charges one level of the expression-depth budget, returning a syntax
	/// error when it is exhausted.
	///
	/// This guards the otherwise-unbounded depth of the [`GqlExpr`] tree —
	/// and of the `sql::Expr` tree it lowers to — built by the operator
	/// cascade below; see [`GqlParserSettings::expr_recursion_limit`]
	/// (mirroring `syn`) for why an unbounded operator chain is a
	/// denial-of-service vector. Property-access chains are deliberately not
	/// charged: they lower to flat `sql::Idiom` part vectors, and the
	/// `GqlExpr` chain itself is dropped iteratively.
	fn enter_expr_depth(&mut self) -> ParseResult<()> {
		if self.settings.expr_recursion_limit == 0 {
			bail!("Exceeded expression recursion depth limit",
				@self.last_span() => "this expression nests or chains operators too deeply");
		}
		self.settings.expr_recursion_limit -= 1;
		Ok(())
	}

	/// Parse a full value expression. The entry point for every
	/// `searchCondition` and `valueExpression` position.
	///
	/// This is a thin wrapper around [`Self::parse_expr_inner`] that charges
	/// the expression-depth budget for the level introduced by this call (so
	/// nested expressions, which recurse through here, are bounded) and
	/// restores it on return so that sibling expressions are not charged for
	/// one another. The operator loops below charge the budget again for
	/// every operator they append to a left-associative spine or prefix
	/// chain.
	pub(super) async fn parse_expr(&mut self, stk: &mut Stk) -> ParseResult<GqlExpr> {
		let restore_to = self.settings.expr_recursion_limit;
		self.enter_expr_depth()?;
		let res = self.parse_expr_inner(stk).await;
		// Restore everything this call consumed, including the levels charged
		// by the operator loops; `restore_to` is exactly this call's entry
		// value.
		self.settings.expr_recursion_limit = restore_to;
		res
	}

	async fn parse_expr_inner(&mut self, stk: &mut Stk) -> ParseResult<GqlExpr> {
		// Level 1: `OR` and `XOR`, same level, left associative.
		let mut left = self.parse_and_expr(stk).await?;
		loop {
			let op = match self.peek_kind() {
				t!("OR") => BinaryOp::Or,
				t!("XOR") => BinaryOp::Xor,
				_ => break,
			};
			self.pop_peek();
			self.enter_expr_depth()?;
			let right = self.parse_and_expr(stk).await?;
			let span = left.span().covers(right.span());
			left = GqlExpr::Binary {
				left: Box::new(left),
				op,
				right: Box::new(right),
				span,
			};
		}
		Ok(left)
	}

	/// Level 2: `AND`, left associative.
	async fn parse_and_expr(&mut self, stk: &mut Stk) -> ParseResult<GqlExpr> {
		let mut left = self.parse_is_expr(stk).await?;
		while self.eat(t!("AND")) {
			self.enter_expr_depth()?;
			let right = self.parse_is_expr(stk).await?;
			let span = left.span().covers(right.span());
			left = GqlExpr::Binary {
				left: Box::new(left),
				op: BinaryOp::And,
				right: Box::new(right),
				span,
			};
		}
		Ok(left)
	}

	/// Level 3: the postfix `IS [NOT] TRUE|FALSE|UNKNOWN` boolean test
	/// (`#isNotExprAlt`). All other `IS …` predicates which can syntactically
	/// occur here are rejected with targeted errors.
	async fn parse_is_expr(&mut self, stk: &mut Stk) -> ParseResult<GqlExpr> {
		let mut expr = self.parse_not_expr(stk).await?;
		while self.peek_kind() == t!("IS") {
			self.enter_expr_depth()?;
			let is_token = self.pop_peek();
			let negated = self.eat(t!("NOT"));
			let token = self.next();
			let value = match token.kind {
				t!("TRUE") => TruthValue::True,
				t!("FALSE") => TruthValue::False,
				t!("UNKNOWN") => TruthValue::Unknown,
				// A primary consumes its own `IS [NOT] NULL` postfix, so
				// reaching a null test here means the operand was a larger
				// expression, which the grammar does not allow.
				t!("NULL") => {
					bail!(
						"`IS NULL` may only directly follow a simple expression",
						@is_token.span.covers(token.span),
						@expr.span() => "wrap this expression in parentheses: `(…) IS NULL`"
					);
				}
				t!("TYPED") => {
					bail!(
						"`IS [NOT] TYPED` type predicates are not supported yet",
						@is_token.span.covers(token.span)
					);
				}
				t!("NORMALIZED") | t!("NFC") | t!("NFD") | t!("NFKC") | t!("NFKD") => {
					bail!(
						"`IS [NOT] NORMALIZED` predicates are not supported yet",
						@is_token.span.covers(token.span)
					);
				}
				t!("LABELED") => {
					bail!(
						"`IS [NOT] LABELED` predicates are not supported yet",
						@is_token.span.covers(token.span)
					);
				}
				t!("DIRECTED") => {
					bail!(
						"`IS [NOT] DIRECTED` predicates are not supported yet",
						@is_token.span.covers(token.span)
					);
				}
				t!("SOURCE") | t!("DESTINATION") => {
					bail!(
						"`IS [NOT] SOURCE/DESTINATION OF` predicates are not supported yet",
						@is_token.span.covers(token.span)
					);
				}
				_ => unexpected!(self, token, "`TRUE`, `FALSE`, `UNKNOWN` or `NULL`"),
			};
			let span = expr.span().covers(token.span);
			expr = GqlExpr::IsBool {
				expr: Box::new(expr),
				value,
				negated,
				span,
			};
		}
		Ok(expr)
	}

	/// Level 4: the prefix `NOT` operator.
	async fn parse_not_expr(&mut self, stk: &mut Stk) -> ParseResult<GqlExpr> {
		let token = self.peek();
		if token.kind == t!("NOT") {
			self.pop_peek();
			self.enter_expr_depth()?;
			let expr = stk.run(|stk| self.parse_not_expr(stk)).await?;
			let span = token.span.covers(expr.span());
			return Ok(GqlExpr::Unary {
				op: UnaryOp::Not,
				expr: Box::new(expr),
				span,
			});
		}
		self.parse_comparison_expr(stk).await
	}

	/// Level 6: the comparison operators `=` `<>` `<` `>` `<=` `>=`.
	///
	/// The grammar parses chained comparisons left associatively
	/// (`a = b = c` as `(a = b) = c`) but chaining has no useful semantics,
	/// so a chained comparison is rejected with a targeted error.
	async fn parse_comparison_expr(&mut self, stk: &mut Stk) -> ParseResult<GqlExpr> {
		let left = self.parse_concat_expr(stk).await?;
		self.check_rejected_operator()?;
		let Some(op) = self.peek_comparison_op() else {
			return Ok(left);
		};
		self.pop_peek();
		let start = self.peek().span;
		let right = self.parse_concat_expr(stk).await?;
		Self::check_operand_null_test(&right, start)?;
		if self.peek_comparison_op().is_some() {
			bail!(
				"Comparison operators cannot be chained; use AND to combine comparisons",
				@self.peek().span
			);
		}
		self.check_rejected_operator()?;
		let span = left.span().covers(right.span());
		Ok(GqlExpr::Binary {
			left: Box::new(left),
			op,
			right: Box::new(right),
			span,
		})
	}

	/// Rejects an unparenthesized `IS [NOT] NULL` test used directly as the
	/// operand of an operator.
	///
	/// The null predicate operand must be a *primary* (19.5), so a null test
	/// cannot itself bind as the right operand of `=`, `||`, `+`, `*`, … —
	/// `a.x + 1 IS NULL` is invalid and must be written `(a.x + 1) IS NULL`.
	/// A parenthesized test like `a + (b IS NULL)` is recognisable by its
	/// span starting after the `(` and is left for lowering to type-check.
	fn check_operand_null_test(operand: &GqlExpr, start: Span) -> ParseResult<()> {
		if let GqlExpr::IsNull {
			span,
			..
		} = operand
			&& span.offset == start.offset
		{
			bail!(
				"`IS NULL` may only directly follow a simple expression",
				@*span => "wrap the left-hand expression in parentheses: `(…) IS NULL`"
			);
		}
		Ok(())
	}

	/// Returns the comparison operator the next token corresponds to, if
	/// any, without consuming it.
	fn peek_comparison_op(&mut self) -> Option<BinaryOp> {
		match self.peek_kind() {
			t!("=") => Some(BinaryOp::Eq),
			t!("<>") => Some(BinaryOp::Neq),
			t!("<") => Some(BinaryOp::Lt),
			t!("<=") => Some(BinaryOp::Lte),
			t!(">") => Some(BinaryOp::Gt),
			t!(">=") => Some(BinaryOp::Gte),
			_ => None,
		}
	}

	/// Rejects operators which do not exist in GQL but are common in other
	/// query languages, with targeted errors: `!=`, `IN`, `LIKE`,
	/// `STARTS WITH`, `ENDS WITH` and `CONTAINS`.
	fn check_rejected_operator(&mut self) -> ParseResult<()> {
		let token = self.peek();
		match token.kind {
			t!("!") if self.peek1().kind == t!("=") => {
				bail!(
					"GQL uses `<>` for inequality",
					@token.span.covers(self.peek1().span) => "replace `!=` with `<>`"
				);
			}
			// `IN` is reserved but only used by `FOR` and `LET … IN … END`;
			// there is no membership predicate.
			t!("IN") => {
				bail!(
					"GQL has no `IN` membership operator",
					@token.span => "compare against the values individually, combined with OR"
				);
			}
			// `LIKE` is reserved but only used in DDL graph type clauses.
			t!("LIKE") => {
				bail!("GQL has no `LIKE` operator", @token.span);
			}
			TokenKind::Identifier => {
				let text = self.span_str(token.span);
				if text.eq_ignore_ascii_case("STARTS") && self.peek1().kind == t!("WITH") {
					bail!(
						"GQL has no `STARTS WITH` operator",
						@token.span.covers(self.peek1().span)
					);
				}
				if text.eq_ignore_ascii_case("ENDS") && self.peek1().kind == t!("WITH") {
					bail!(
						"GQL has no `ENDS WITH` operator",
						@token.span.covers(self.peek1().span)
					);
				}
				if text.eq_ignore_ascii_case("CONTAINS") {
					bail!("GQL has no `CONTAINS` operator", @token.span);
				}
				Ok(())
			}
			_ => Ok(()),
		}
	}

	/// Level 7: the `||` concatenation operator, left associative.
	async fn parse_concat_expr(&mut self, stk: &mut Stk) -> ParseResult<GqlExpr> {
		let mut left = self.parse_additive_expr(stk).await?;
		while self.eat(t!("||")) {
			self.enter_expr_depth()?;
			let start = self.peek().span;
			let right = self.parse_additive_expr(stk).await?;
			Self::check_operand_null_test(&right, start)?;
			let span = left.span().covers(right.span());
			left = GqlExpr::Binary {
				left: Box::new(left),
				op: BinaryOp::Concat,
				right: Box::new(right),
				span,
			};
		}
		Ok(left)
	}

	/// Level 8: binary `+` and `-`, left associative.
	async fn parse_additive_expr(&mut self, stk: &mut Stk) -> ParseResult<GqlExpr> {
		let mut left = self.parse_multiplicative_expr(stk).await?;
		loop {
			let op = match self.peek_kind() {
				t!("+") => BinaryOp::Add,
				t!("-") => BinaryOp::Sub,
				_ => break,
			};
			self.pop_peek();
			self.enter_expr_depth()?;
			let start = self.peek().span;
			let right = self.parse_multiplicative_expr(stk).await?;
			Self::check_operand_null_test(&right, start)?;
			let span = left.span().covers(right.span());
			left = GqlExpr::Binary {
				left: Box::new(left),
				op,
				right: Box::new(right),
				span,
			};
		}
		Ok(left)
	}

	/// Level 9: binary `*` and `/`, left associative.
	async fn parse_multiplicative_expr(&mut self, stk: &mut Stk) -> ParseResult<GqlExpr> {
		let mut left = self.parse_unary_expr(stk).await?;
		loop {
			let op = match self.peek_kind() {
				t!("*") => BinaryOp::Mul,
				t!("/") => BinaryOp::Div,
				_ => break,
			};
			self.pop_peek();
			self.enter_expr_depth()?;
			let start = self.peek().span;
			let right = self.parse_unary_expr(stk).await?;
			Self::check_operand_null_test(&right, start)?;
			let span = left.span().covers(right.span());
			left = GqlExpr::Binary {
				left: Box::new(left),
				op,
				right: Box::new(right),
				span,
			};
		}
		Ok(left)
	}

	/// Level 10: the unary `+` and `-` sign operators.
	async fn parse_unary_expr(&mut self, stk: &mut Stk) -> ParseResult<GqlExpr> {
		let token = self.peek();
		let op = match token.kind {
			t!("+") => UnaryOp::Plus,
			t!("-") => UnaryOp::Neg,
			_ => return self.parse_primary_expr(stk).await,
		};
		self.pop_peek();
		self.enter_expr_depth()?;
		let start = self.peek().span;
		let expr = stk.run(|stk| self.parse_unary_expr(stk)).await?;
		Self::check_operand_null_test(&expr, start)?;
		let span = token.span.covers(expr.span());
		Ok(GqlExpr::Unary {
			op,
			expr: Box::new(expr),
			span,
		})
	}

	/// Level 11: a value expression primary with its postfixes: property
	/// access chains and the `IS [NOT] NULL` null test.
	async fn parse_primary_expr(&mut self, stk: &mut Stk) -> ParseResult<GqlExpr> {
		enter_object_recursion!(this = self => {
			let expr = this.parse_primary_atom(stk).await?;
			this.parse_primary_postfix(expr)
		})
	}

	/// Parse the postfixes of a primary: `.name` property references
	/// (`valueExpressionPrimary PERIOD propertyName`, 20.11) and the
	/// `IS [NOT] NULL` null predicate (19.5), which only applies to a
	/// primary.
	fn parse_primary_postfix(&mut self, mut expr: GqlExpr) -> ParseResult<GqlExpr> {
		while self.eat(t!(".")) {
			// Property names are identifiers: non-reserved keywords and
			// `"…"` delimited identifiers are valid.
			let name = self.parse_ident()?;
			let span = expr.span().covers(name.span);
			expr = GqlExpr::Property(Box::new(expr), name, span);
		}
		// `IS [NOT] NULL` directly after a primary. Other `IS` forms bind at
		// the boolean test level and are left for it.
		if self.peek_kind() == t!("IS") {
			let negated = match (self.peek1().kind, self.peek2().kind) {
				(t!("NULL"), _) => {
					self.pop_peek();
					self.pop_peek();
					Some(false)
				}
				(t!("NOT"), t!("NULL")) => {
					self.pop_peek();
					self.pop_peek();
					self.pop_peek();
					Some(true)
				}
				_ => None,
			};
			if let Some(negated) = negated {
				let span = expr.span().covers(self.last_span());
				expr = GqlExpr::IsNull {
					expr: Box::new(expr),
					negated,
					span,
				};
			}
		}
		Ok(expr)
	}

	/// Parse a single primary atom: a literal, parameter, variable, function
	/// call, list or record literal, or a parenthesized expression.
	async fn parse_primary_atom(&mut self, stk: &mut Stk) -> ParseResult<GqlExpr> {
		let token = self.peek();
		match token.kind {
			t!("(") => {
				let open = self.pop_peek().span;
				let expr = stk.run(|stk| self.parse_expr(stk)).await?;
				self.expect_closing_delimiter(t!(")"), open)?;
				Ok(expr)
			}
			t!("[") => {
				let open = self.pop_peek().span;
				let mut items = Vec::new();
				if self.peek_kind() != t!("]") {
					loop {
						items.push(stk.run(|stk| self.parse_expr(stk)).await?);
						if !self.eat(t!(",")) {
							break;
						}
					}
				}
				self.expect_closing_delimiter(t!("]"), open)?;
				Ok(GqlExpr::List(items, open.covers(self.last_span())))
			}
			t!("{") => {
				let open = self.pop_peek().span;
				let mut fields = Vec::new();
				if self.peek_kind() != t!("}") {
					loop {
						// Field names are identifier positions: a `"…"`
						// token is a delimited identifier here.
						let key = self.parse_ident()?;
						let colon = self.peek();
						if colon.kind != t!(":") {
							unexpected!(self, colon, "`:`");
						}
						self.pop_peek();
						let value = stk.run(|stk| self.parse_expr(stk)).await?;
						fields.push((key, value));
						if !self.eat(t!(",")) {
							break;
						}
					}
				}
				self.expect_closing_delimiter(t!("}"), open)?;
				Ok(GqlExpr::Map(fields, open.covers(self.last_span())))
			}
			TokenKind::Parameter => {
				self.pop_peek();
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
			TokenKind::Number {
				kind,
				suffix,
			} => {
				self.pop_peek();
				let literal = self.parse_number_literal(token, kind, suffix)?;
				Ok(GqlExpr::Literal(literal, token.span))
			}
			// In expression position a single- or double-quoted token is a
			// character string literal; in identifier positions (variable
			// declarations, label names, property keys, aliases) the
			// double-quoted token is a delimited identifier instead. See
			// `Parser::parse_ident`.
			TokenKind::SingleQuoted {
				..
			}
			| TokenKind::DoubleQuoted {
				..
			} => {
				self.pop_peek();
				let value = Lexer::unescape_quoted_span(self.span_str(token.span), token.span)?;
				Ok(GqlExpr::Literal(GqlLiteral::String(value), token.span))
			}
			// An accent-quoted token is always a delimited identifier: in
			// expression position it is a variable reference.
			TokenKind::AccentQuoted {
				..
			} => {
				self.pop_peek();
				let name = Lexer::unescape_quoted_span(self.span_str(token.span), token.span)?;
				Ok(GqlExpr::Variable(Ident {
					name,
					span: token.span,
				}))
			}
			t!("TRUE") => {
				self.pop_peek();
				Ok(GqlExpr::Literal(GqlLiteral::Bool(true), token.span))
			}
			t!("FALSE") => {
				self.pop_peek();
				Ok(GqlExpr::Literal(GqlLiteral::Bool(false), token.span))
			}
			// The UNKNOWN boolean literal is the null value of the boolean
			// type (ISO 39075 three-valued logic), so it parses as the null
			// literal.
			t!("UNKNOWN") | t!("NULL") => {
				self.pop_peek();
				Ok(GqlExpr::Literal(GqlLiteral::Null, token.span))
			}
			t!("EXISTS") => {
				bail!("`EXISTS` predicates are not supported yet", @token.span);
			}
			t!("CASE") => {
				bail!("`CASE` expressions are not supported yet", @token.span);
			}
			t!("CAST") => {
				bail!("`CAST` expressions are not supported yet", @token.span);
			}
			TokenKind::Identifier => {
				if self.peek1().kind == t!("(") {
					return self.parse_function_call(stk).await;
				}
				self.pop_peek();
				Ok(GqlExpr::Variable(Ident {
					name: self.span_str(token.span).to_owned(),
					span: token.span,
				}))
			}
			TokenKind::Keyword(keyword) => {
				// Any keyword directly followed by `(` is accepted as a
				// function name (`ABS`, `COUNT`, `UPPER`, …, which are all
				// reserved words); lowering validates the name.
				if self.peek1().kind == t!("(") {
					return self.parse_function_call(stk).await;
				}
				if keyword.is_non_reserved() {
					self.pop_peek();
					return Ok(GqlExpr::Variable(Ident {
						name: self.span_str(token.span).to_owned(),
						span: token.span,
					}));
				}
				// A temporal type keyword followed by a string is a typed
				// temporal literal (`temporalLiteral`, GQL.g4:3010), not a
				// misused variable name.
				if matches!(
					keyword,
					Keyword::Date
						| Keyword::Time | Keyword::Datetime
						| Keyword::Timestamp
						| Keyword::Duration
				) && matches!(
					self.peek1().kind,
					TokenKind::SingleQuoted { .. } | TokenKind::DoubleQuoted { .. }
				) {
					bail!(
						"Typed temporal literals (`{} '…'`) are not supported yet",
						self.span_str(token.span),
						@token.span.covers(self.peek1().span)
					);
				}
				// `SESSION_USER` is a grammatical primary
				// (`generalValueSpecification`, GQL.g4:2273).
				if keyword == Keyword::SessionUser {
					bail!(
						"The `SESSION_USER` value specification is not supported yet",
						@token.span
					);
				}
				bail!(
					"`{}` is a reserved word and cannot be used as a variable name",
					self.span_str(token.span),
					@token.span => "use a `\"…\"` or `` `…` `` delimited identifier instead"
				);
			}
			_ => unexpected!(self, token, "an expression"),
		}
	}

	/// Parse a function call: a name followed by a parenthesized, comma
	/// separated argument list. The caller must have checked that the next
	/// two tokens are a name and `(`.
	///
	/// The aggregate argument forms — a sole `*` (`count(*)`, GQL.g4:2381)
	/// and a leading `DISTINCT`/`ALL` set quantifier (`generalSetFunction`,
	/// GQL.g4:2387) — are parsed into the AST for any function name; lowering
	/// validates the name and rejects aggregates with a targeted error.
	async fn parse_function_call(&mut self, stk: &mut Stk) -> ParseResult<GqlExpr> {
		let name_token = self.pop_peek();
		let name = Ident {
			name: self.span_str(name_token.span).to_owned(),
			span: name_token.span,
		};
		let open = self.pop_peek().span;
		let star = if self.peek_kind() == t!("*") && self.peek1().kind == t!(")") {
			Some(self.pop_peek().span)
		} else {
			None
		};
		let quantifier = if star.is_some() {
			None
		} else if self.eat(t!("DISTINCT")) {
			Some(SetQuantifier::Distinct)
		} else if self.eat(t!("ALL")) {
			Some(SetQuantifier::All)
		} else {
			None
		};
		let mut args = Vec::new();
		if star.is_none() && (quantifier.is_some() || self.peek_kind() != t!(")")) {
			loop {
				args.push(stk.run(|stk| self.parse_expr(stk)).await?);
				if !self.eat(t!(",")) {
					break;
				}
			}
		}
		self.expect_closing_delimiter(t!(")"), open)?;
		Ok(GqlExpr::FunctionCall {
			name,
			quantifier,
			star,
			args,
			span: name_token.span.covers(self.last_span()),
		})
	}

	/// Decode the name of a parameter token.
	pub(super) fn parse_parameter_name(&self, token: Token) -> ParseResult<String> {
		Lexer::parameter_name_span(self.span_str(token.span), token.span)
	}

	/// Parse a numeric literal token into a literal value.
	fn parse_number_literal(
		&self,
		token: Token,
		kind: NumberKind,
		suffix: Option<NumberSuffix>,
	) -> ParseResult<GqlLiteral> {
		match kind {
			NumberKind::Integer | NumberKind::Hex | NumberKind::Octal | NumberKind::Binary => {
				match suffix {
					// The `M` exact number suffix on an integer is already
					// exact.
					None | Some(NumberSuffix::Exact) => {
						Ok(GqlLiteral::Integer(self.parse_integer_token(token, kind)?))
					}
					// `F`/`D` approximate suffixes only occur on decimal
					// integers (the prefixed radix forms consume no suffix).
					Some(NumberSuffix::Float | NumberSuffix::Double) => {
						Ok(GqlLiteral::Float(self.parse_float_token(token)?))
					}
				}
			}
			// Common and scientific notation parse as a 64-bit float. The
			// AST has no exact decimal representation, so the `M` suffix is
			// approximated as well.
			NumberKind::Float | NumberKind::Scientific => {
				Ok(GqlLiteral::Float(self.parse_float_token(token)?))
			}
		}
	}

	/// Parse the value of an integer numeric token.
	pub(super) fn parse_integer_token(&self, token: Token, kind: NumberKind) -> ParseResult<i64> {
		let (radix, digits) = self.integer_token_digits(token, kind);
		i64::from_str_radix(&digits, radix).map_err(|_| {
			// The lexer guarantees well-formed digits, so the only error is
			// overflow.
			syntax_error!("Integer literal is too large to fit in a 64-bit integer", @token.span)
		})
	}

	/// Parse the value of an integer numeric token as a `u32`, for
	/// quantifier bounds.
	pub(super) fn parse_u32_token(&self, token: Token, kind: NumberKind) -> ParseResult<u32> {
		let (radix, digits) = self.integer_token_digits(token, kind);
		u32::from_str_radix(&digits, radix)
			.map_err(|_| syntax_error!("Quantifier bound is too large", @token.span))
	}

	/// Returns the radix and the cleaned digits (radix prefix, digit
	/// separators and suffix removed) of an integer numeric token.
	fn integer_token_digits(&self, token: Token, kind: NumberKind) -> (u32, String) {
		let text = self.number_token_text(token);
		let (radix, digits) = match kind {
			NumberKind::Hex => (16, text.strip_prefix("0x").unwrap_or(text)),
			NumberKind::Octal => (8, text.strip_prefix("0o").unwrap_or(text)),
			NumberKind::Binary => (2, text.strip_prefix("0b").unwrap_or(text)),
			_ => (10, text),
		};
		(radix, digits.replace('_', ""))
	}

	/// Parse the value of a decimal numeric token as a 64-bit float.
	fn parse_float_token(&self, token: Token) -> ParseResult<f64> {
		let text = self.number_token_text(token).replace('_', "");
		text.parse::<f64>().map_err(|_| {
			// The lexer guarantees a well-formed literal; this is
			// unreachable in practice.
			syntax_error!("Invalid numeric literal", @token.span)
		})
	}

	/// Returns the text of a numeric token with the suffix, if any, removed.
	fn number_token_text(&self, token: Token) -> &str {
		let text = self.span_str(token.span);
		if let TokenKind::Number {
			suffix: Some(_),
			..
		} = token.kind
		{
			// The suffix is a single ascii character, so the slice cannot
			// split a character.
			&text[..text.len() - 1]
		} else {
			text
		}
	}
}
