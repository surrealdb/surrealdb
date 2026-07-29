//! This module defines the pratt parser for operators.

use reblessive::Stk;
use surrealdb_sql::operator::{BindingPower, BooleanOperator, MatchesOperator, NearestNeighbor};
use surrealdb_sql::{BinaryOperator, Expr, Literal, Part, PostfixOperator, PrefixOperator};
use surrealdb_types::ToSql;

use super::enter_query_recursion;
use super::mac::unexpected;
use crate::error::bail;
use crate::lexer::compound::Numeric;
use crate::parser::mac::expected;
use crate::parser::{ParseResult, Parser};
use crate::token::{Span, Token, TokenKind, t};

impl Parser<'_> {
	/// Parse a generic expression without triggering the query depth and
	/// setting table_as_field.
	///
	/// Meant to be used when parsing an expression the first time to avoid
	/// having the depth limit be lowered unnecessarily.
	pub async fn parse_expr_start(&mut self, stk: &mut Stk) -> ParseResult<Expr> {
		self.table_as_field = true;
		self.pratt_parse_expr(stk, BindingPower::Base).await
	}

	/// Parsers a generic value.
	///
	/// A generic loose ident like `foo` in for example `foo.bar` can be two
	/// different values depending on context: a table or a field the current
	/// document. This function parses loose idents as a table, see
	/// [`Parser::parse_expr_field`] for parsing loose idents as fields
	pub async fn parse_expr_table(&mut self, stk: &mut Stk) -> ParseResult<Expr> {
		let old = self.table_as_field;
		self.table_as_field = false;
		let res = enter_query_recursion!(this = self => {
			this.pratt_parse_expr(stk, BindingPower::Base).await
		});
		self.table_as_field = old;
		res
	}

	/// Parsers a generic value.
	///
	/// A generic loose ident like `foo` in for example `foo.bar` can be two
	/// different values depending on context: a table or a field the current
	/// document. This function parses loose idents as a field, see
	/// [`Parser::parse_expr_table`] for parsing loose idents as table
	pub async fn parse_expr_field(&mut self, stk: &mut Stk) -> ParseResult<Expr> {
		let old = self.table_as_field;
		self.table_as_field = true;
		let res = enter_query_recursion!(this = self => {
			this.pratt_parse_expr(stk, BindingPower::Base).await
		});
		self.table_as_field = old;
		res
	}

	/// Parsers a generic value.
	///
	/// Inherits how loose identifiers are parsed from it's caller.
	pub(super) async fn parse_expr_inherit(&mut self, stk: &mut Stk) -> ParseResult<Expr> {
		enter_query_recursion!(this = self => {
			this.pratt_parse_expr(stk, BindingPower::Base).await
		})
	}

	/// Returns the binding power of an infix operator.
	///
	/// Binding power is the opposite of precedence: a higher binding power
	/// means that a token is more like to operate directly on it's neighbours.
	/// Example `*` has a higher binding power than `-` resulting in 1 - 2 * 3
	/// being parsed as 1 - (2 * 3).
	///
	/// All operators in SurrealQL which are parsed by the functions in this
	/// module are left associative or have no defined associativity.
	fn infix_binding_power(&mut self, token: TokenKind) -> Option<BindingPower> {
		match token {
			// assigment operators have the lowest binding power.
			//t!("+=") | t!("-=") | t!("+?=") => Some((2, 1)),
			t!("||") | t!("OR") => Some(BindingPower::Or),
			t!("&&") | t!("AND") => Some(BindingPower::And),

			// Equality operators have same binding power.
			t!("=") | t!("IS") | t!("==") | t!("!=") | t!("*=") | t!("?=") | t!("@") => {
				Some(BindingPower::Equality)
			}

			t!("<") => {
				if let Some(peek) = self.peek_whitespace1()
					&& let t!("-") | t!("~") | t!("->") | t!("..") = peek.kind
				{
					return None;
				}
				Some(BindingPower::Relation)
			}

			t!(">") => {
				if let Some(t!("..")) = self.peek_whitespace1().map(|x| x.kind) {
					return Some(BindingPower::Range);
				}
				Some(BindingPower::Relation)
			}

			t!("..") => Some(BindingPower::Range),

			t!("<=")
			| t!(">=")
			| t!("∋")
			| t!("CONTAINS")
			| t!("∌")
			| t!("CONTAINSNOT")
			| t!("∈")
			| t!("INSIDE")
			| t!("∉")
			| t!("NOTINSIDE")
			| t!("⊇")
			| t!("CONTAINSALL")
			| t!("⊃")
			| t!("CONTAINSANY")
			| t!("⊅")
			| t!("CONTAINSNONE")
			| t!("⊆")
			| t!("ALLINSIDE")
			| t!("⊂")
			| t!("ANYINSIDE")
			| t!("⊄")
			| t!("NONEINSIDE")
			| t!("OUTSIDE")
			| t!("INTERSECTS")
			| t!("NOT")
			| t!("IN")
			| t!("<|") => Some(BindingPower::Relation),

			t!("+") | t!("-") => Some(BindingPower::AddSub),
			t!("*") | t!("×") | t!("/") | t!("÷") | t!("%") => Some(BindingPower::MulDiv),
			t!("**") => Some(BindingPower::Power),
			t!("?:") | t!("?") => Some(BindingPower::Nullish),
			_ => None,
		}
	}

	fn prefix_binding_power(&mut self, token: TokenKind) -> Option<BindingPower> {
		match token {
			t!("!") | t!("+") | t!("-") => Some(BindingPower::Prefix),
			t!("..") => Some(BindingPower::Range),
			t!("<") => {
				if let Some(peek) = self.peek_whitespace1() {
					if peek.kind == t!("-") {
						let recover = self.last_span();
						if self.peek2().kind == TokenKind::Digits {
							self.backup_after(recover);
							return Some(BindingPower::Prefix);
						}
						return None;
					}
					if let t!("~") | t!("->") = peek.kind {
						return None;
					}
				}
				Some(BindingPower::Prefix)
			}
			_ => None,
		}
	}

	fn postfix_binding_power(&mut self, token: TokenKind) -> Option<BindingPower> {
		match token {
			t!(">") => {
				if let Some(peek) = self.peek_whitespace1()
					&& let t!("..") = peek.kind
				{
					if let Some(peek) = self.peek_whitespace2()
						&& (t!("=") == peek.kind || Self::kind_starts_expression(peek.kind))
					{
						return None;
					} else {
						return Some(BindingPower::Range);
					}
				}
				None
			}
			t!("..") => match self.peek_whitespace1().map(|x| x.kind) {
				Some(t!("=")) => None,
				Some(x) if Self::kind_starts_expression(x) => None,
				_ => Some(BindingPower::Range),
			},
			t!("(") => Some(BindingPower::Call),
			_ => None,
		}
	}

	async fn parse_prefix_op(&mut self, stk: &mut Stk, min_bp: BindingPower) -> ParseResult<Expr> {
		let token = self.peek();
		let operator = match token.kind {
			t!("+") => {
				// +123 is a single number token, so parse it as such
				if let Some(TokenKind::Digits) = self.peek_whitespace1().map(|x| x.kind) {
					// This is a bit of an annoying special case.
					// The problem is that `+` and `-` can be an prefix operator and a the start
					// of a number token.
					// To figure out which it is we need to peek the next whitespace token,
					// This eats the digits that the lexer needs to lex the number. So we we need
					// to backup before the digits token was consumed, clear the digits token from
					// the token buffer so it isn't popped after parsing the number and then lex the
					// number.
					self.pop_peek();
					let expr = match self.next_token_value::<Numeric>()? {
						Numeric::Float(f) => Expr::Literal(Literal::Float(f)),
						Numeric::Integer(i) => {
							Expr::Literal(Literal::Integer(i.into_int(self.recent_span())?))
						}
						Numeric::Decimal(d) => Expr::Literal(Literal::Decimal(d)),
						Numeric::Duration(d) => Expr::Prefix {
							op: PrefixOperator::Positive,
							expr: Box::new(Expr::Literal(Literal::Duration(d))),
						},
					};
					if self.peek_continues_idiom() {
						return self
							.parse_remaining_value_idiom(stk, vec![Part::Start(expr)])
							.await;
					} else {
						return Ok(expr);
					}
				}
				self.pop_peek();

				PrefixOperator::Positive
			}
			t!("-") => {
				// -123 is a single number token, so parse it as such
				if let Some(TokenKind::Digits) = self.peek_whitespace1().map(|x| x.kind) {
					// This is a bit of an annoying special case.
					// The problem is that `+` and `-` can be an prefix operator and a the start
					// of a number token.
					// To figure out which it is we need to peek the next whitespace token,
					// This eats the digits that the lexer needs to lex the number. So we we need
					// to backup before the digits token was consumed, clear the digits token from
					// the token buffer so it isn't popped after parsing the number and then lex the
					// number.
					self.pop_peek();
					let expr = match self.next_token_value::<Numeric>()? {
						Numeric::Float(f) => Expr::Literal(Literal::Float(-f)),
						Numeric::Integer(i) => {
							Expr::Literal(Literal::Integer(i.into_neg_int(self.recent_span())?))
						}
						Numeric::Decimal(d) => Expr::Literal(Literal::Decimal(-d)),
						Numeric::Duration(d) => Expr::Prefix {
							op: PrefixOperator::Negate,
							expr: Box::new(Expr::Literal(Literal::Duration(d))),
						},
					};
					if self.peek_continues_idiom() {
						return self
							.parse_remaining_value_idiom(stk, vec![Part::Start(expr)])
							.await;
					} else {
						return Ok(expr);
					}
				}

				self.pop_peek();

				PrefixOperator::Negate
			}
			t!("!") => {
				self.pop_peek();
				PrefixOperator::Not
			}
			t!("<") => {
				self.pop_peek();
				let kind = self.parse_kind(stk, token.span).await?;
				PrefixOperator::Cast(kind)
			}
			t!("..") => {
				self.pop_peek();
				if let Some(x) = self.peek_whitespace() {
					if let t!("=") = x.kind {
						self.pop_peek();
						PrefixOperator::RangeInclusive
					} else if !Self::kind_starts_prime_value(x.kind) {
						// unbounded range.
						return Ok(Expr::Literal(Literal::UnboundedRange));
					} else {
						PrefixOperator::Range
					}
				} else {
					return Ok(Expr::Literal(Literal::UnboundedRange));
				}
			}
			// should be unreachable as we previously check if the token was a prefix op.
			_ => unreachable!(),
		};

		let v = stk.run(|stk| self.pratt_parse_expr(stk, min_bp)).await?;

		Ok(Expr::Prefix {
			op: operator,
			expr: Box::new(v),
		})
	}

	pub(super) fn parse_nearest_neighbor(&mut self, token: Token) -> ParseResult<NearestNeighbor> {
		let amount = self.next_token_value()?;
		let res = if self.eat(t!(",")) {
			let token = self.peek();
			match token.kind {
				TokenKind::Distance(_) => {
					let d = self.parse_distance()?;
					NearestNeighbor::K(amount, d)
				}
				TokenKind::Digits => {
					let ef = self.next_token_value()?;
					NearestNeighbor::Approximate(amount, ef)
				}
				_ => {
					bail!("Unexpected token {} expected a distance of an integer", token.kind,
						@token.span => "The NN operator accepts either a distance or an EF value (integer)")
				}
			}
		} else {
			NearestNeighbor::KTree(amount)
		};
		if !self.eat(t!("|")) || !self.eat_whitespace(t!(">")) {
			bail!("Unexpected token `{}` expected delimiter `|>`",
				self.peek().kind,
				@self.recent_span(),
				@token.span=> "expected this delimiter to close"
			);
		}

		Ok(res)
	}

	/// Returns if an operator has a defined associativity.
	/// For example: `a - b - c == (a - b) - c` so `-` is left associative.
	/// However `a == b == c` is not defined to be either `(a == b) == c` nor `a == (b == c)`.
	fn operator_has_associativity(operator: &BinaryOperator) -> bool {
		!matches!(
			operator,
			BinaryOperator::Equal
				| BinaryOperator::NotEqual
				| BinaryOperator::AllEqual
				| BinaryOperator::AnyEqual
				| BinaryOperator::LessThan
				| BinaryOperator::LessThanEqual
				| BinaryOperator::MoreThan
				| BinaryOperator::MoreThanEqual
				| BinaryOperator::Matches(_)
				| BinaryOperator::Contain
				| BinaryOperator::NotContain
				| BinaryOperator::ContainAll
				| BinaryOperator::ContainAny
				| BinaryOperator::ContainNone
				| BinaryOperator::Inside
				| BinaryOperator::NotInside
				| BinaryOperator::AllInside
				| BinaryOperator::AnyInside
				| BinaryOperator::NoneInside
				| BinaryOperator::Outside
				| BinaryOperator::Intersects
				| BinaryOperator::NearestNeighbor(_)
		)
	}

	fn expr_is_range(expr: &Expr) -> bool {
		match expr {
			Expr::Binary {
				op,
				..
			} => matches!(
				op,
				BinaryOperator::Range
					| BinaryOperator::RangeSkipInclusive
					| BinaryOperator::RangeSkip
					| BinaryOperator::RangeInclusive
			),
			Expr::Prefix {
				op,
				..
			} => matches!(op, PrefixOperator::Range | PrefixOperator::RangeInclusive),
			Expr::Postfix {
				op,
				..
			} => matches!(op, PostfixOperator::Range | PostfixOperator::RangeSkip),
			_ => false,
		}
	}

	async fn parse_infix_op(
		&mut self,
		stk: &mut Stk,
		min_bp: BindingPower,
		lhs: Expr,
		lhs_prime: bool, /* if lhs was a prime expression, required for ensuring (a..b)..c does
		                  * not fail. */
	) -> ParseResult<Expr> {
		let token = self.next();
		let operator = match token.kind {
			t!("||") | t!("OR") => BinaryOperator::Or,
			t!("&&") | t!("AND") => BinaryOperator::And,
			t!("?:") => BinaryOperator::TenaryCondition,
			t!("?") => {
				if !self.eat_whitespace(t!("?")) {
					unexpected!(self, token, "`??`")
				}
				BinaryOperator::NullCoalescing
			}
			t!("==") => BinaryOperator::ExactEqual,
			t!("!=") => BinaryOperator::NotEqual,
			t!("*=") => BinaryOperator::AllEqual,
			t!("?=") => BinaryOperator::AnyEqual,
			t!("=") => BinaryOperator::Equal,
			t!("@") => {
				let op = self.parse_matches()?;
				BinaryOperator::Matches(op)
			}
			t!("<=") => BinaryOperator::LessThanEqual,
			t!("<") => BinaryOperator::LessThan,
			t!(">=") => BinaryOperator::MoreThanEqual,
			t!("**") => BinaryOperator::Power,
			t!("+") => BinaryOperator::Add,
			t!("-") => BinaryOperator::Subtract,
			t!("*") | t!("×") => BinaryOperator::Multiply,
			t!("/") | t!("÷") => BinaryOperator::Divide,
			t!("%") => BinaryOperator::Remainder,
			t!("∋") | t!("CONTAINS") => BinaryOperator::Contain,
			t!("∌") | t!("CONTAINSNOT") => BinaryOperator::NotContain,
			t!("∈") | t!("INSIDE") => BinaryOperator::Inside,
			t!("∉") | t!("NOTINSIDE") => BinaryOperator::NotInside,
			t!("⊇") | t!("CONTAINSALL") => BinaryOperator::ContainAll,
			t!("⊃") | t!("CONTAINSANY") => BinaryOperator::ContainAny,
			t!("⊅") | t!("CONTAINSNONE") => BinaryOperator::ContainNone,
			t!("⊆") | t!("ALLINSIDE") => BinaryOperator::AllInside,
			t!("⊂") | t!("ANYINSIDE") => BinaryOperator::AnyInside,
			t!("⊄") | t!("NONEINSIDE") => BinaryOperator::NoneInside,
			t!("IS") => {
				if self.eat(t!("NOT")) {
					BinaryOperator::NotEqual
				} else {
					BinaryOperator::Equal
				}
			}
			t!("OUTSIDE") => BinaryOperator::Outside,
			t!("INTERSECTS") => BinaryOperator::Intersects,
			t!("NOT") => {
				expected!(self, t!("IN"));
				BinaryOperator::NotInside
			}
			t!("IN") => BinaryOperator::Inside,
			t!("<|") => {
				BinaryOperator::NearestNeighbor(Box::new(self.parse_nearest_neighbor(token)?))
			}

			t!(">") => {
				if let Some(t!("..")) = self.peek_whitespace().map(|x| x.kind) {
					self.pop_peek();
					if let Some(t!("=")) = self.peek_whitespace().map(|x| x.kind) {
						self.pop_peek();
						BinaryOperator::RangeSkipInclusive
					} else {
						BinaryOperator::RangeSkip
					}
				} else {
					BinaryOperator::MoreThan
				}
			}
			t!("..") => {
				if let Some(t!("=")) = self.peek_whitespace().map(|x| x.kind) {
					self.pop_peek();
					BinaryOperator::RangeInclusive
				} else {
					BinaryOperator::Range
				}
			}

			// should be unreachable as we previously check if the token was a prefix op.
			x => unreachable!("found non-operator token {x:?}"),
		};
		let rhs_covered = self.peek().kind == t!("(");
		let rhs = stk.run(|ctx| self.pratt_parse_expr(ctx, min_bp)).await?;

		let has_associatitivity = Self::operator_has_associativity(&operator);
		if !lhs_prime
			&& !has_associatitivity
			&& BindingPower::for_expr(&lhs) == BindingPower::for_binary_operator(&operator)
		{
			let span = token.span.covers(self.recent_span());
			if matches!(
				operator,
				BinaryOperator::Range
					| BinaryOperator::RangeSkipInclusive
					| BinaryOperator::RangeSkip
					| BinaryOperator::RangeInclusive
			) {
				bail!("Chained range operators has no specified associativity",
				@span => "use parens, '()', to specify which operator must be evaluated first")
			} else {
				bail!("Chained relational operators have no defined associativity.",
				@span => "Use parens, '()', to specify which operator must be evaluated first")
			}
		}

		if !rhs_covered
			&& !has_associatitivity
			&& BindingPower::for_expr(&rhs) == BindingPower::for_binary_operator(&operator)
		{
			let span = token.span.covers(self.recent_span());
			if matches!(
				operator,
				BinaryOperator::Range
					| BinaryOperator::RangeSkipInclusive
					| BinaryOperator::RangeSkip
					| BinaryOperator::RangeInclusive
			) {
				bail!("Chained range operators have no defined associativity.",
				@span => "Use parens, '()', to specify which operator must be evaluated first")
			} else {
				bail!("Chained relational operators have no defined associativity.",
				@span => "Use parens, '()', to specify which operator must be evaluated first")
			}
		}

		Ok(Expr::Binary {
			left: Box::new(lhs),
			op: operator,
			right: Box::new(rhs),
		})
	}

	fn parse_matches(&mut self) -> ParseResult<MatchesOperator> {
		let peek = self.peek();
		match peek.kind {
			TokenKind::Digits => {
				let number = self.next_token_value()?;
				let op = if self.eat(t!(",")) {
					let peek = self.next();
					let op = match peek.kind {
						t!("AND") => BooleanOperator::And,
						t!("OR") => BooleanOperator::Or,
						_ => unexpected!(self, peek, "either `AND` or `OR`"),
					};
					Some(op)
				} else {
					None
				};
				expected!(self, t!("@"));
				Ok(MatchesOperator {
					operator: op,
					rf: Some(number),
				})
			}
			t!("AND") => {
				self.pop_peek();
				expected!(self, t!("@"));
				Ok(MatchesOperator {
					operator: Some(BooleanOperator::And),
					rf: None,
				})
			}
			t!("OR") => {
				self.pop_peek();
				expected!(self, t!("@"));
				Ok(MatchesOperator {
					operator: Some(BooleanOperator::Or),
					rf: None,
				})
			}
			t!("@") => {
				self.pop_peek();
				Ok(MatchesOperator {
					operator: None,
					rf: None,
				})
			}
			_ => unexpected!(self, peek, "a match reference, operator or `@`"),
		}
	}

	async fn parse_postfix(
		&mut self,
		stk: &mut Stk,
		lhs: Expr,
		lhs_prime: bool,
	) -> ParseResult<Expr> {
		let token = self.next();
		let op = match token.kind {
			t!(">") => {
				assert!(self.eat_whitespace(t!("..")));
				if !lhs_prime && Self::expr_is_range(&lhs) {
					bail!("Chaining range operators has no specified associativity",
						@token.span => "use parens, '()', to specify which operator must be evaluated first")
				}
				PostfixOperator::RangeSkip
			}
			t!("..") => {
				if !lhs_prime && Self::expr_is_range(&lhs) {
					bail!("Chaining range operators has no specified associativity",
						@token.span => "use parens, '()', to specify which operator must be evaluated first")
				}
				PostfixOperator::Range
			}
			t!("(") => {
				let mut args = Vec::new();
				loop {
					if self.eat(t!(")")) {
						break;
					}

					let arg = stk.run(|ctx| self.parse_expr_inherit(ctx)).await?;
					args.push(arg);

					if !self.eat(t!(",")) {
						self.expect_closing_delimiter(t!(")"), token.span)?;
						break;
					}
				}
				PostfixOperator::Call(args)
			}
			t!(".") => {
				let name = self.parse_ident()?;
				expected!(self, t!("("));

				let mut args = Vec::new();
				loop {
					if self.eat(t!(")")) {
						break;
					}

					let arg = stk.run(|ctx| self.parse_expr_inherit(ctx)).await?;
					args.push(arg);

					if !self.eat(t!(",")) {
						self.expect_closing_delimiter(t!(")"), token.span)?;
						break;
					}
				}
				PostfixOperator::MethodCall(name.into_string(), args)
			}
			// should be unreachable as we previously check if the token was a postfix op.
			x => unreachable!("found non-operator token {x:?}"),
		};

		Ok(Expr::Postfix {
			expr: Box::new(lhs),
			op,
		})
	}

	/// Account for one additional level of expression-operator nesting against
	/// the dedicated depth budget, returning a syntax error when it is
	/// exhausted.
	///
	/// This guards the otherwise-unbounded depth of the `Expr` tree built by
	/// [`Self::pratt_parse_expr`]; see `ParserSettings::expr_recursion_limit`
	/// for why an unbounded operator chain is a denial-of-service vector.
	fn enter_expr_depth(&mut self) -> ParseResult<()> {
		if self.settings.expr_recursion_limit == 0 {
			bail!("Exceeded expression recursion depth limit",
				@self.last_span() => "this expression nests or chains operators too deeply");
		}
		self.settings.expr_recursion_limit -= 1;
		Ok(())
	}

	/// The pratt parsing loop.
	/// Parses expression according to binding power.
	///
	/// This is a thin wrapper around [`Self::pratt_parse_expr_inner`] that
	/// charges the expression-depth budget for the level introduced by this
	/// call (so prefix chains, which recurse through here, are bounded) and
	/// restores it on return so that sibling expressions are not charged for
	/// one another. The inner loop charges the budget again for every operator
	/// it appends to the left-associative spine.
	async fn pratt_parse_expr(&mut self, stk: &mut Stk, min_bp: BindingPower) -> ParseResult<Expr> {
		let restore_to = self.settings.expr_recursion_limit;
		self.enter_expr_depth()?;
		let res = self.pratt_parse_expr_inner(stk, min_bp).await;
		// Restore everything this call consumed, including the levels charged by
		// the spine loop. Nested `pratt_parse_expr` calls restore themselves, so
		// `restore_to` is exactly this call's entry value.
		self.settings.expr_recursion_limit = restore_to;
		res
	}

	async fn pratt_parse_expr_inner(
		&mut self,
		stk: &mut Stk,
		min_bp: BindingPower,
	) -> ParseResult<Expr> {
		let peek = self.peek();
		let (mut lhs, mut lhs_prime) = if let Some(bp) = self.prefix_binding_power(peek.kind) {
			(self.parse_prefix_op(stk, bp).await?, false)
		} else {
			(self.parse_prime_expr(stk).await?, true)
		};

		loop {
			let token = self.peek();

			if let Some(bp) = self.postfix_binding_power(token.kind) {
				if bp <= min_bp {
					break;
				}

				// Appending a postfix operator deepens the spine by one level.
				self.enter_expr_depth()?;
				lhs = self.parse_postfix(stk, lhs, lhs_prime).await?;
				lhs_prime = false;
				continue;
			}

			// explain that assignment operators can't be used in normal expressions.
			if let t!("+=") | t!("-=") | t!("+?=") = token.kind {
				unexpected!(self,token,"an operator",
					=> "assignment operators are only allowed in SET and DUPLICATE KEY UPDATE clauses")
			}

			let Some(bp) = self.infix_binding_power(token.kind) else {
				break;
			};

			if bp <= min_bp {
				break;
			}

			// Appending an infix operator deepens the spine by one level.
			self.enter_expr_depth()?;
			lhs = self.parse_infix_op(stk, bp, lhs, lhs_prime).await?;
			lhs_prime = false;
		}

		Ok(lhs)
	}

	pub fn reject_letless_let(expr: &Expr, span: Span) -> ParseResult<()> {
		let Expr::Binary {
			left,
			op,
			..
		} = expr
		else {
			return Ok(());
		};
		let Expr::Param(p) = &**left else {
			return Ok(());
		};
		let BinaryOperator::Equal = op else {
			return Ok(());
		};
		bail!("Parameter declarations without `let` are deprecated.",
			@span => "Replace with `let {} = ...` to keep the previous behavior.", p.to_sql())
	}
}

#[cfg(test)]
mod test {
	use surrealdb_sql::{BinaryOperator, Expr, Kind, Literal, PrefixOperator};
	use surrealdb_types::ToSql;

	#[test]
	fn cast_int() {
		let sql = "<int>1.2345";
		let out = crate::expr(sql).unwrap();
		assert_eq!("<int> 1.2345f", out.to_sql());
		assert_eq!(
			out,
			Expr::Prefix {
				op: PrefixOperator::Cast(Kind::Int),
				expr: Box::new(Expr::Literal(Literal::Float(1.2345)))
			}
		)
	}

	#[test]
	fn cast_string() {
		let sql = "<string>1.2345";
		let out = crate::expr(sql).unwrap();
		assert_eq!("<string> 1.2345f", out.to_sql());
		assert_eq!(
			out,
			Expr::Prefix {
				op: PrefixOperator::Cast(Kind::String),
				expr: Box::new(Expr::Literal(Literal::Float(1.2345)))
			}
		)
	}

	#[test]
	fn expression_statement() {
		let sql = "true AND false";
		let out = crate::expr(sql).unwrap();
		assert_eq!("true AND false", out.to_sql());
	}

	#[test]
	fn expression_left_opened() {
		let sql = "3 * 3 * 3 = 27";
		let out = crate::expr(sql).unwrap();
		assert_eq!("3 * 3 * 3 = 27", out.to_sql());
	}

	#[test]
	fn expression_left_closed() {
		let sql = "(3 * 3 * 3) = 27";
		let out = crate::expr(sql).unwrap();
		assert_eq!("3 * 3 * 3 = 27", out.to_sql());
	}

	#[test]
	fn expression_right_opened() {
		let sql = "27 = 3 * 3 * 3";
		let out = crate::expr(sql).unwrap();
		assert_eq!("27 = 3 * 3 * 3", out.to_sql());
	}

	#[test]
	fn expression_right_closed() {
		let sql = "27 = (3 * 3 * 3)";
		let out = crate::expr(sql).unwrap();
		assert_eq!("27 = 3 * 3 * 3", out.to_sql());
	}

	#[test]
	fn expression_both_opened() {
		let sql = "3 * 3 * 3 = 3 * 3 * 3";
		let out = crate::expr(sql).unwrap();
		assert_eq!("3 * 3 * 3 = 3 * 3 * 3", out.to_sql());
	}

	#[test]
	fn expression_both_closed() {
		let sql = "(3 * 3 * 3) = (3 * 3 * 3)";
		let out = crate::expr(sql).unwrap();
		assert_eq!("3 * 3 * 3 = 3 * 3 * 3", out.to_sql());
	}

	#[test]
	fn expression_closed_required() {
		let sql = "(3 + 3) * 3";
		let out = crate::expr(sql).unwrap();
		assert_eq!("(3 + 3) * 3", out.to_sql());
	}

	#[test]
	fn range_closed_required() {
		let sql = "(1..2)..3";
		let out = crate::expr(sql).unwrap();
		assert_eq!("(1..2)..3", out.to_sql());
	}

	#[test]
	fn expression_unary() {
		let sql = "-a";
		let out = crate::expr(sql).unwrap();
		assert_eq!(sql, out.to_sql());
	}

	#[test]
	fn expression_with_unary() {
		let sql = "-(5) + 5";
		let out = crate::expr(sql).unwrap();
		assert_eq!("-5 + 5", out.to_sql());
	}

	#[test]
	fn expression_left_associative() {
		let sql = "1 - 1 - 1";
		let out = crate::expr(sql).unwrap();
		let one = Expr::Literal(Literal::Integer(1));

		let expected = Expr::Binary {
			left: Box::new(Expr::Binary {
				left: Box::new(one.clone()),
				op: BinaryOperator::Subtract,
				right: Box::new(one.clone()),
			}),
			op: BinaryOperator::Subtract,
			right: Box::new(one),
		};
		assert_eq!(expected, out);
	}
}
