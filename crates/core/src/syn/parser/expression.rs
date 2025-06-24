//! This module defines the pratt parser for operators.

use reblessive::Stk;

use super::mac::unexpected;
use crate::sql::operator::{BindingPower, NearestNeighbor};
use crate::sql::{AssignOperator, BinaryOperator, Expr, Part, PostfixOperator, PrefixOperator};
use crate::syn::error::bail;
use crate::syn::parser::mac::expected;
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::{self, Token, TokenKind, t};
use crate::val::Number;

impl Parser<'_> {
	/// Parsers a generic value.
	///
	/// A generic loose ident like `foo` in for example `foo.bar` can be two different values
	/// depending on context: a table or a field the current document. This function parses loose
	/// idents as a table, see [`parse_expr_field`] for parsing loose idents as fields
	pub async fn parse_expr_table(&mut self, ctx: &mut Stk) -> ParseResult<Expr> {
		let old = self.table_as_field;
		self.table_as_field = false;
		let res = self.pratt_parse_expr(ctx, BindingPower::Base).await;
		self.table_as_field = old;
		res
	}

	/// Parsers a generic value.
	///
	/// A generic loose ident like `foo` in for example `foo.bar` can be two different values
	/// depending on context: a table or a field the current document. This function parses loose
	/// idents as a field, see [`parse_value`] for parsing loose idents as table
	pub(crate) async fn parse_expr_field(&mut self, ctx: &mut Stk) -> ParseResult<Expr> {
		let old = self.table_as_field;
		self.table_as_field = true;
		let res = self.pratt_parse_expr(ctx, BindingPower::Base).await;
		self.table_as_field = old;
		res
	}

	/// Parsers a generic value.
	///
	/// Inherits how loose identifiers are parsed from it's caller.
	pub(super) async fn parse_expr_inherit(&mut self, ctx: &mut Stk) -> ParseResult<Expr> {
		self.pratt_parse_expr(ctx, BindingPower::Base).await
	}

	/// Parse a assigner operator.
	pub(super) fn parse_assign_operator(&mut self) -> ParseResult<AssignOperator> {
		let token = self.next();
		match token.kind {
			t!("=") => Ok(AssignOperator::Equal),
			t!("+=") => Ok(AssignOperator::Add),
			t!("-=") => Ok(AssignOperator::Subtract),
			t!("+?=") => Ok(AssignOperator::Extend),
			_ => unexpected!(self, token, "an assign operator"),
		}
	}

	/// Returns the binding power of an infix operator.
	///
	/// Binding power is the opposite of precendence: a higher binding power means that a token is
	/// more like to operate directly on it's neighbours. Example `*` has a higher binding power
	/// than `-` resulting in 1 - 2 * 3 being parsed as 1 - (2 * 3).
	///
	/// All operators in SurrealQL which are parsed by the functions in this module are left
	/// associative or have no defined associativity.
	fn infix_binding_power(&mut self, token: TokenKind) -> Option<BindingPower> {
		// TODO: Look at ordering of operators.
		match token {
			// assigment operators have the lowest binding power.
			//t!("+=") | t!("-=") | t!("+?=") => Some((2, 1)),
			t!("||") | t!("OR") => Some(BindingPower::Or),
			t!("&&") | t!("AND") => Some(BindingPower::And),

			// Equality operators have same binding power.
			t!("=")
			| t!("IS")
			| t!("==")
			| t!("!=")
			| t!("*=")
			| t!("?=")
			| t!("~")
			| t!("!~")
			| t!("*~")
			| t!("?~")
			| t!("@") => Some(BindingPower::Equality),

			t!("<") => {
				let peek = self.peek_whitespace1();
				if matches!(peek.kind, t!("-") | t!("->") | t!("..")) {
					return None;
				}
				Some(BindingPower::Relation)
			}

			t!(">") => {
				if self.peek_whitespace1().kind == t!("..") {
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
			t!("?:") | t!("??") => Some(BindingPower::Nullish),
			_ => None,
		}
	}

	fn prefix_binding_power(&mut self, token: TokenKind) -> Option<BindingPower> {
		match token {
			t!("!") | t!("+") | t!("-") => Some(BindingPower::Prefix),
			t!("..") => Some(BindingPower::Range),
			t!("<") => {
				let peek = self.peek1();
				if matches!(peek.kind, t!("-") | t!("->") | t!("FUTURE")) {
					return None;
				}
				Some(BindingPower::Prefix)
			}
			_ => None,
		}
	}

	fn postfix_binding_power(&mut self, token: TokenKind) -> Option<BindingPower> {
		match token {
			t!(">") if self.peek_whitespace1().kind == t!("..") => Some(BindingPower::Range),
			t!("..") => Some(BindingPower::Range),
			t!("(") => Some(BindingPower::Call),
			_ => None,
		}
	}

	async fn parse_prefix_op(&mut self, ctx: &mut Stk, min_bp: BindingPower) -> ParseResult<Expr> {
		let token = self.peek();
		let operator = match token.kind {
			t!("+") => {
				// +123 is a single number token, so parse it as such
				let p = self.peek_whitespace1();
				if matches!(p.kind, TokenKind::Digits) {
					// This is a bit of an annoying special case.
					// The problem is that `+` and `-` can be an prefix operator and a the start
					// of a number token.
					// To figure out which it is we need to peek the next whitespace token,
					// This eats the digits that the lexer needs to lex the number. So we we need
					// to backup before the digits token was consumed, clear the digits token from
					// the token buffer so it isn't popped after parsing the number and then lex the
					// number.
					self.lexer.backup_before(p.span);
					self.token_buffer.clear();
					self.token_buffer.push(token);
					let number = self.next_token_value::<Number>().map(Value::Number)?;
					if self.peek_continues_idiom() {
						return self
							.parse_remaining_value_idiom(ctx, vec![Part::Start(number)])
							.await;
					} else {
						return Ok(number);
					}
				}
				self.pop_peek();

				PrefixOperator::Positive
			}
			t!("-") => {
				// -123 is a single number token, so parse it as such
				let p = self.peek_whitespace1();
				if matches!(p.kind, TokenKind::Digits) {
					// This is a bit of an annoying special case.
					// The problem is that `+` and `-` can be an prefix operator and a the start
					// of a number token.
					// To figure out which it is we need to peek the next whitespace token,
					// This eats the digits that the lexer needs to lex the number. So we we need
					// to backup before the digits token was consumed, clear the digits token from
					// the token buffer so it isn't popped after parsing the number and then lex the
					// number.
					self.lexer.backup_before(p.span);
					self.token_buffer.clear();
					self.token_buffer.push(token);
					let number = self.next_token_value::<Number>().map(SqlValue::Number)?;
					if self.peek_continues_idiom() {
						return self
							.parse_remaining_value_idiom(ctx, vec![Part::Start(number)])
							.await;
					} else {
						return Ok(number);
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
				let kind = self.parse_kind(ctx, token.span).await?;
				PrefixOperator::Cast(kind)
			}
			t!("..") => {
				if self.peek_whitespace().kind == t!("=") {
					self.pop_peek();
					PrefixOperator::RangeInclusive
				} else {
					PrefixOperator::Range
				}
			}
			// should be unreachable as we previously check if the token was a prefix op.
			_ => unreachable!(),
		};

		let v = ctx.run(|ctx| self.pratt_parse_expr(ctx, min_bp)).await?;

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
				TokenKind::Digits | TokenKind::Glued(token::Glued::Number) => {
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
		self.expect_closing_delimiter(t!("|>"), token.span)?;
		Ok(res)
	}

	fn operator_is_relation(operator: &BinaryOperator) -> bool {
		matches!(
			operator,
			BinaryOperator::Equal
				| BinaryOperator::NotEqual
				| BinaryOperator::AllEqual
				| BinaryOperator::AnyEqual
				| BinaryOperator::Contain
				| BinaryOperator::NotContain
				| BinaryOperator::NotInside
				| BinaryOperator::ContainAll
				| BinaryOperator::ContainNone
				| BinaryOperator::AllInside
				| BinaryOperator::AnyInside
				| BinaryOperator::NoneInside
				| BinaryOperator::Outside
				| BinaryOperator::Intersects
				| BinaryOperator::Inside
				| BinaryOperator::NearestNeighbor(_)
		)
	}

	fn expr_is_relation(expr: &Expr) -> bool {
		match expr {
			Expr::Binary {
				op,
				..
			} => Self::operator_is_relation(op),
			_ => false,
		}
	}

	fn expr_is_range(expr: &Expr) -> bool {
		//TODO(EXPR): Prefix and Postfix range
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
		ctx: &mut Stk,
		min_bp: BindingPower,
		lhs: Expr,
		lhs_prime: bool, // if lhs was a prime expression, required for ensuring (a..b)..c does not
		                 // fail.
	) -> ParseResult<Expr> {
		let token = self.next();
		let operator = match token.kind {
			// TODO: change operator name?
			t!("||") | t!("OR") => BinaryOperator::Or,
			t!("&&") | t!("AND") => BinaryOperator::And,
			t!("??") => BinaryOperator::NullCoalescing,
			t!("==") => BinaryOperator::ExactEqual,
			t!("!=") => BinaryOperator::NotEqual,
			t!("*=") => BinaryOperator::AllEqual,
			t!("?=") => BinaryOperator::AnyEqual,
			t!("=") => BinaryOperator::Equal,
			t!("@") => {
				let reference = (!self.eat(t!("@")))
					.then(|| {
						let number = self.next_token_value()?;
						expected!(self, t!("@"));
						Ok(number)
					})
					.transpose()?;
				BinaryOperator::Matches(reference)
			}
			t!("<=") => BinaryOperator::LessThanEqual,
			t!("<") => BinaryOperator::LessThan,
			t!(">=") => BinaryOperator::MoreThanEqual,
			t!("**") => BinaryOperator::Power,
			t!("+") => BinaryOperator::Add,
			t!("-") => BinaryOperator::Subtract,
			t!("*") | t!("×") => BinaryOperator::Multiply,
			t!("/") | t!("÷") => BinaryOperator::Divide,
			t!("%") => BinaryOperator::Modulo,
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
				if self.peek_whitespace().kind == t!("..") {
					self.pop_peek();
					if self.peek_whitespace().kind == t!("=") {
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
				if self.peek_whitespace().kind == t!("=") {
					self.pop_peek();
					BinaryOperator::RangeInclusive
				} else {
					BinaryOperator::Range
				}
			}

			// should be unreachable as we previously check if the token was a prefix op.
			x => unreachable!("found non-operator token {x:?}"),
		};
		let before = self.recent_span();

		let is_relation = Self::operator_is_relation(&operator);
		if !lhs_prime && is_relation && Self::expr_is_relation(&lhs) {
			bail!("Chained relational operators have no defined associativity.",
				@span => "Use parens, '()', to specify which operator must be evaluated first")
		}

		let is_range = matches!(
			operator,
			BinaryOperator::Range
				| BinaryOperator::RangeSkipInclusive
				| BinaryOperator::RangeSkip
				| BinaryOperator::RangeInclusive
		);
		if !lhs_prime && is_range && Self::expr_is_range(&lhs) {
			bail!("Chained range operators has no specified associativity",
				@span => "use parens, '()', to specify which operator must be evaluated first")
		}

		let rhs_covered = self.peek().kind == t!("(");
		let rhs = ctx.run(|ctx| self.pratt_parse_expr(ctx, min_bp)).await?;

		if !rhs_covered && is_relation && Self::expr_is_relation(&rhs) {
			bail!("Chained relational operators have no defined associativity.",
				@span => "Use parens, '()', to specify which operator must be evaluated first")
		}

		if !rhs_covered && is_range && Self::expr_is_range(&rhs) {
			bail!("Chained range operators have no defined associativity.",
				@span => "Use parens, '()', to specify which operator must be evaluated first")
		}

		Ok(Expr::Binary {
			left: Box::new(lhs),
			op,
			right: Box::new(rhs),
		})
	}

	async fn parse_infix_range(
		&mut self,
		ctx: &mut Stk,
		exclusive: bool,
		lhs: Expr,
		lhs_prime: bool,
	) -> ParseResult<Expr> {
		let inclusive = self.eat_whitespace(t!("="));

		let before = self.recent_span();
		let peek = self.peek_whitespace();
		let (rhs, rhs_covered) = if inclusive {
			// ..= must be followed by an expression.
			if peek.kind == TokenKind::WhiteSpace {
				bail!("Unexpected whitespace, expected inclusive range to be immediately followed by a expression",
					@peek.span => "Whitespace between a range and it's operands is dissallowed")
			}
			let rhs_covered = self.peek().kind == t!("(");
			(ctx.run(|ctx| self.pratt_parse_expr(ctx, BindingPower::Range)).await?, rhs_covered)
		} else if Self::kind_starts_expression(peek.kind) {
			let rhs_covered = self.peek().kind == t!("(");
			(ctx.run(|ctx| self.pratt_parse_expr(ctx, BindingPower::Range)).await?, rhs_covered)
		} else {
			todo!()
			/*
			return Ok(Box::new(Range {
				beg: if exclusive {
					Bound::Excluded(lhs)
				} else {
					Bound::Included(lhs)
				},
				end: Bound::Unbounded,
			})));
			*/
		};

		if Self::expr_is_range(&lhs) && !lhs_prime {
			let span = before.covers(self.recent_span());
			// a..b..c is ambiguous, so throw an error
			bail!("Chaining range operators has no specified associativity",
				@span => "use parens, '()', to specify which operator must be evaluated first")
		}

		if Self::expr_is_range(&rhs) && !rhs_covered {
			let span = before.covers(self.recent_span());
			// a..b..c is ambiguous, so throw an error
			bail!("Chaining range operators has no specified associativity",
				@span => "use parens, '()', to specify which operator must be evaluated first")
		}

		let op = if exclusive {
			if inclusive {
				BinaryOperator::RangeSkipInclusive
			} else {
				BinaryOperator::RangeSkip
			}
		} else {
			if inclusive {
				BinaryOperator::RangeInclusive
			} else {
				BinaryOperator::Range
			}
		};

		Ok(Expr::Binary {
			left: Box::new(lhs),
			op,
			right: Box::new(rhs),
		})
	}

	async fn parse_postfix(
		&mut self,
		ctx: &mut Stk,
		lhs: Expr,
		lhs_prime: bool,
	) -> ParseResult<Expr> {
		let token = self.next();
		let op = match token.kind {
			t!(">") => {
				assert!(self.eat_whitespace(t!("..")));
				if !lhs_prime && Self::expr_is_range(&lhs) {
					bail!("Chaining range operators has no specified associativity",
						@span => "use parens, '()', to specify which operator must be evaluated first")
				}
				PostfixOperator::RangeSkip
			}
			t!("..") => {
				if !lhs_prime && Self::expr_is_range(&lhs) {
					bail!("Chaining range operators has no specified associativity",
						@span => "use parens, '()', to specify which operator must be evaluated first")
				}
				PostfixOperator::Range
			}
			t!("(") => {
				self.pop_peek();
				let mut args = Vec::new();
				loop {
					if self.eat(t!(")")) {
						break;
					}

					let arg = ctx.run(|ctx| self.parse_expr_inherit(ctx)).await?;
					args.push(arg);

					if !self.eat(t!(",")) {
						self.expect_closing_delimiter(t!(")"), token.span)?;
						break;
					}
				}
				PostfixOperator::Call(args)
			}
			// should be unreachable as we previously check if the token was a postfix op.
			x => unreachable!("found non-operator token {x:?}"),
		};

		Ok(Expr::Postfix {
			expr: Box::new(lhs),
			op,
		})
	}

	/// The pratt parsing loop.
	/// Parses expression according to binding power.
	async fn pratt_parse_expr(&mut self, ctx: &mut Stk, min_bp: BindingPower) -> ParseResult<Expr> {
		let peek = self.peek();
		let (mut lhs, mut lhs_prime) = if let Some(bp) = self.prefix_binding_power(peek.kind) {
			(self.parse_prefix_op(ctx, bp).await?, false)
		} else {
			(self.parse_prime_expr(ctx).await?, true)
		};

		loop {
			let token = self.peek();

			if let Some(bp) = self.postfix_binding_power(token.kind) {
				if bp <= min_bp {
					break;
				}

				lhs = self.parse_postfix(ctx, lhs, lhs_prime).await?;
				lhs_prime = false;
				continue;
			}

			// explain that assignment operators can't be used in normal expressions.
			if let t!("+=") | t!("*=") | t!("-=") | t!("+?=") = token.kind {
				unexpected!(self,token,"an operator",
					=> "assignment operators are only allowed in SET and DUPLICATE KEY UPDATE clauses")
			}

			let Some(bp) = self.infix_binding_power(token.kind) else {
				break;
			};

			if bp <= min_bp {
				break;
			}

			lhs = self.parse_infix_op(ctx, bp, lhs, lhs_prime).await?;
			lhs_prime = false;
		}

		Ok(lhs)
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use crate::sql::{Block, Future, Kind};
	use crate::syn::Parse;

	//TODO(EXPR)
	/*
	#[test]
	fn cast_int() {
		let sql = "<int>1.2345";
		let out = SqlValue::parse(sql);
		assert_eq!("<int> 1.2345f", format!("{}", out));
		assert_eq!(out, SqlValue::from(Cast(Kind::Int, 1.2345.into())));
	}

	#[test]
	fn cast_string() {
		let sql = "<string>1.2345";
		let out = SqlValue::parse(sql);
		assert_eq!("<string> 1.2345f", format!("{}", out));
		assert_eq!(out, SqlValue::from(Cast(Kind::String, 1.2345.into())));
	}

	#[test]
	fn expression_statement() {
		let sql = "true AND false";
		let out = SqlValue::parse(sql);
		assert_eq!("true AND false", format!("{}", out));
	}

	#[test]
	fn expression_left_opened() {
		let sql = "3 * 3 * 3 = 27";
		let out = SqlValue::parse(sql);
		assert_eq!("3 * 3 * 3 = 27", format!("{}", out));
	}

	#[test]
	fn expression_left_closed() {
		let sql = "(3 * 3 * 3) = 27";
		let out = SqlValue::parse(sql);
		assert_eq!("3 * 3 * 3 = 27", format!("{}", out));
	}

	#[test]
	fn expression_right_opened() {
		let sql = "27 = 3 * 3 * 3";
		let out = SqlValue::parse(sql);
		assert_eq!("27 = 3 * 3 * 3", format!("{}", out));
	}

	#[test]
	fn expression_right_closed() {
		let sql = "27 = (3 * 3 * 3)";
		let out = SqlValue::parse(sql);
		assert_eq!("27 = 3 * 3 * 3", format!("{}", out));
	}

	#[test]
	fn expression_both_opened() {
		let sql = "3 * 3 * 3 = 3 * 3 * 3";
		let out = SqlValue::parse(sql);
		assert_eq!("3 * 3 * 3 = 3 * 3 * 3", format!("{}", out));
	}

	#[test]
	fn expression_both_closed() {
		let sql = "(3 * 3 * 3) = (3 * 3 * 3)";
		let out = SqlValue::parse(sql);
		assert_eq!("3 * 3 * 3 = 3 * 3 * 3", format!("{}", out));
	}

	#[test]
	fn expression_closed_required() {
		let sql = "(3 + 3) * 3";
		let out = SqlValue::parse(sql);
		assert_eq!("(3 + 3) * 3", format!("{}", out));
	}

	#[test]
	fn range_closed_required() {
		let sql = "(1..2)..3";
		let out = SqlValue::parse(sql);
		assert_eq!("(1..2)..3", format!("{}", out));
	}

	#[test]
	fn expression_unary() {
		let sql = "-a";
		let out = SqlValue::parse(sql);
		assert_eq!(sql, format!("{}", out));
	}

	#[test]
	fn expression_with_unary() {
		let sql = "-(5) + 5";
		let out = SqlValue::parse(sql);
		assert_eq!("-5 + 5", format!("{}", out));
	}

	#[test]
	fn expression_left_associative() {
		let sql = "1 - 1 - 1";
		let out = SqlValue::parse(sql);
		let one = SqlValue::Number(Number::Int(1));
		let expected = SqlValue::Expression(Box::new(Expression::Binary {
			l: SqlValue::Expression(Box::new(Expression::Binary {
				l: one.clone(),
				o: Operator::Sub,
				r: one.clone(),
			})),
			o: Operator::Sub,
			r: one,
		}));
		assert_eq!(expected, out);
	}

	#[test]
	fn parse_expression() {
		let sql = "<future> { 5 + 10 }";
		let out = SqlValue::parse(sql);
		assert_eq!("<future> { 5 + 10 }", format!("{}", out));
		assert_eq!(
			out,
			SqlValue::from(Future(Block::from(SqlValue::from(Expression::Binary {
				l: SqlValue::Number(Number::Int(5)),
				o: Operator::Add,
				r: SqlValue::Number(Number::Int(10))
			}))))
		);
	}
	*/
}
