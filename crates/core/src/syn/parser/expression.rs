//! This module defines the pratt parser for operators.

use reblessive::Stk;

use super::enter_query_recursion;
use super::mac::unexpected;
use crate::sql::operator::{BindingPower, BooleanOperator, MatchesOperator, NearestNeighbor};
use crate::sql::{BinaryOperator, Expr, Ident, Literal, Part, PostfixOperator, PrefixOperator};
use crate::syn::error::bail;
use crate::syn::lexer::compound::Numeric;
use crate::syn::parser::mac::expected;
use crate::syn::parser::{ParseResult, Parser};
use crate::syn::token::{self, Glued, Token, TokenKind, t};
use crate::val;

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
	/// [`parse_expr_field`] for parsing loose idents as fields
	pub(crate) async fn parse_expr_table(&mut self, stk: &mut Stk) -> ParseResult<Expr> {
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
	/// [`parse_value`] for parsing loose idents as table
	pub(crate) async fn parse_expr_field(&mut self, stk: &mut Stk) -> ParseResult<Expr> {
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
		// TODO: Look at ordering of operators.
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
				let peek = self.peek_whitespace1();
				if matches!(peek.kind, t!("-") | t!("~") | t!("->") | t!("..")) {
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
				if matches!(peek.kind, t!("-") | t!("~") | t!("->")) {
					return None;
				}
				Some(BindingPower::Prefix)
			}
			_ => None,
		}
	}

	fn postfix_binding_power(&mut self, token: TokenKind) -> Option<BindingPower> {
		match token {
			t!(">") => {
				if self.peek_whitespace1().kind != t!("..") {
					return None;
				}

				let peek2 = self.peek_whitespace2().kind;
				if peek2 == t!("=") || Self::kind_starts_expression(peek2) {
					return None;
				}

				Some(BindingPower::Range)
			}
			t!("..") => match self.peek_whitespace1().kind {
				t!("=") => None,
				x if Self::kind_starts_expression(x) => None,
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
					let expr = match self.next_token_value::<Numeric>()? {
						Numeric::Float(f) => Expr::Literal(Literal::Float(f)),
						Numeric::Integer(i) => Expr::Literal(Literal::Integer(i)),
						Numeric::Decimal(d) => Expr::Literal(Literal::Decimal(d)),
						Numeric::Duration(d) => Expr::Prefix {
							op: PrefixOperator::Positive,
							expr: Box::new(Expr::Literal(Literal::Duration(val::Duration(d)))),
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
					let expr = match self.next_token_value::<Numeric>()? {
						Numeric::Float(f) => Expr::Literal(Literal::Float(f)),
						Numeric::Integer(i) => Expr::Literal(Literal::Integer(i)),
						Numeric::Decimal(d) => Expr::Literal(Literal::Decimal(d)),
						Numeric::Duration(d) => Expr::Prefix {
							op: PrefixOperator::Negate,
							expr: Box::new(Expr::Literal(Literal::Duration(val::Duration(d)))),
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
				if self.peek_whitespace().kind == t!("=") {
					self.pop_peek();
					PrefixOperator::RangeInclusive
				} else {
					if !Self::kind_starts_prime_value(self.peek_whitespace().kind) {
						// unbounded range.
						return Ok(Expr::Literal(Literal::UnboundedRange));
					}
					PrefixOperator::Range
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
		stk: &mut Stk,
		min_bp: BindingPower,
		lhs: Expr,
		lhs_prime: bool, /* if lhs was a prime expression, required for ensuring (a..b)..c does
		                  * not fail. */
	) -> ParseResult<Expr> {
		let token = self.next();
		let operator = match token.kind {
			// TODO: change operator name?
			t!("||") | t!("OR") => BinaryOperator::Or,
			t!("&&") | t!("AND") => BinaryOperator::And,
			t!("?:") => BinaryOperator::TenaryCondition,
			t!("??") => BinaryOperator::NullCoalescing,
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
		let rhs_covered = self.peek().kind == t!("(");
		let rhs = stk.run(|ctx| self.pratt_parse_expr(ctx, min_bp)).await?;

		let is_relation = Self::operator_is_relation(&operator);
		if !lhs_prime && is_relation && Self::expr_is_relation(&lhs) {
			let span = before.covers(self.recent_span());
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
			let span = before.covers(self.recent_span());
			bail!("Chained range operators has no specified associativity",
				@span => "use parens, '()', to specify which operator must be evaluated first")
		}

		if !rhs_covered && is_relation && Self::expr_is_relation(&rhs) {
			let span = before.covers(self.recent_span());
			bail!("Chained relational operators have no defined associativity.",
				@span => "Use parens, '()', to specify which operator must be evaluated first")
		}

		if !rhs_covered && is_range && Self::expr_is_range(&rhs) {
			let span = before.covers(self.recent_span());
			bail!("Chained range operators have no defined associativity.",
				@span => "Use parens, '()', to specify which operator must be evaluated first")
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
			TokenKind::Digits | TokenKind::Glued(Glued::Number) => {
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
				let name = self.next_token_value::<Ident>()?;
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
				PostfixOperator::MethodCall(name, args)
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
	async fn pratt_parse_expr(&mut self, stk: &mut Stk, min_bp: BindingPower) -> ParseResult<Expr> {
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

				lhs = self.parse_postfix(stk, lhs, lhs_prime).await?;
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

			lhs = self.parse_infix_op(stk, bp, lhs, lhs_prime).await?;
			lhs_prime = false;
		}

		Ok(lhs)
	}
}

#[cfg(test)]
mod test {
	use crate::sql::{BinaryOperator, Expr, Kind, Literal, PrefixOperator};
	use crate::syn;

	#[test]
	fn cast_int() {
		let sql = "<int>1.2345";
		let out = syn::expr(sql).unwrap();
		assert_eq!("<int> 1.2345f", format!("{}", out));
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
		let out = syn::expr(sql).unwrap();
		assert_eq!("<string> 1.2345f", format!("{}", out));
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
		let out = syn::expr(sql).unwrap();
		assert_eq!("true AND false", format!("{}", out));
	}

	#[test]
	fn expression_left_opened() {
		let sql = "3 * 3 * 3 = 27";
		let out = syn::expr(sql).unwrap();
		assert_eq!("3 * 3 * 3 = 27", format!("{}", out));
	}

	#[test]
	fn expression_left_closed() {
		let sql = "(3 * 3 * 3) = 27";
		let out = syn::expr(sql).unwrap();
		assert_eq!("3 * 3 * 3 = 27", format!("{}", out));
	}

	#[test]
	fn expression_right_opened() {
		let sql = "27 = 3 * 3 * 3";
		let out = syn::expr(sql).unwrap();
		assert_eq!("27 = 3 * 3 * 3", format!("{}", out));
	}

	#[test]
	fn expression_right_closed() {
		let sql = "27 = (3 * 3 * 3)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("27 = 3 * 3 * 3", format!("{}", out));
	}

	#[test]
	fn expression_both_opened() {
		let sql = "3 * 3 * 3 = 3 * 3 * 3";
		let out = syn::expr(sql).unwrap();
		assert_eq!("3 * 3 * 3 = 3 * 3 * 3", format!("{}", out));
	}

	#[test]
	fn expression_both_closed() {
		let sql = "(3 * 3 * 3) = (3 * 3 * 3)";
		let out = syn::expr(sql).unwrap();
		assert_eq!("3 * 3 * 3 = 3 * 3 * 3", format!("{}", out));
	}

	#[test]
	fn expression_closed_required() {
		let sql = "(3 + 3) * 3";
		let out = syn::expr(sql).unwrap();
		assert_eq!("(3 + 3) * 3", format!("{}", out));
	}

	#[test]
	fn range_closed_required() {
		let sql = "(1..2)..3";
		let out = syn::expr(sql).unwrap();
		assert_eq!("(1..2)..3", format!("{}", out));
	}

	#[test]
	fn expression_unary() {
		let sql = "-a";
		let out = syn::expr(sql).unwrap();
		assert_eq!(sql, format!("{}", out));
	}

	#[test]
	fn expression_with_unary() {
		let sql = "-(5) + 5";
		let out = syn::expr(sql).unwrap();
		assert_eq!("-5 + 5", format!("{}", out));
	}

	#[test]
	fn expression_left_associative() {
		let sql = "1 - 1 - 1";
		let out = syn::expr(sql).unwrap();
		let one = Expr::Literal(Literal::Integer(1));

		let expected = Expr::Binary {
			left: Box::new(Expr::Binary {
				left: Box::new(one.clone()),
				op: BinaryOperator::Subtract,
				right: Box::new(one.clone()),
			}),
			op: BinaryOperator::Subtract,
			right: Box::new(one.clone()),
		};
		assert_eq!(expected, out);
	}
}
