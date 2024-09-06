//! This module defines the pratt parser for operators.

use std::ops::Bound;

use reblessive::Stk;

use super::mac::{expected_whitespace, unexpected};
use crate::sql::Range;
use crate::sql::{value::TryNeg, Cast, Expression, Number, Operator, Value};
use crate::syn::error::bail;
use crate::syn::token::{self, Token};
use crate::syn::{
	parser::{mac::expected, ParseResult, Parser},
	token::{t, TokenKind},
};

/// An enum which defines how strong a operator binds it's operands.
///
/// If a binding power is higher the operator is more likely to directly operate on it's
/// neighbours.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub enum BindingPower {
	Base,
	Range,
	Or,
	And,
	Equality,
	Relation,
	AddSub,
	MulDiv,
	Power,
	Nullish,
	Unary,
	Cast,
}

impl Parser<'_> {
	/// Parsers a generic value.
	///
	/// A generic loose ident like `foo` in for example `foo.bar` can be two different values
	/// depending on context: a table or a field the current document. This function parses loose
	/// idents as a table, see [`parse_value_field`] for parsing loose idents as fields
	pub async fn parse_value_class(&mut self, ctx: &mut Stk) -> ParseResult<Value> {
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
	pub async fn parse_value_field(&mut self, ctx: &mut Stk) -> ParseResult<Value> {
		let old = self.table_as_field;
		self.table_as_field = true;
		let res = self.pratt_parse_expr(ctx, BindingPower::Base).await;
		self.table_as_field = old;
		res
	}

	/// Parse a assigner operator.
	pub fn parse_assigner(&mut self) -> ParseResult<Operator> {
		let token = self.next();
		match token.kind {
			t!("=") => Ok(Operator::Equal),
			t!("+=") => Ok(Operator::Inc),
			t!("-=") => Ok(Operator::Dec),
			t!("+?=") => Ok(Operator::Ext),
			_ => unexpected!(self, token, "an assign operator"),
		}
	}

	/// Returns the binding power of an infix operator.
	///
	/// Binding power is the opposite of precendence: a higher binding power means that a token is
	/// more like to operate directly on it's neighbours. Example `*` has a higher binding power
	/// than `-` resulting in 1 - 2 * 3 being parsed as 1 - (2 * 3).
	///
	/// This returns two numbers: the binding power of the left neighbour and the right neighbour.
	/// If the left number is lower then the right it is left associative: i.e. '1 op 2 op 3' will
	/// be parsed as '(1 op 2) op 3'. If the right number is lower the operator is right
	/// associative: i.e. '1 op 2 op 3' will be parsed as '1 op (2 op 3)'. For example: `+=` is
	/// right associative so `a += b += 3` will be parsed as `a += (b += 3)` while `+` is left
	/// associative and will be parsed as `(a + b) + c`.
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
			t!("!") | t!("+") | t!("-") => Some(BindingPower::Unary),
			t!("..") => Some(BindingPower::Range),
			t!("<") => {
				let peek = self.peek1();
				if matches!(peek.kind, t!("-") | t!("->") | t!("FUTURE")) {
					return None;
				}
				Some(BindingPower::Cast)
			}
			_ => None,
		}
	}

	async fn parse_prefix_op(&mut self, ctx: &mut Stk, min_bp: BindingPower) -> ParseResult<Value> {
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
					return self.next_token_value::<Number>().map(Value::Number);
				}
				self.pop_peek();

				Operator::Add
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
					return self.next_token_value::<Number>().map(Value::Number);
				}

				self.pop_peek();

				Operator::Neg
			}
			t!("!") => {
				self.pop_peek();
				Operator::Not
			}
			t!("<") => {
				self.pop_peek();
				let kind = self.parse_kind(ctx, token.span).await?;
				let value = ctx.run(|ctx| self.pratt_parse_expr(ctx, BindingPower::Cast)).await?;
				let cast = Cast(kind, value);
				return Ok(Value::Cast(Box::new(cast)));
			}
			t!("..") => return self.parse_prefix_range(ctx).await,
			// should be unreachable as we previously check if the token was a prefix op.
			_ => unreachable!(),
		};

		let v = ctx.run(|ctx| self.pratt_parse_expr(ctx, min_bp)).await?;

		// HACK: For compatiblity with the old parser apply + and - operator immediately if the
		// left value is a number.
		if let Value::Number(number) = v {
			if let Operator::Neg = operator {
				// this can only panic if `number` is i64::MIN which currently can't be parsed.
				return Ok(Value::Number(number.try_neg().unwrap()));
			}

			if let Operator::Add = operator {
				// doesn't do anything.
				return Ok(Value::Number(number));
			}
			Ok(Value::Expression(Box::new(Expression::Unary {
				o: operator,
				v: Value::Number(number),
			})))
		} else {
			Ok(Value::Expression(Box::new(Expression::Unary {
				o: operator,
				v,
			})))
		}
	}

	pub fn parse_knn(&mut self, token: Token) -> ParseResult<Operator> {
		let amount = self.next_token_value()?;
		let op = if self.eat(t!(",")) {
			let token = self.peek();
			match token.kind {
				TokenKind::Distance(_) => {
					let d = self.parse_distance().map(Some)?;
					Operator::Knn(amount, d)
				}
				TokenKind::Digits | TokenKind::Glued(token::Glued::Number) => {
					let ef = self.next_token_value()?;
					Operator::Ann(amount, ef)
				}
				_ => {
					bail!("Unexpected token {} expected a distance of an integer", token.kind,
						@token.span => "The NN operator accepts either a distance or an EF value (integer)")
				}
			}
		} else {
			Operator::Knn(amount, None)
		};
		self.expect_closing_delimiter(t!("|>"), token.span)?;
		Ok(op)
	}

	fn expression_is_relation(value: &Value) -> bool {
		if let Value::Expression(x) = value {
			return Self::operator_is_relation(x.operator());
		}
		false
	}

	fn operator_is_relation(operator: &Operator) -> bool {
		matches!(
			operator,
			Operator::Equal
				| Operator::NotEqual
				| Operator::AllEqual
				| Operator::AnyEqual
				| Operator::NotLike
				| Operator::AllLike
				| Operator::AnyLike
				| Operator::Like
				| Operator::Contain
				| Operator::NotContain
				| Operator::NotInside
				| Operator::ContainAll
				| Operator::ContainNone
				| Operator::AllInside
				| Operator::AnyInside
				| Operator::NoneInside
				| Operator::Outside
				| Operator::Intersects
				| Operator::Inside
				| Operator::Knn(_, _)
		)
	}

	async fn parse_infix_op(
		&mut self,
		ctx: &mut Stk,
		min_bp: BindingPower,
		lhs: Value,
	) -> ParseResult<Value> {
		let token = self.next();
		let operator = match token.kind {
			// TODO: change operator name?
			t!("||") | t!("OR") => Operator::Or,
			t!("&&") | t!("AND") => Operator::And,
			t!("?:") => Operator::Tco,
			t!("??") => Operator::Nco,
			t!("==") => Operator::Exact,
			t!("!=") => Operator::NotEqual,
			t!("*=") => Operator::AllEqual,
			t!("?=") => Operator::AnyEqual,
			t!("=") => Operator::Equal,
			t!("!~") => Operator::NotLike,
			t!("*~") => Operator::AllLike,
			t!("?~") => Operator::AnyLike,
			t!("~") => Operator::Like,
			t!("@") => {
				let reference = (!self.eat(t!("@")))
					.then(|| {
						let number = self.next_token_value()?;
						expected!(self, t!("@"));
						Ok(number)
					})
					.transpose()?;
				Operator::Matches(reference)
			}
			t!("<=") => Operator::LessThanOrEqual,
			t!("<") => Operator::LessThan,
			t!(">=") => Operator::MoreThanOrEqual,
			t!("**") => Operator::Pow,
			t!("+") => Operator::Add,
			t!("-") => Operator::Sub,
			t!("*") | t!("×") => Operator::Mul,
			t!("/") | t!("÷") => Operator::Div,
			t!("%") => Operator::Rem,
			t!("∋") | t!("CONTAINS") => Operator::Contain,
			t!("∌") | t!("CONTAINSNOT") => Operator::NotContain,
			t!("∈") | t!("INSIDE") => Operator::Inside,
			t!("∉") | t!("NOTINSIDE") => Operator::NotInside,
			t!("⊇") | t!("CONTAINSALL") => Operator::ContainAll,
			t!("⊃") | t!("CONTAINSANY") => Operator::ContainAny,
			t!("⊅") | t!("CONTAINSNONE") => Operator::ContainNone,
			t!("⊆") | t!("ALLINSIDE") => Operator::AllInside,
			t!("⊂") | t!("ANYINSIDE") => Operator::AnyInside,
			t!("⊄") | t!("NONEINSIDE") => Operator::NoneInside,
			t!("IS") => {
				if self.eat(t!("NOT")) {
					Operator::NotEqual
				} else {
					Operator::Equal
				}
			}
			t!("OUTSIDE") => Operator::Outside,
			t!("INTERSECTS") => Operator::Intersects,
			t!("NOT") => {
				expected!(self, t!("IN"));
				Operator::NotInside
			}
			t!("IN") => Operator::Inside,
			t!("<|") => self.parse_knn(token)?,

			t!(">") => {
				if self.peek_whitespace().kind == t!("..") {
					return self.parse_infix_range(ctx, true, lhs).await;
				}
				Operator::MoreThan
			}
			t!("..") => {
				return self.parse_infix_range(ctx, false, lhs).await;
			}

			// should be unreachable as we previously check if the token was a prefix op.
			x => unreachable!("found non-operator token {x:?}"),
		};
		let before = self.recent_span();
		let rhs = ctx.run(|ctx| self.pratt_parse_expr(ctx, min_bp)).await?;

		if Self::operator_is_relation(&operator) && Self::expression_is_relation(&rhs) {
			let span = before.covers(self.recent_span());
			// 1 >= 2 >= 3 has no defined associativity and is often a mistake.
			bail!("Chaining relational operators have no defined associativity.",
				@span => "Use parens, '()', to specify which expression must be evaluated first")
		}

		Ok(Value::Expression(Box::new(Expression::Binary {
			l: lhs,
			o: operator,
			r: rhs,
		})))
	}

	async fn parse_infix_range(
		&mut self,
		ctx: &mut Stk,
		exclusive: bool,
		lhs: Value,
	) -> ParseResult<Value> {
		expected_whitespace!(self, t!(".."));
		let inclusive = self.eat_whitespace(t!("="));

		let before = self.recent_span();
		let peek = self.peek_whitespace();
		let rhs = if inclusive {
			// ..= must be followed by an expression.
			if peek.kind == TokenKind::WhiteSpace {
				bail!("Unexpected whitespace, expected inclusive range to be immediately followed by a expression",
					@peek.span => "Whitespace between a range and it's operands is dissallowed")
			}
			ctx.run(|ctx| self.pratt_parse_expr(ctx, BindingPower::Range)).await?
		} else if peek.kind != TokenKind::WhiteSpace {
			ctx.run(|ctx| self.pratt_parse_expr(ctx, BindingPower::Range)).await?
		} else {
			return Ok(Value::Range(Box::new(Range {
				beg: if exclusive {
					Bound::Excluded(lhs)
				} else {
					Bound::Included(lhs)
				},
				end: Bound::Unbounded,
			})));
		};

		if matches!(rhs, Value::Range(_)) {
			let span = before.covers(self.recent_span());
			// a..b..c is ambigous, so throw an error
			bail!("Chaining range operators has no specified associativity",
				@span => "use parens, '()', to specify which expression must be evaluated first")
		}

		Ok(Value::Range(Box::new(Range {
			beg: if exclusive {
				Bound::Excluded(lhs)
			} else {
				Bound::Included(lhs)
			},
			end: if inclusive {
				Bound::Included(rhs)
			} else {
				Bound::Excluded(rhs)
			},
		})))
	}

	async fn parse_prefix_range(&mut self, ctx: &mut Stk) -> ParseResult<Value> {
		expected_whitespace!(self, t!(".."));
		let inclusive = self.eat_whitespace(t!("="));
		let before = self.recent_span();
		let peek = self.peek_whitespace();
		let rhs = if inclusive {
			// ..= must be followed by an expression.
			if peek.kind == TokenKind::WhiteSpace {
				bail!("Unexpected whitespace, expected inclusive range to be immediately followed by a expression",
					@peek.span => "Whitespace between a range and it's operands is dissallowed")
			}
			ctx.run(|ctx| self.pratt_parse_expr(ctx, BindingPower::Range)).await?
		} else if peek.kind != TokenKind::WhiteSpace {
			ctx.run(|ctx| self.pratt_parse_expr(ctx, BindingPower::Range)).await?
		} else {
			return Ok(Value::Range(Box::new(Range {
				beg: Bound::Unbounded,
				end: Bound::Unbounded,
			})));
		};

		if matches!(rhs, Value::Range(_)) {
			let span = before.covers(self.recent_span());
			// a..b..c is ambigous, so throw an error
			bail!("Chaining range operators has no specified associativity",
						@span => "use parens, '()', to specify which expression must be evaluated first")
		}

		let range = Range {
			beg: Bound::Unbounded,
			end: if inclusive {
				Bound::Included(rhs)
			} else {
				Bound::Excluded(rhs)
			},
		};
		Ok(Value::Range(Box::new(range)))
	}

	/// The pratt parsing loop.
	/// Parses expression according to binding power.
	async fn pratt_parse_expr(
		&mut self,
		ctx: &mut Stk,
		min_bp: BindingPower,
	) -> ParseResult<Value> {
		let peek = self.peek();
		let mut lhs = if let Some(bp) = self.prefix_binding_power(peek.kind) {
			self.parse_prefix_op(ctx, bp).await?
		} else {
			self.parse_idiom_expression(ctx).await?
		};

		loop {
			let token = self.peek();
			let Some(bp) = self.infix_binding_power(token.kind) else {
				// explain that assignment operators can't be used in normal expressions.
				if let t!("+=") | t!("*=") | t!("-=") | t!("+?=") = token.kind {
					unexpected!(self,token,"an operator",
						=> "assignment operators are only allowed in SET and DUPLICATE KEY UPDATE clauses")
				}
				break;
			};

			if bp < min_bp {
				break;
			}

			lhs = self.parse_infix_op(ctx, bp, lhs).await?;
		}

		Ok(lhs)
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use crate::sql::{Block, Future, Kind};
	use crate::syn::Parse;

	#[test]
	fn cast_int() {
		let sql = "<int>1.2345";
		let out = Value::parse(sql);
		assert_eq!("<int> 1.2345f", format!("{}", out));
		assert_eq!(out, Value::from(Cast(Kind::Int, 1.2345.into())));
	}

	#[test]
	fn cast_string() {
		let sql = "<string>1.2345";
		let out = Value::parse(sql);
		assert_eq!("<string> 1.2345f", format!("{}", out));
		assert_eq!(out, Value::from(Cast(Kind::String, 1.2345.into())));
	}

	#[test]
	fn expression_statement() {
		let sql = "true AND false";
		let out = Value::parse(sql);
		assert_eq!("true AND false", format!("{}", out));
	}

	#[test]
	fn expression_left_opened() {
		let sql = "3 * 3 * 3 = 27";
		let out = Value::parse(sql);
		assert_eq!("3 * 3 * 3 = 27", format!("{}", out));
	}

	#[test]
	fn expression_left_closed() {
		let sql = "(3 * 3 * 3) = 27";
		let out = Value::parse(sql);
		assert_eq!("(3 * 3 * 3) = 27", format!("{}", out));
	}

	#[test]
	fn expression_right_opened() {
		let sql = "27 = 3 * 3 * 3";
		let out = Value::parse(sql);
		assert_eq!("27 = 3 * 3 * 3", format!("{}", out));
	}

	#[test]
	fn expression_right_closed() {
		let sql = "27 = (3 * 3 * 3)";
		let out = Value::parse(sql);
		assert_eq!("27 = (3 * 3 * 3)", format!("{}", out));
	}

	#[test]
	fn expression_both_opened() {
		let sql = "3 * 3 * 3 = 3 * 3 * 3";
		let out = Value::parse(sql);
		assert_eq!("3 * 3 * 3 = 3 * 3 * 3", format!("{}", out));
	}

	#[test]
	fn expression_both_closed() {
		let sql = "(3 * 3 * 3) = (3 * 3 * 3)";
		let out = Value::parse(sql);
		assert_eq!("(3 * 3 * 3) = (3 * 3 * 3)", format!("{}", out));
	}

	#[test]
	fn expression_unary() {
		let sql = "-a";
		let out = Value::parse(sql);
		assert_eq!(sql, format!("{}", out));
	}

	#[test]
	fn expression_with_unary() {
		let sql = "-(5) + 5";
		let out = Value::parse(sql);
		assert_eq!(sql, format!("{}", out));
	}

	#[test]
	fn parse_expression() {
		let sql = "<future> { 5 + 10 }";
		let out = Value::parse(sql);
		assert_eq!("<future> { 5 + 10 }", format!("{}", out));
		assert_eq!(
			out,
			Value::from(Future(Block::from(Value::from(Expression::Binary {
				l: Value::Number(Number::Int(5)),
				o: Operator::Add,
				r: Value::Number(Number::Int(10))
			}))))
		);
	}
}
