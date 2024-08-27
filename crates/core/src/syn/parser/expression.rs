//! This module defines the pratt parser for operators.

use reblessive::Stk;

use super::mac::unexpected;
use super::ParseError;
use crate::sql::{value::TryNeg, Cast, Expression, Number, Operator, Value};
use crate::syn::token::Token;
use crate::syn::{
	parser::{mac::expected, ParseErrorKind, ParseResult, Parser},
	token::{t, TokenKind},
};

impl Parser<'_> {
	/// Parsers a generic value.
	///
	/// A generic loose ident like `foo` in for example `foo.bar` can be two different values
	/// depending on context: a table or a field the current document. This function parses loose
	/// idents as a table, see [`parse_value_field`] for parsing loose idents as fields
	pub async fn parse_value(&mut self, ctx: &mut Stk) -> ParseResult<Value> {
		let old = self.table_as_field;
		self.table_as_field = false;
		let res = self.pratt_parse_expr(ctx, 0).await;
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
		let res = self.pratt_parse_expr(ctx, 0).await;
		self.table_as_field = old;
		res
	}

	/// Parse a assigner operator.
	pub fn parse_assigner(&mut self) -> ParseResult<Operator> {
		match self.next().kind {
			t!("=") => Ok(Operator::Equal),
			t!("+=") => Ok(Operator::Inc),
			t!("-=") => Ok(Operator::Dec),
			t!("+?=") => Ok(Operator::Ext),
			x => unexpected!(self, x, "an assign operator"),
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
	fn infix_binding_power(token: TokenKind) -> Option<(u8, u8)> {
		// TODO: Look at ordering of operators.
		match token {
			// assigment operators have the lowest binding power.
			//t!("+=") | t!("-=") | t!("+?=") => Some((2, 1)),
			t!("||") | t!("OR") => Some((3, 4)),
			t!("&&") | t!("AND") => Some((5, 6)),

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
			| t!("@") => Some((7, 8)),

			t!("<")
			| t!("<=")
			| t!(">")
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
			| t!("<|") => Some((9, 10)),

			t!("+") | t!("-") => Some((11, 12)),
			t!("*") | t!("×") | t!("/") | t!("÷") | t!("%") => Some((13, 14)),
			t!("**") => Some((15, 16)),
			t!("?:") | t!("??") => Some((17, 18)),
			_ => None,
		}
	}

	fn prefix_binding_power(&mut self, token: TokenKind) -> Option<((), u8)> {
		match token {
			t!("!") | t!("+") | t!("-") => Some(((), 19)),
			t!("<") => {
				if self.peek_token_at(1).kind != t!("FUTURE") {
					Some(((), 20))
				} else {
					None
				}
			}
			_ => None,
		}
	}

	async fn parse_prefix_op(&mut self, ctx: &mut Stk, min_bp: u8) -> ParseResult<Value> {
		let token = self.peek();
		let operator = match token.kind {
			t!("+") => {
				// +123 is a single number token, so parse it as such
				let p = self.peek_whitespace_token_at(1);
				if matches!(p.kind, TokenKind::Digits) {
					return self.next_token_value::<Number>().map(Value::Number);
				}
				self.pop_peek();

				Operator::Add
			}
			t!("-") => {
				// -123 is a single number token, so parse it as such
				let p = self.peek_whitespace_token_at(1);
				if matches!(p.kind, TokenKind::Digits) {
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
				let value = ctx.run(|ctx| self.pratt_parse_expr(ctx, min_bp)).await?;
				let cast = Cast(kind, value);
				return Ok(Value::Cast(Box::new(cast)));
			}
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
			match self.peek_kind(){
				TokenKind::Distance(ref k) => {
					self.pop_peek();
					let d = self.convert_distance(k).map(Some)?;
					Operator::Knn(amount, d)
				},
				TokenKind::Digits | TokenKind::Number(_) => {
					let ef = self.next_token_value()?;
					Operator::Ann(amount, ef)
				}
				_ => {
					return Err(ParseError::new(
						ParseErrorKind::UnexpectedExplain {
							found: token.kind,
							expected: "a distance or an integer",
							explain: "The NN operator accepts either a distance for brute force operation, or an EF value for approximate operations",
						},
						token.span,
					))
				}
			}
		} else {
			Operator::Knn(amount, None)
		};
		self.expect_closing_delimiter(t!("|>"), token.span)?;
		Ok(op)
	}

	async fn parse_infix_op(
		&mut self,
		ctx: &mut Stk,
		min_bp: u8,
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
			t!(">") => Operator::MoreThan,
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

			// should be unreachable as we previously check if the token was a prefix op.
			x => unreachable!("found non-operator token {x:?}"),
		};
		let rhs = ctx.run(|ctx| self.pratt_parse_expr(ctx, min_bp)).await?;
		Ok(Value::Expression(Box::new(Expression::Binary {
			l: lhs,
			o: operator,
			r: rhs,
		})))
	}

	/// The pratt parsing loop.
	/// Parses expression according to binding power.
	async fn pratt_parse_expr(&mut self, ctx: &mut Stk, min_bp: u8) -> ParseResult<Value> {
		let peek = self.peek();
		let mut lhs = if let Some(((), r_bp)) = self.prefix_binding_power(peek.kind) {
			self.parse_prefix_op(ctx, r_bp).await?
		} else {
			self.parse_idiom_expression(ctx).await?
		};

		loop {
			let token = self.peek();
			let Some((l_bp, r_bp)) = Self::infix_binding_power(token.kind) else {
				// explain that assignment operators can't be used in normal expressions.
				if let t!("+=") | t!("*=") | t!("-=") | t!("+?=") = token.kind {
					return Err(ParseError::new(
							    ParseErrorKind::UnexpectedExplain {
								    found: token.kind,
								    expected: "an operator",
								    explain: "assignement operator are only allowed in SET and DUPLICATE KEY UPDATE statements",
							    },
							    token.span,
						    ));
				}
				break;
			};

			if l_bp < min_bp {
				break;
			}

			lhs = self.parse_infix_op(ctx, r_bp, lhs).await?;
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
