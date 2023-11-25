//! This module defines the pratt parser for operators.
use crate::sql::{value::TryNeg, Cast, Expression, Operator, Value};
use crate::syn::v2::{
	parser::{mac::expected, ParseResult, Parser},
	token::{t, TokenKind},
};

use super::mac::unexpected;

impl Parser<'_> {
	/// Parsers a generic value.
	///
	/// A generic loose ident like `foo` in for example `foo.bar` can be two different values
	/// depending on context: a table or a field the current document. This function parses loose
	/// idents as a table, see [`parse_value_field`] for parsing loose idents as fields
	pub fn parse_value(&mut self) -> ParseResult<Value> {
		let old = self.table_as_field;
		self.table_as_field = false;
		let res = self.pratt_parse_expr(0);
		self.table_as_field = old;
		res
	}

	/// Parsers a generic value.
	///
	/// A generic loose ident like `foo` in for example `foo.bar` can be two different values
	/// depending on context: a table or a field the current document. This function parses loose
	/// idents as a field, see [`parse_value`] for parsing loose idents as table
	pub fn parse_value_field(&mut self) -> ParseResult<Value> {
		let old = self.table_as_field;
		self.table_as_field = true;
		let res = self.pratt_parse_expr(0);
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
			// assigment operators have the lowes binding power.
			t!("+=") | t!("-=") | t!("+?=") => Some((2, 1)),

			t!("||") | t!("OR") => Some((3, 4)),
			t!("&&") | t!("AND") => Some((5, 6)),

			// Equality operators have same binding power.
			t!("=")
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
			| t!("IN") => Some((9, 10)),

			t!("+") | t!("-") => Some((11, 12)),
			t!("*") | t!("×") | t!("/") | t!("÷") => Some((13, 14)),
			t!("**") => Some((15, 16)),
			t!("?:") | t!("??") => Some((17, 18)),
			_ => None,
		}
	}

	fn prefix_binding_power(&mut self, token: TokenKind) -> Option<((), u8)> {
		match token {
			t!("!") | t!("+") | t!("-") => Some(((), 19)),
			t!("<") if self.peek_token_at(1).kind != t!("FUTURE") => Some(((), 20)),
			_ => None,
		}
	}

	fn parse_prefix_op(&mut self, min_bp: u8) -> ParseResult<Value> {
		let token = self.next();
		let operator = match token.kind {
			t!("+") => Operator::Add,
			t!("-") => Operator::Neg,
			t!("!") => Operator::Not,
			t!("<") => {
				let kind = self.parse_kind(token.span)?;
				let value = self.pratt_parse_expr(min_bp)?;
				let cast = Cast(kind, value);
				return Ok(Value::Cast(Box::new(cast)));
			}
			// should be unreachable as we previously check if the token was a prefix op.
			_ => unreachable!(),
		};
		let v = self.pratt_parse_expr(min_bp)?;

		// HACK: For compatiblity with the old parser apply + and - operator immediately if the
		// left value is a number.
		// FIXME: This has the problem that you can't specify the full range of an integer. All numbers
		// are currently parsed as positive numbers and the range of values for positive numbers
		// is one smaller then the range positive values, resulting in an overflow if you try to
		// use the max negative value.
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

	fn parse_infix_op(&mut self, min_bp: u8, lhs: Value) -> ParseResult<Value> {
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
						let number = self.parse_token_value()?;
						expected!(self, "@");
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
				expected!(self, "IN");
				Operator::NotInside
			}
			t!("IN") => Operator::Inside,
			t!("KNN") => {
				let start = expected!(self, "<").span;
				let amount = self.parse_token_value()?;
				self.expect_closing_delimiter(t!(">"), start)?;
				Operator::Knn(amount)
			}

			// should be unreachable as we previously check if the token was a prefix op.
			_ => unreachable!(),
		};
		let rhs = self.pratt_parse_expr(min_bp)?;
		Ok(Value::Expression(Box::new(Expression::Binary {
			l: lhs,
			o: operator,
			r: rhs,
		})))
	}

	/// The pratt parsing loop.
	/// Parses expression according to binding power.
	fn pratt_parse_expr(&mut self, min_bp: u8) -> ParseResult<Value> {
		let peek = self.peek();
		let mut lhs = if let Some(((), r_bp)) = self.prefix_binding_power(peek.kind) {
			self.parse_prefix_op(r_bp)?
		} else {
			self.parse_idiom_expression()?
		};

		loop {
			let token = self.peek();
			let Some((l_bp, r_bp)) = Self::infix_binding_power(token.kind) else {
				break;
			};

			if l_bp < min_bp {
				break;
			}

			lhs = self.parse_infix_op(r_bp, lhs)?;
		}

		Ok(lhs)
	}
}
