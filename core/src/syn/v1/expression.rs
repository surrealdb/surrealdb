use super::{
	block::block,
	comment::mightbespace,
	common::{closechevron, expect_delimited, openchevron},
	kind::kind,
	operator,
	value::single,
	IResult,
};
use crate::sql::{Cast, Expression, Future, Operator, Value};
use nom::{bytes::complete::tag, character::complete::char, combinator::cut, sequence::delimited};

pub fn cast(i: &str) -> IResult<&str, Cast> {
	let (i, k) = delimited(char('<'), cut(kind), char('>'))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = cut(single)(i)?;
	Ok((i, Cast(k, v)))
}

pub fn unary(i: &str) -> IResult<&str, Expression> {
	let (i, o) = operator::unary(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = single(i)?;
	Ok((
		i,
		Expression::Unary {
			o,
			v,
		},
	))
}

/// Augment an existing expression
pub(crate) fn augment(mut this: Expression, l: Value, o: Operator) -> Expression {
	match &mut this {
		Expression::Binary {
			l: left,
			o: op,
			..
		} if o.precedence() >= op.precedence() => match left {
			Value::Expression(x) => {
				*x.as_mut() = augment(std::mem::take(x), l, o);
				this
			}
			_ => {
				*left = Expression::new(l, o, std::mem::take(left)).into();
				this
			}
		},
		e => {
			let r = Value::from(std::mem::take(e));
			Expression::new(l, o, r)
		}
	}
}

#[cfg(test)]
pub fn binary(i: &str) -> IResult<&str, Expression> {
	use super::depth;
	use super::value;

	let (i, l) = single(i)?;
	let (i, o) = operator::binary(i)?;
	// Make sure to dive if the query is a right-deep binary tree.
	let _diving = depth::dive(i)?;
	let (i, r) = value::value(i)?;
	let v = match r {
		Value::Expression(r) => augment(*r, l, o),
		_ => Expression::new(l, o, r),
	};
	Ok((i, v))
}

pub fn future(i: &str) -> IResult<&str, Future> {
	let (i, _) = expect_delimited(openchevron, tag("future"), closechevron)(i)?;
	cut(|i| {
		let (i, _) = mightbespace(i)?;
		let (i, v) = block(i)?;
		Ok((i, Future(v)))
	})(i)
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::{Block, Kind, Number, Operator, Value};

	#[test]
	fn cast_int() {
		let sql = "<int>1.2345";
		let res = cast(sql);
		let out = res.unwrap().1;
		assert_eq!("<int> 1.2345f", format!("{}", out));
		assert_eq!(out, Cast(Kind::Int, 1.2345.into()));
	}

	#[test]
	fn cast_string() {
		let sql = "<string>1.2345";
		let res = cast(sql);
		let out = res.unwrap().1;
		assert_eq!("<string> 1.2345f", format!("{}", out));
		assert_eq!(out, Cast(Kind::String, 1.2345.into()));
	}

	#[test]
	fn expression_statement() {
		let sql = "true AND false";
		let res = binary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("true AND false", format!("{}", out));
	}

	#[test]
	fn expression_left_opened() {
		let sql = "3 * 3 * 3 = 27";
		let res = binary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("3 * 3 * 3 = 27", format!("{}", out));
	}

	#[test]
	fn expression_left_closed() {
		let sql = "(3 * 3 * 3) = 27";
		let res = binary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("(3 * 3 * 3) = 27", format!("{}", out));
	}

	#[test]
	fn expression_right_opened() {
		let sql = "27 = 3 * 3 * 3";
		let res = binary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("27 = 3 * 3 * 3", format!("{}", out));
	}

	#[test]
	fn expression_right_closed() {
		let sql = "27 = (3 * 3 * 3)";
		let res = binary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("27 = (3 * 3 * 3)", format!("{}", out));
	}

	#[test]
	fn expression_both_opened() {
		let sql = "3 * 3 * 3 = 3 * 3 * 3";
		let res = binary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("3 * 3 * 3 = 3 * 3 * 3", format!("{}", out));
	}

	#[test]
	fn expression_both_closed() {
		let sql = "(3 * 3 * 3) = (3 * 3 * 3)";
		let res = binary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("(3 * 3 * 3) = (3 * 3 * 3)", format!("{}", out));
	}

	#[test]
	fn expression_unary() {
		let sql = "-a";
		let res = unary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out));
	}

	#[test]
	fn expression_with_unary() {
		let sql = "-(5) + 5";
		let res = binary(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!(sql, format!("{}", out));
	}

	#[test]
	fn future_expression() {
		let sql = "<future> { 5 + 10 }";
		let res = future(sql);
		let out = res.unwrap().1;
		assert_eq!("<future> { 5 + 10 }", format!("{}", out));
		assert_eq!(
			out,
			Future(Block::from(Value::from(Expression::Binary {
				l: Value::Number(Number::Int(5)),
				o: Operator::Add,
				r: Value::Number(Number::Int(10))
			})))
		);
	}
}
