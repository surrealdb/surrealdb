use crate::sql::array::{array, Array};
use crate::sql::expression::{expression, Expression};
use crate::sql::idiom::{idiom, Idiom};
use crate::sql::param::{param, Param};
use crate::sql::value::{value, Value};

pub trait Parse<T> {
	fn parse(val: &str) -> T;
}

impl Parse<Value> for Value {
	fn parse(val: &str) -> Value {
		value(val).unwrap().1
	}
}

impl Parse<Array> for Array {
	fn parse(val: &str) -> Array {
		array(val).unwrap().1
	}
}

impl Parse<Param> for Param {
	fn parse(val: &str) -> Param {
		param(val).unwrap().1
	}
}

impl Parse<Idiom> for Idiom {
	fn parse(val: &str) -> Idiom {
		idiom(val).unwrap().1
	}
}

impl Parse<Expression> for Expression {
	fn parse(val: &str) -> Expression {
		expression(val).unwrap().1
	}
}
