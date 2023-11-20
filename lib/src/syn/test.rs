pub(crate) use super::v1::builtin::builtin_name;
use crate::sql::{Array, Expression, Idiom, Param, Script, Thing, Value};

use super::v1::test::*;

pub trait Parse<T> {
	fn parse(val: &str) -> T;
}

impl Parse<Self> for Value {
	fn parse(val: &str) -> Self {
		value(val).unwrap().1
	}
}

impl Parse<Self> for Array {
	fn parse(val: &str) -> Self {
		array(val).unwrap().1
	}
}

impl Parse<Self> for Param {
	fn parse(val: &str) -> Self {
		param(val).unwrap().1
	}
}

impl Parse<Self> for Idiom {
	fn parse(val: &str) -> Self {
		idiom(val).unwrap().1
	}
}

impl Parse<Self> for Script {
	fn parse(val: &str) -> Self {
		script(val).unwrap().1
	}
}

impl Parse<Self> for Thing {
	fn parse(val: &str) -> Self {
		thing(val).unwrap().1
	}
}

impl Parse<Self> for Expression {
	fn parse(val: &str) -> Self {
		expression(val).unwrap().1
	}
}
