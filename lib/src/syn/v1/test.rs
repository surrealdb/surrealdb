use super::{
	super::Parse,
	expression::binary as expression,
	function::script_body as script,
	idiom::plain as idiom,
	literal::param,
	thing::thing,
	value::{array, value},
};
use nom::Finish;

use crate::sql::{Array, Expression, Idiom, Param, Script, Thing, Value};

impl Parse<Self> for Value {
	fn parse(val: &str) -> Self {
		value(val).finish().unwrap().1
	}
}

impl Parse<Self> for Array {
	fn parse(val: &str) -> Self {
		array(val).finish().unwrap().1
	}
}

impl Parse<Self> for Param {
	fn parse(val: &str) -> Self {
		param(val).finish().unwrap().1
	}
}

impl Parse<Self> for Idiom {
	fn parse(val: &str) -> Self {
		idiom(val).finish().unwrap().1
	}
}

impl Parse<Self> for Script {
	fn parse(val: &str) -> Self {
		script(val).finish().unwrap().1
	}
}

impl Parse<Self> for Thing {
	fn parse(val: &str) -> Self {
		thing(val).finish().unwrap().1
	}
}

impl Parse<Self> for Expression {
	fn parse(val: &str) -> Self {
		expression(val).finish().unwrap().1
	}
}
