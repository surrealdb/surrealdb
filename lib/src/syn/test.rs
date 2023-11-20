use crate::{
	api::err::Error,
	sql::{Array, Expression, Idiom, Param, Script, Thing, Value},
};

use super::{idiom, thing, value};

pub fn builtin_name(i: &str) -> Result<(), Error> {
	todo!()
}

pub trait Parse<T> {
	fn parse(val: &str) -> T;
}

impl Parse<Self> for Value {
	fn parse(val: &str) -> Self {
		value(val).unwrap()
	}
}

impl Parse<Self> for Array {
	fn parse(val: &str) -> Self {
		todo!()
	}
}

impl Parse<Self> for Param {
	fn parse(val: &str) -> Self {
		todo!()
	}
}

impl Parse<Self> for Idiom {
	fn parse(val: &str) -> Self {
		idiom(val).unwrap()
	}
}

impl Parse<Self> for Script {
	fn parse(val: &str) -> Self {
		todo!()
	}
}

impl Parse<Self> for Thing {
	fn parse(val: &str) -> Self {
		thing(val).unwrap()
	}
}

impl Parse<Self> for Expression {
	fn parse(val: &str) -> Self {
		todo!()
	}
}
