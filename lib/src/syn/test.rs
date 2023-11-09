use crate::sql::array::Array;
use crate::sql::expression::Expression;
use crate::sql::idiom::Idiom;
use crate::sql::param::Param;
use crate::sql::script::Script;
use crate::sql::thing::Thing;
use crate::sql::value::Value;

fn builtin_name(name: &str) -> Value {
	todo!()
}

pub trait Parse<T> {
	fn parse(val: &str) -> T;
}

impl Parse<Self> for Value {
	fn parse(val: &str) -> Self {
		todo!();
	}
}

impl Parse<Self> for Array {
	fn parse(val: &str) -> Self {
		todo!();
	}
}

impl Parse<Self> for Param {
	fn parse(val: &str) -> Self {
		todo!();
	}
}

impl Parse<Self> for Idiom {
	fn parse(val: &str) -> Self {
		todo!();
	}
}

impl Parse<Self> for Script {
	fn parse(val: &str) -> Self {
		todo!();
	}
}

impl Parse<Self> for Thing {
	fn parse(val: &str) -> Self {
		todo!();
	}
}

impl Parse<Self> for Expression {
	fn parse(val: &str) -> Self {
		todo!();
	}
}
