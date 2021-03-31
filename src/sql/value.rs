use crate::dbs;
use crate::dbs::Executor;
use crate::dbs::Runtime;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::expression::Expression;
use crate::sql::literal::Literal;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum Value {
	Literal(Literal),
	Expression(Expression),
}

impl Default for Value {
	fn default() -> Value {
		Value::Literal(Literal::None)
	}
}

impl From<Literal> for Value {
	fn from(v: Literal) -> Self {
		Value::Literal(v)
	}
}

impl From<Expression> for Value {
	fn from(v: Expression) -> Self {
		Value::Expression(v)
	}
}

impl fmt::Display for Value {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Value::Literal(ref v) => write!(f, "{}", v),
			Value::Expression(ref v) => write!(f, "{}", v),
		}
	}
}

impl dbs::Process for Value {
	fn process(
		&self,
		ctx: &Runtime,
		exe: &Executor,
		doc: Option<&Document>,
	) -> Result<Literal, Error> {
		match self {
			Value::Literal(ref v) => v.process(ctx, exe, doc),
			Value::Expression(ref v) => v.process(ctx, exe, doc),
		}
	}
}
