use std::mem;

use crate::sql::{
	statements::UseStatement, Cond, Ident, Idiom, Limit, Order, Orders, Part, Start, Table, Value,
};
use async_graphql::dynamic::Scalar;

pub trait IntoExt<T> {
	fn intox(self) -> T;
}

impl<S, T> IntoExt<T> for S
where
	T: FromExt<S>,
{
	fn intox(self) -> T {
		T::from(self)
	}
}

trait FromExt<T> {
	fn from(value: T) -> Self;
}

impl<V> FromExt<V> for Cond
where
	V: Into<Value>,
{
	fn from(value: V) -> Self {
		Self(value.into())
	}
}
impl<V> FromExt<V> for Limit
where
	V: Into<Value>,
{
	fn from(value: V) -> Self {
		Self(value.into())
	}
}

impl<I> FromExt<(I, bool, bool, bool, bool)> for Order
where
	I: Into<Idiom>,
{
	fn from((order, random, collate, numeric, direction): (I, bool, bool, bool, bool)) -> Self {
		Self {
			order: order.into(),
			random,
			collate,
			numeric,
			direction,
		}
	}
}

impl<V> FromExt<V> for Start
where
	V: Into<Value>,
{
	fn from(value: V) -> Self {
		Start(value.into())
	}
}

impl FromExt<(&str, &str)> for UseStatement {
	fn from(value: (&str, &str)) -> Self {
		Self {
			ns: Some(value.0.into()),
			db: Some(value.1.into()),
		}
	}
}
impl FromExt<(String, String)> for UseStatement {
	fn from(value: (String, String)) -> Self {
		Self {
			ns: Some(value.0),
			db: Some(value.1),
		}
	}
}
impl FromExt<(Option<String>, Option<String>)> for UseStatement {
	fn from(value: (Option<String>, Option<String>)) -> Self {
		Self {
			ns: value.0,
			db: value.1,
		}
	}
}

impl<S> FromExt<S> for Table
where
	S: Into<String>,
{
	fn from(value: S) -> Self {
		Table(value.into())
	}
}

impl FromExt<Vec<Order>> for Orders {
	fn from(value: Vec<Order>) -> Self {
		Orders(value)
	}
}

impl<S> FromExt<S> for Ident
where
	S: Into<String>,
{
	fn from(value: S) -> Self {
		Ident(value.into())
	}
}

impl<P> FromExt<P> for Idiom
where
	P: Into<Part>,
{
	fn from(value: P) -> Self {
		Idiom(vec![value.into()])
	}
}

pub trait ValidatorExt {
	fn add_validator(
		&mut self,
		validator: impl Fn(&async_graphql::Value) -> bool + Send + Sync + 'static,
	) -> &mut Self;
}

impl ValidatorExt for Scalar {
	fn add_validator(
		&mut self,
		validator: impl Fn(&async_graphql::Value) -> bool + Send + Sync + 'static,
	) -> &mut Self {
		let mut tmp = Scalar::new("");
		mem::swap(self, &mut tmp);
		*self = tmp.validator(validator);
		self
	}
}

use crate::sql::Thing as SqlThing;
use crate::sql::Value as SqlValue;

pub trait TryAsExt {
	fn try_as_thing(self) -> Result<SqlThing, Self>
	where
		Self: Sized;
}
impl TryAsExt for SqlValue {
	fn try_as_thing(self) -> Result<SqlThing, Self> {
		match self {
			SqlValue::Thing(t) => Ok(t),
			v => Err(v),
		}
	}
}
