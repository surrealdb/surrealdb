use std::ops::Deref;

use crate::sql::statements::define::config::graphql::TableConfig;
use crate::sql::statements::DefineTableStatement;
use crate::sql::{statements::UseStatement, Cond, Ident, Idiom, Limit, Part, Start, Table, Value};
use async_graphql::{Name, Value as GqlValue};

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

pub trait TryIntoExt<T> {
	type Error;

	fn try_intox(self) -> Result<T, Self::Error>;
}

pub trait TryFromExt<T>: Sized {
	type Error;

	fn try_fromx(value: T) -> Result<Self, Self::Error>;
}

impl<S, T> TryIntoExt<T> for S
where
	T: TryFromExt<S>,
{
	type Error = <T as TryFromExt<S>>::Error;

	fn try_intox(self) -> Result<T, <T as TryFromExt<S>>::Error> {
		T::try_fromx(self)
	}
}

impl TryFromExt<f64> for async_graphql::Value {
	type Error = GqlError;

	fn try_fromx(value: f64) -> Result<Self, GqlError> {
		Ok(Self::Number(Number::from_f64(value).ok_or_else(|| {
			resolver_error(format!("non-finite float (not supported in json): {}", value))
		})?))
	}
}

// impl TryFromExt<Coord<f64>> for (GqlValue, GqlValue) {
// 	type Error = GqlError;

// 	fn try_fromx(value: Coord<f64>) -> Result<Self, Self::Error> {
// 		// Ok(GqlValue::Object(
// 		// 	[
// 		// 		(Name::new("type"), GqlValue::String("Point".to_string())),
// 		// 		(
// 		// 			Name::new("coordinates"),
// 		// 			GqlValue::List(vec![value.x().try_intox()?, value.y().try_intox()?]),
// 		// 		),
// 		// 	]
// 		// 	.into(),
// 		// ))
// 	}
// }

#[cfg(debug_assertions)]
pub trait ValidatorExt {
	fn add_validator(
		&mut self,
		validator: impl Fn(&async_graphql::Value) -> bool + Send + Sync + 'static,
	) -> &mut Self;
}

#[cfg(debug_assertions)]
use async_graphql::dynamic::Scalar;
use geo::{Coord, CoordFloat};
use serde_json::Number;
#[cfg(debug_assertions)]
impl ValidatorExt for Scalar {
	fn add_validator(
		&mut self,
		validator: impl Fn(&async_graphql::Value) -> bool + Send + Sync + 'static,
	) -> &mut Self {
		let mut tmp = Scalar::new("");
		std::mem::swap(self, &mut tmp);
		*self = tmp.validator(validator);
		self
	}
}

use crate::sql::Thing as SqlThing;
use crate::sql::Value as SqlValue;

use super::error::resolver_error;
use super::GqlError;

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

pub trait Named {
	fn name(&self) -> &str;
}

impl Named for DefineTableStatement {
	fn name(&self) -> &str {
		&self.name
	}
}

impl Named for TableConfig {
	fn name(&self) -> &str {
		&self.name
	}
}

pub trait NamedContainer {
	fn contains_name(&self, name: &str) -> bool;
}

impl<I, N> NamedContainer for I
where
	I: Deref<Target = [N]>,
	N: Named,
{
	fn contains_name(&self, name: &str) -> bool {
		self.iter().any(|n| n.name() == name)
	}
}
