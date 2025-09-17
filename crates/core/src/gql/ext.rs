use std::ops::Deref;

use crate::expr::statements::define::config::graphql::TableConfig;
use crate::expr::statements::{DefineTableStatement, UseStatement};
use crate::expr::{Cond, Ident, Idiom, Limit, Part, Start, Table};
use crate::val::{RecordId, Value};

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

#[cfg(debug_assertions)]
pub trait ValidatorExt {
	fn add_validator(
		&mut self,
		validator: impl Fn(&async_graphql::Value) -> bool + Send + Sync + 'static,
	) -> &mut Self;
}

#[cfg(debug_assertions)]
use async_graphql::dynamic::Scalar;
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

pub trait TryAsExt {
	fn try_as_thing(self) -> Result<RecordId, Self>
	where
		Self: Sized;
}
impl TryAsExt for Value {
	fn try_as_thing(self) -> Result<RecordId, Self> {
		match self {
			Value::RecordId(t) => Ok(t),
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
