use surrealdb::sql::{
	self, statements::UseStatement, Cond, Idiom, Limit, Order, Orders, Start, Table, Value,
};

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
		let mut out = Self::default();
		out.0 = value.into();
		out
	}
}
impl<V> FromExt<V> for Limit
where
	V: Into<Value>,
{
	fn from(value: V) -> Self {
		let mut out = Self::default();
		out.0 = value.into();
		out
	}
}

impl<I> FromExt<(I, bool, bool, bool, bool)> for Order
where
	I: Into<Idiom>,
{
	fn from((order, random, collate, numeric, direction): (I, bool, bool, bool, bool)) -> Self {
		let mut out = Self::default();
		out.order = order.into();
		out.random = random;
		out.collate = collate;
		out.numeric = numeric;
		out.direction = direction;

		out
	}
}

impl<V> FromExt<V> for Start
where
	V: Into<Value>,
{
	fn from(value: V) -> Self {
		let mut out = Self::default();
		out.0 = value.into();
		out
	}
}

impl FromExt<(&str, &str)> for UseStatement {
	fn from(value: (&str, &str)) -> Self {
		let mut out = Self::default();
		out.ns = Some(value.0.into());
		out.db = Some(value.1.into());
		out
	}
}
impl FromExt<(String, String)> for UseStatement {
	fn from(value: (String, String)) -> Self {
		let mut out = Self::default();
		out.ns = Some(value.0);
		out.db = Some(value.1);
		out
	}
}
impl FromExt<(Option<String>, Option<String>)> for UseStatement {
	fn from(value: (Option<String>, Option<String>)) -> Self {
		let mut out = Self::default();
		out.ns = value.0.into();
		out.db = value.1.into();
		out
	}
}

impl<S> FromExt<S> for Table
where
	S: Into<String>,
{
	fn from(value: S) -> Self {
		let mut out = Table::default();
		out.0 = value.into();

		out
	}
}

impl FromExt<Vec<Order>> for Orders {
	fn from(value: Vec<Order>) -> Self {
		let mut out = Orders::default();
		out.0 = value;
		out
	}
}

// impl FromExt<sql::Value> for Cond {
// 	fn from(value: sql::Value) -> Self {
// 		let mut out = Cond::default();
// 		out.0 = value;
// 		out
// 	}
// }
