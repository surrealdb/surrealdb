use surrealdb_core::sql::Value;

use crate::api::conn::Method;
use crate::api::conn::Param;
use crate::api::Connection;
use crate::api::Result;
use crate::method::OnceLockExt;
// use crate::sql::Value;
use crate::sql::Array;
use crate::Surreal;
use std::borrow::Cow;
use std::future::Future;
use std::future::IntoFuture;
use std::pin::Pin;

/// A run future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Run<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) fn_name: String,
	pub(super) fn_version: Option<String>,
	pub(super) params: Array,
}
impl<C> Run<'_, C>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
	pub fn into_owned(self) -> Run<'static, C> {
		Run {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

impl<'r, Client> IntoFuture for Run<'r, Client>
where
	Client: Connection,
{
	type Output = Result<Value>;
	type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + Sync + 'r>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let mut conn = Client::new(Method::Run);
			conn.execute(
				self.client.router.extract()?,
				Param::new(vec![self.fn_name.into(), self.fn_version.into(), self.params.into()]),
			)
			.await
		})
	}
}

pub trait IntoParams {
	fn into_params(self) -> Array;
}

impl IntoParams for Array {
	fn into_params(self) -> Array {
		self
	}
}

impl IntoParams for Vec<Value> {
	fn into_params(self) -> Array {
		self.into()
	}
}

impl IntoParams for Value {
	fn into_params(self) -> Array {
		let arr: Vec<Value> = vec![self];
		Array::from(arr)
	}
}

impl IntoParams for () {
	fn into_params(self) -> Array {
		Vec::<Value>::new().into()
	}
}

impl<T0> IntoParams for (T0,)
where
	T0: Into<Value>,
{
	fn into_params(self) -> Array {
		let mut arr: Vec<Value> = Vec::with_capacity(1);
		arr.push(self.0.into());
		Array::from(arr)
	}
}

impl<T0, T1> IntoParams for (T0, T1)
where
	T0: Into<Value>,
	T1: Into<Value>,
{
	fn into_params(self) -> Array {
		let mut arr: Vec<Value> = Vec::with_capacity(1);
		arr.push(self.0.into());
		arr.push(self.1.into());
		Array::from(arr)
	}
}

impl<T0, T1, T2> IntoParams for (T0, T1, T2)
where
	T0: Into<Value>,
	T1: Into<Value>,
	T2: Into<Value>,
{
	fn into_params(self) -> Array {
		let mut arr: Vec<Value> = Vec::with_capacity(1);
		arr.push(self.0.into());
		arr.push(self.1.into());
		arr.push(self.2.into());
		Array::from(arr)
	}
}

pub trait IntoFn {
	fn into_fn(self) -> (String, Option<String>);
}

impl IntoFn for String {
	fn into_fn(self) -> (String, Option<String>) {
		(self, None)
	}
}
impl IntoFn for &str {
	fn into_fn(self) -> (String, Option<String>) {
		(self.to_owned(), None)
	}
}

impl<S0, S1> IntoFn for (S0, S1)
where
	S0: Into<String>,
	S1: Into<String>,
{
	fn into_fn(self) -> (String, Option<String>) {
		(self.0.into(), Some(self.1.into()))
	}
}
