use crate::api::conn::Command;
use crate::api::Connection;
use crate::api::Result;
use crate::method::OnceLockExt;
use crate::sql::Array;
use crate::sql::Value;
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
	pub(super) name: String,
	pub(super) version: Option<String>,
	pub(super) args: Array,
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
		let Run {
			client,
			name,
			version,
			args,
		} = self;
		Box::pin(async move {
			let router = client.router.extract()?;
			router
				.execute_value(Command::Run {
					name,
					version,
					args,
				})
				.await
		})
	}
}

pub trait IntoArgs {
	fn into_args(self) -> Array;
}

impl IntoArgs for Array {
	fn into_args(self) -> Array {
		self
	}
}

impl IntoArgs for Value {
	fn into_args(self) -> Array {
		let arr: Vec<Value> = vec![self];
		Array::from(arr)
	}
}

impl<T> IntoArgs for Vec<T>
where
	T: Into<Value>,
{
	fn into_args(self) -> Array {
		let arr: Vec<Value> = self.into_iter().map(Into::into).collect();
		Array::from(arr)
	}
}

impl<T, const N: usize> IntoArgs for [T; N]
where
	T: Into<Value>,
{
	fn into_args(self) -> Array {
		let arr: Vec<Value> = self.into_iter().map(Into::into).collect();
		Array::from(arr)
	}
}

impl<T, const N: usize> IntoArgs for &[T; N]
where
	T: Into<Value> + Clone,
{
	fn into_args(self) -> Array {
		let arr: Vec<Value> = self.iter().cloned().map(Into::into).collect();
		Array::from(arr)
	}
}

impl<T> IntoArgs for &[T]
where
	T: Into<Value> + Clone,
{
	fn into_args(self) -> Array {
		let arr: Vec<Value> = self.iter().cloned().map(Into::into).collect();
		Array::from(arr)
	}
}
impl IntoArgs for () {
	fn into_args(self) -> Array {
		Vec::<Value>::new().into()
	}
}

impl<T0> IntoArgs for (T0,)
where
	T0: Into<Value>,
{
	fn into_args(self) -> Array {
		let arr: Vec<Value> = vec![self.0.into()];
		Array::from(arr)
	}
}

impl<T0, T1> IntoArgs for (T0, T1)
where
	T0: Into<Value>,
	T1: Into<Value>,
{
	fn into_args(self) -> Array {
		let arr: Vec<Value> = vec![self.0.into(), self.1.into()];
		Array::from(arr)
	}
}

impl<T0, T1, T2> IntoArgs for (T0, T1, T2)
where
	T0: Into<Value>,
	T1: Into<Value>,
	T2: Into<Value>,
{
	fn into_args(self) -> Array {
		let arr: Vec<Value> = vec![self.0.into(), self.1.into(), self.2.into()];
		Array::from(arr)
	}
}

macro_rules! into_impl {
	($type:ty) => {
		impl IntoArgs for $type {
			fn into_args(self) -> Array {
				let val: Value = self.into();
				Array::from(val)
			}
		}
	};
}
into_impl!(i8);
into_impl!(i16);
into_impl!(i32);
into_impl!(i64);
into_impl!(i128);
into_impl!(u8);
into_impl!(u16);
into_impl!(u32);
into_impl!(u64);
into_impl!(u128);
into_impl!(usize);
into_impl!(isize);
into_impl!(f32);
into_impl!(f64);
into_impl!(String);
into_impl!(&str);

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
