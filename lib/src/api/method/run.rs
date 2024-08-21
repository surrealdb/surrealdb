use crate::api::conn::Command;
use crate::api::Connection;
use crate::api::Result;
use crate::method::OnceLockExt;
use crate::Surreal;
use crate::Value;
use std::borrow::Cow;
use std::future::IntoFuture;
use surrealdb_core::sql::Array as CoreArray;

use super::BoxFuture;

/// A run future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Run<'r, C: Connection> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) name: String,
	pub(super) version: Option<String>,
	pub(super) args: CoreArray,
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
	type IntoFuture = BoxFuture<'r, Self::Output>;

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
	fn into_args(self) -> Vec<Value>;
}

impl IntoArgs for Value {
	fn into_args(self) -> Vec<Value> {
		vec![self]
	}
}

impl<T> IntoArgs for Vec<T>
where
	T: Into<Value>,
{
	fn into_args(self) -> Vec<Value> {
		self.into_iter().map(Into::into).collect()
	}
}

impl<T, const N: usize> IntoArgs for [T; N]
where
	T: Into<Value>,
{
	fn into_args(self) -> Vec<Value> {
		self.into_iter().map(Into::into).collect()
	}
}

impl<T, const N: usize> IntoArgs for &[T; N]
where
	T: Into<Value> + Clone,
{
	fn into_args(self) -> Vec<Value> {
		self.iter().cloned().map(Into::into).collect()
	}
}

impl<T> IntoArgs for &[T]
where
	T: Into<Value> + Clone,
{
	fn into_args(self) -> Vec<Value> {
		self.iter().cloned().map(Into::into).collect()
	}
}

macro_rules! impl_args_tuple {
    ($($i:ident), *$(,)?) => {
		impl_args_tuple!(@marker $($i,)*);
    };
    ($($cur:ident,)* @marker $head:ident, $($tail:ident,)*) => {
		impl<$($cur: Into<Value>,)*> IntoArgs for ($($cur,)*) {
			#[allow(non_snake_case)]
			fn into_args(self) -> Vec<Value> {
				let ($($cur,)*) = self;
				vec![$($cur.into(),)*]
			}
		}

		impl_args_tuple!($($cur,)* $head, @marker $($tail,)*);
	};
    ($($cur:ident,)* @marker ) => {
		impl<$($cur: Into<Value>,)*> IntoArgs for ($($cur,)*) {
			#[allow(non_snake_case)]
			fn into_args(self) -> Vec<Value> {
				let ($($cur,)*) = self;
				vec![$($cur.into(),)*]
			}
		}
	}
}

impl_args_tuple!(A, B, C, D, E, F,);

/* TODO: Removed for now.
 * The detach value PR removed a lot of conversion methods with, pending later request which might
 * add them back depending on how the sdk turns out.
 *
macro_rules! into_impl {
	($type:ty) => {
		impl IntoArgs for $type {
			fn into_args(self) -> Vec<Value> {
				vec![Value::from(self)]
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
*/

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
