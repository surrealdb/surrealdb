use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

use surrealdb_types::{self, Array, SurrealValue, Value};

use crate::conn::Command;
use crate::method::{BoxFuture, OnceLockExt};
use crate::{Connection, Result, Surreal};

/// A run future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Run<'r, C: Connection, R> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) function: Result<(String, Option<String>)>,
	pub(super) args: Value,
	pub(super) response_type: PhantomData<R>,
}
impl<C, R> Run<'_, C, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different
	/// thread
	pub fn into_owned(self) -> Run<'static, C, R> {
		Run {
			client: Cow::Owned(self.client.into_owned()),
			..self
		}
	}
}

impl<'r, Client, R> IntoFuture for Run<'r, Client, R>
where
	Client: Connection,
	R: SurrealValue,
{
	type Output = Result<R>;
	type IntoFuture = BoxFuture<'r, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Run {
			client,
			function,
			args,
			..
		} = self;
		Box::pin(async move {
			let router = client.inner.router.extract()?;
			let (name, version) = function?;

			let args = match args.into_value() {
				Value::None => Array::new(),
				Value::Array(array) => array,
				value => Array::from(vec![value]),
			};

			router
				.execute(Command::Run {
					name,
					version,
					args,
				})
				.await
		})
	}
}

impl<Client, R> Run<'_, Client, R>
where
	Client: Connection,
{
	/// Supply arguments to the function being run.
	pub fn args(mut self, args: impl SurrealValue) -> Self {
		self.args = args.into_value();
		self
	}
}

/// Converts a function into name and version parts
pub trait IntoFn: into_fn::Sealed {}

impl IntoFn for String {}
impl into_fn::Sealed for String {
	fn into_fn(self) -> Result<(String, Option<String>)> {
		match self.split_once('<') {
			Some((name, rest)) => match rest.strip_suffix('>') {
				Some(version) => Ok((name.to_owned(), Some(version.to_owned()))),
				None => Err(crate::error::Api::InvalidParams(format!(
					"Invalid function syntax '{}': function version is missing a closing '>'",
					self
				))),
			},
			None => Ok((self, None)),
		}
	}
}

impl IntoFn for &str {}
impl into_fn::Sealed for &str {
	fn into_fn(self) -> Result<(String, Option<String>)> {
		match self.split_once('<') {
			Some((name, rest)) => match rest.strip_suffix('>') {
				Some(version) => Ok((name.to_owned(), Some(version.to_owned()))),
				None => Err(crate::error::Api::InvalidParams(format!(
					"Invalid function syntax '{}': function version is missing a closing '>'",
					self
				))),
			},
			None => Ok((self.to_owned(), None)),
		}
	}
}

impl IntoFn for &String {}
impl into_fn::Sealed for &String {
	fn into_fn(self) -> Result<(String, Option<String>)> {
		self.as_str().into_fn()
	}
}

mod into_fn {
	pub trait Sealed {
		/// Handles the conversion of the function string
		fn into_fn(self) -> super::Result<(String, Option<String>)>;
	}
}
