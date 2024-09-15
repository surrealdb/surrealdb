use crate::api::conn::Command;
use crate::api::method::BoxFuture;
use crate::api::Connection;
use crate::api::Result;
use crate::method::OnceLockExt;
use crate::sql::Value;
use crate::Surreal;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_content::Serializer;
use serde_content::Value as Content;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::sql::to_value;
use surrealdb_core::sql::Array;

/// A run future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Run<'r, C: Connection, R> {
	pub(super) client: Cow<'r, Surreal<C>>,
	pub(super) function: Result<(String, Option<String>)>,
	pub(super) args: serde_content::Result<serde_content::Value<'static>>,
	pub(super) response_type: PhantomData<R>,
}
impl<C, R> Run<'_, C, R>
where
	C: Connection,
{
	/// Converts to an owned type which can easily be moved to a different thread
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
	R: DeserializeOwned,
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
			let router = client.router.extract()?;
			let (name, version) = function?;
			let value = match args.map_err(crate::error::Db::from)? {
				// Tuples are treated as multiple function arguments
				Content::Tuple(tup) => tup,
				// Everything else is treated as a single argument
				content => vec![content],
			};
			let args = match to_value(value)? {
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

impl<'r, Client, R> Run<'r, Client, R>
where
	Client: Connection,
{
	/// Supply arguments to the function being run.
	pub fn args(mut self, args: impl Serialize) -> Self {
		self.args = Serializer::new().serialize(args);
		self
	}
}

/// Converts a function into name and version parts
pub trait IntoFn {
	/// Handles the conversion of the function string
	fn into_fn(self) -> Result<(String, Option<String>)>;
}

impl IntoFn for String {
	fn into_fn(self) -> Result<(String, Option<String>)> {
		match self.split_once('<') {
			Some((name, rest)) => match rest.strip_suffix('>') {
				Some(version) => Ok((name.to_owned(), Some(version.to_owned()))),
				None => Err(crate::error::Db::InvalidFunction {
					name: self,
					message: "function version is missing a clossing '>'".to_owned(),
				}
				.into()),
			},
			None => Ok((self, None)),
		}
	}
}

impl IntoFn for &str {
	fn into_fn(self) -> Result<(String, Option<String>)> {
		match self.split_once('<') {
			Some((name, rest)) => match rest.strip_suffix('>') {
				Some(version) => Ok((name.to_owned(), Some(version.to_owned()))),
				None => Err(crate::error::Db::InvalidFunction {
					name: self.to_owned(),
					message: "function version is missing a clossing '>'".to_owned(),
				}
				.into()),
			},
			None => Ok((self.to_owned(), None)),
		}
	}
}

impl IntoFn for &String {
	fn into_fn(self) -> Result<(String, Option<String>)> {
		self.as_str().into_fn()
	}
}
