use crate::Surreal;

use crate::api::Result;
use crate::api::conn::Command;
use crate::api::method::BoxFuture;

use anyhow::Context;
use futures::StreamExt;
use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;
use surrealdb_core::expr::Array;
use surrealdb_core::expr::TryFromValue;
use surrealdb_core::expr::Value;
use surrealdb_core::sql::Function;
use surrealdb_core::sql::Model;
use surrealdb_core::sql::SqlValue;
use surrealdb_core::sql::Statement;
use surrealdb_protocol::QueryResponseValueStream;
use surrealdb_protocol::proto::rpc::v1::QueryRequest;

/// A run future
#[derive(Debug)]
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct Run<R> {
	pub(super) client: Surreal,
	pub(super) function: Result<(String, Option<String>)>,
	pub(super) args: Array,
	pub(super) response_type: PhantomData<R>,
}
impl<R> Run<R> {}

impl<R> IntoFuture for Run<R>
where
	R: TryFromValue,
{
	type Output = Result<R>;
	type IntoFuture = BoxFuture<'static, Self::Output>;

	fn into_future(self) -> Self::IntoFuture {
		let Run {
			client,
			function,
			args,
			..
		} = self;

		Box::pin(async move {
			let mut client = client.client.clone();
			let (name, version) = function?;

			let args = args.0.into_iter().map(|x| x.into()).collect::<Vec<_>>();

			let func: SqlValue = match name.strip_prefix("fn::") {
				Some(name) => Function::Custom(name.to_owned(), args).into(),
				None => match name.strip_prefix("ml::") {
					Some(name) => {
						let mut tmp = Model::default();
						name.clone_into(&mut tmp.name);
						tmp.args = args;
						tmp.version = version.context("ML functions must have a version")?;
						tmp.into()
					}
					None => Function::Normal(name, args).into(),
				},
			};

			let stmt = Statement::Value(func).to_string();

			let response = client
				.query(QueryRequest {
					query: stmt,
					variables: None,
					txn_id: None,
				})
				.await?;

			let mut stream = QueryResponseValueStream::new(response.into_inner());

			let value = stream.next().await.context("Failed to get value from stream")??;

			let value: Value = value.try_into()?;

			Ok(R::try_from_value(value)?)
		})
	}
}

impl<R> Run<R> {
	/// Supply arguments to the function being run.
	pub fn args(mut self, args: impl Into<Array>) -> Self {
		self.args = args.into();
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

impl IntoFn for &str {}
impl into_fn::Sealed for &str {
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
