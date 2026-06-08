use std::borrow::Cow;
use std::future::IntoFuture;
use std::marker::PhantomData;

use crate::conn::Command;
use crate::method::{BoxFuture, OnceLockExt};
use crate::types::{Array, SurrealValue, Value};
use crate::{Connection, Result, Surreal};

/// Returned by [`Surreal::run`](crate::Surreal::run) to invoke a defined function (`fn::…`).
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
				.execute(
					client.session_id,
					Command::Run {
						name,
						version,
						args,
					},
				)
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
		self.as_str().into_fn()
	}
}

impl IntoFn for &str {}
impl into_fn::Sealed for &str {
	fn into_fn(self) -> Result<(String, Option<String>)> {
		into_fn::parse(self)
	}
}

impl IntoFn for &String {}
impl into_fn::Sealed for &String {
	fn into_fn(self) -> Result<(String, Option<String>)> {
		self.as_str().into_fn()
	}
}

mod into_fn {
	use super::Result;

	pub trait Sealed {
		/// Handles the conversion of the function string
		fn into_fn(self) -> Result<(String, Option<String>)>;
	}

	/// Split `name<version>` and reject anything that isn't a SurrealQL function
	/// identifier. The local engine builds the run query as `format!("{name}(...)")`,
	/// so without this check a name like `SELECT * FROM secret; --` would be
	/// executed as arbitrary SurrealQL instead of a function call.
	pub(super) fn parse(input: &str) -> Result<(String, Option<String>)> {
		let (name, version) = match input.split_once('<') {
			Some((name, rest)) => match rest.strip_suffix('>') {
				Some(version) => (name, Some(version)),
				None => {
					return Err(crate::Error::validation(
						format!(
							"Invalid function syntax '{input}': function version is missing a closing '>'"
						),
						Some(crate::types::ValidationError::InvalidParams),
					));
				}
			},
			None => (input, None),
		};
		if !is_function_name(name) {
			return Err(crate::Error::validation(
				format!("Invalid function name '{name}'"),
				Some(crate::types::ValidationError::InvalidParams),
			));
		}
		if let Some(v) = version
			&& !is_function_version(v)
		{
			return Err(crate::Error::validation(
				format!("Invalid function version '{v}'"),
				Some(crate::types::ValidationError::InvalidParams),
			));
		}
		Ok((name.to_owned(), version.map(str::to_owned)))
	}

	fn is_function_name(s: &str) -> bool {
		if s.is_empty() {
			return false;
		}
		s.split("::").all(|part| {
			let mut chars = part.chars();
			match chars.next() {
				Some(c) if c.is_ascii_alphabetic() || c == '_' => {
					chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
				}
				_ => false,
			}
		})
	}

	fn is_function_version(s: &str) -> bool {
		!s.is_empty()
			&& s.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
	}

	#[cfg(test)]
	mod tests {
		use super::parse;

		#[test]
		fn accepts_well_formed_names() {
			assert_eq!(parse("fn::greet").unwrap(), ("fn::greet".to_owned(), None));
			assert_eq!(parse("time::now").unwrap(), ("time::now".to_owned(), None));
			assert_eq!(parse("_private").unwrap(), ("_private".to_owned(), None));
			assert_eq!(
				parse("fn::greet<1.0.0>").unwrap(),
				("fn::greet".to_owned(), Some("1.0.0".to_owned()))
			);
		}

		#[test]
		fn rejects_sql_injection_attempts() {
			assert!(parse("SELECT * FROM secret; --").is_err());
			assert!(parse("foo(); DROP TABLE bar").is_err());
			assert!(parse("a b").is_err());
			assert!(parse("").is_err());
			assert!(parse("1abc").is_err());
			assert!(parse("foo::").is_err());
			assert!(parse("::bar").is_err());
		}

		#[test]
		fn rejects_bad_versions() {
			assert!(parse("foo<>").is_err());
			assert!(parse("foo<1.0; DROP TABLE x>").is_err());
			assert!(parse("foo<bad version>").is_err());
		}
	}
}
