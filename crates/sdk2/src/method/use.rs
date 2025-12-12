use anyhow::Result;
use surrealdb_types::Nullable;

use crate::{api::SurrealContext, method::{Executable, Request}};

#[derive(Clone)]
pub struct UseNamespaceDatabase {
	pub(crate) namespace: Nullable<String>,
	pub(crate) database: Nullable<String>,
}

impl UseNamespaceDatabase {
	pub(crate) fn req_from_namespace<C: SurrealContext + ?Sized,T: Into<NullableString>>(ctx: &C, namespace: T) -> Request<Self> {
		let namespace: NullableString = namespace.into();
		Request::new(ctx, Self {
			namespace: namespace.into(),
			database: Nullable::None,
		})
	}

	pub(crate) fn req_from_database<C: SurrealContext + ?Sized,T: Into<NullableString>>(ctx: &C, database: T) -> Request<Self> {
		let database: NullableString = database.into();
		Request::new(ctx, Self {
			namespace: Nullable::None,
			database: database.into(),
		})
	}
}

impl Request<UseNamespaceDatabase> {
	pub fn use_ns<T: Into<NullableString>>(mut self, namespace: T) -> Self {
		let namespace: NullableString = namespace.into();
		self.inner.namespace = namespace.into();
		self
	}

	pub fn use_db<T: Into<NullableString>>(mut self, database: T) -> Self {
		let database: NullableString = database.into();
		self.inner.database = database.into();
		self
	}
}

impl Executable for UseNamespaceDatabase {
	type Output = (Option<String>, Option<String>);

	fn execute(req: Request<Self>) -> impl Future<Output = Result<Self::Output>> + Send {
		async move {
			req.controller
				.r#use(
					req.session_id,
					req.inner.namespace.clone().into(),
					req.inner.database.clone().into(),
				)
				.await
		}
	}
}

#[derive(Clone)]
pub struct UseDefaults;

impl Executable for UseDefaults {
	type Output = (Option<String>, Option<String>);

	fn execute(req: Request<Self>) -> impl Future<Output = Result<Self::Output>> + Send {
		async move {
			req.controller.r#use(req.session_id, Nullable::None, Nullable::None).await
		}
	}
}

pub struct NullableString(Option<String>);
impl From<Option<String>> for NullableString {
	fn from(value: Option<String>) -> Self {
		NullableString(value)
	}
}

impl From<String> for NullableString {
	fn from(value: String) -> Self {
		NullableString(Some(value))
	}
}

impl From<&str> for NullableString {
	fn from(value: &str) -> Self {
		NullableString(Some(value.to_string()))
	}
}

impl From<()> for NullableString {
	fn from(_: ()) -> Self {
		NullableString(None)
	}
}

impl From<NullableString> for Nullable<String> {
	fn from(value: NullableString) -> Self {
		if let Some(value) = value.0 {
			Nullable::Some(value)
		} else {
			Nullable::Null
		}
	}
}
