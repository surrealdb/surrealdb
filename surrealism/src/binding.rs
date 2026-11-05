//! Values the extension's definition binds by name.
//!
//! The extension declares each name and its kind under `[bindings]` in
//! `package.toml`, and whoever defines it gives each one an expression:
//!
//! ```toml
//! [bindings]
//! api_key = { type = "string", description = "Mailgun API key", sensitive = true }
//! author = "option<string>"
//! ```
//!
//! ```surql
//! DEFINE EXTENSION ext::mailer FROM silo::acme::mailer<1.2.0> UNSIGNED
//!     BINDINGS { api_key: secret::get("mailgun") };
//! ```
//!
//! ```ignore
//! #[surrealism]
//! async fn send(to: String, body: String) -> anyhow::Result<()> {
//!     let key: String = surrealism::binding("api_key").await?;
//!     let author: Option<String> = surrealism::binding("author").await?;
//!     // …
//! }
//! ```
//!
//! The host evaluates the expression on the first read in an invocation,
//! with permissions off and the caller capped at the definer's level, so a
//! binding can hold a value the caller may not read. Any function may read a binding, whether or
//! not it declared a [`Transaction`](crate::Transaction). `init` cannot: it runs at build time,
//! before any definition exists.

use anyhow::Result;
use surrealdb_types::{SurrealValue, Value};

use crate::bindings::surrealism::extension::host;

/// The value the definition binds to `name`, decoded as `R`.
///
/// An `option<…>` name the definition leaves out reads as `NONE`, so read
/// such a name as an `Option`. A name `package.toml` does not declare is an
/// error.
pub async fn binding<R: SurrealValue>(name: impl Into<String>) -> Result<R> {
	let name = name.into();
	let bytes =
		host::binding(name.clone()).await.map_err(|e| crate::tx::host_err("binding", &e))?;
	let value = match bytes {
		Some(bytes) => surrealdb_types::decode(&bytes)?,
		None => Value::None,
	};
	R::from_value(value).map_err(|e| anyhow::anyhow!("binding `{name}`: {e}"))
}
