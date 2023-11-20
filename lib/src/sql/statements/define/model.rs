use crate::sql::{
	fmt::{is_pretty, pretty_indent},
	Permission,
};
use async_recursion::async_recursion;
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Write;

use crate::{
	ctx::Context,
	dbs::{Options, Transaction},
	doc::CursorDoc,
	err::Error,
	sql::{Ident, Strand, Value},
};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct DefineModelStatement {
	pub name: Ident,
	pub version: String,
	pub comment: Option<Strand>,
	pub permissions: Permission,
}

impl fmt::Display for DefineModelStatement {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "DEFINE MODEL ml::{}<{}>", self.name, self.version)?;
		if let Some(comment) = self.comment.as_ref() {
			write!(f, "COMMENT {}", comment)?;
		}
		if !self.permissions.is_full() {
			let _indent = if is_pretty() {
				Some(pretty_indent())
			} else {
				f.write_char(' ')?;
				None
			};
			write!(f, "PERMISSIONS {}", self.permissions)?;
		}
		Ok(())
	}
}

impl DefineModelStatement {
	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		_opt: &Options,
		_txn: &Transaction,
		_doc: Option<&'async_recursion CursorDoc<'_>>,
	) -> Result<Value, Error> {
		Err(Error::Unimplemented("Ml model definition not yet implemented".to_string()))
	}
}
