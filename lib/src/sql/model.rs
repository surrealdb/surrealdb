use crate::{
	ctx::Context,
	dbs::{Options, Transaction},
	doc::CursorDoc,
	err::Error,
	sql::value::Value,
};
use async_recursion::async_recursion;
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct Model {
	pub name: String,
	pub version: String,
	pub args: Vec<Value>,
}

impl fmt::Display for Model {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ml::{}<{}>(", self.name, self.version)?;
		for (idx, p) in self.args.iter().enumerate() {
			if idx != 0 {
				write!(f, ",")?;
			}
			write!(f, "{}", p)?;
		}
		write!(f, ")")
	}
}

impl Model {
	#[cfg_attr(not(target_arch = "wasm32"), async_recursion)]
	#[cfg_attr(target_arch = "wasm32", async_recursion(?Send))]
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		_opt: &Options,
		_txn: &Transaction,
		_doc: Option<&'async_recursion CursorDoc<'_>>,
	) -> Result<Value, Error> {
		Err(Error::Unimplemented("ML model evaluation not yet implemented".to_string()))
	}
}
