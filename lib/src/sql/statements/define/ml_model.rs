use std::fmt;

use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::sql::{Ident, Strand};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct DefineModelStatement {
	pub name: Ident,
	pub version: String,
	pub comment: Option<Strand>,
	pub model: Vec<u8>, // TODO
}

impl fmt::Display for DefineModelStatement {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "DEFINE MODEL fn::{}<{}>", self.name, self.version)?;
		if let Some(comment) = self.comment.as_ref() {
			write!(f, "COMMENT {}", comment)?;
		}
		Ok(())
	}
}
