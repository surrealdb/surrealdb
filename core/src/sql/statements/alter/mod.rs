mod field;
mod table;

pub use field::AlterFieldStatement;
pub use table::AlterTableStatement;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::value::Value;
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum AlterStatement {
	Table(AlterTableStatement),
	#[revision(start = 2)]
	Field(AlterFieldStatement),
}

impl AlterStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		true
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		match self {
			Self::Table(ref v) => v.compute(stk, ctx, opt, doc).await,
			Self::Field(ref v) => v.compute(stk, ctx, opt, doc).await,
		}
	}
}

impl Display for AlterStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Table(v) => Display::fmt(v, f),
			Self::Field(v) => Display::fmt(v, f),
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::Ident;

	#[test]
	fn check_alter_serialize() {
		let stm = AlterStatement::Table(AlterTableStatement {
			name: Ident::from("test"),
			..Default::default()
		});
		let enc: Vec<u8> = stm.into();
		assert_eq!(16, enc.len());
	}
}
