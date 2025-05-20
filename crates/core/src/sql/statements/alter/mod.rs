mod field;
mod sequence;
mod table;

pub use field::AlterFieldStatement;
pub use sequence::AlterSequenceStatement;
pub use table::AlterTableStatement;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::sql::value::SqlValue;
use anyhow::Result;

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum AlterStatement {
	Table(AlterTableStatement),
	#[revision(start = 2)]
	Sequence(AlterSequenceStatement),
	#[revision(start = 3)]
	Field(AlterFieldStatement),
}

impl Display for AlterStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Table(v) => Display::fmt(v, f),
			Self::Sequence(v) => Display::fmt(v, f),
			Self::Field(v) => Display::fmt(v, f),
		}
	}
}

impl From<AlterStatement> for crate::expr::statements::AlterStatement {
	fn from(v: AlterStatement) -> Self {
		match v {
			AlterStatement::Table(v) => Self::Table(v.into()),
			AlterStatement::Sequence(v) => Self::Sequence(v.into()),
			AlterStatement::Field(v) => Self::Field(v.into()),
		}
	}
}

impl From<crate::expr::statements::AlterStatement> for AlterStatement {
	fn from(v: crate::expr::statements::AlterStatement) -> Self {
		match v {
			crate::expr::statements::AlterStatement::Table(v) => Self::Table(v.into()),
			crate::expr::statements::AlterStatement::Sequence(v) => Self::Sequence(v.into()),
			crate::expr::statements::AlterStatement::Field(v) => Self::Field(v.into()),
		}
	}
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::sql::{Ident, Idiom};

	#[test]
	fn check_alter_serialize_table() {
		let stm = AlterStatement::Table(AlterTableStatement {
			name: Ident::from("test"),
			..Default::default()
		});
		let enc: Vec<u8> = revision::to_vec(&stm).unwrap();
		assert_eq!(15, enc.len());
	}

	#[test]
	fn check_alter_serialize_sequence() {
		let stm = AlterStatement::Sequence(AlterSequenceStatement {
			name: Ident::from("test"),
			..Default::default()
		});
		let enc: Vec<u8> = revision::to_vec(&stm).unwrap();
		assert_eq!(11, enc.len());
	}

	#[test]
	fn check_alter_serialize_field() {
		let stm = AlterStatement::Field(AlterFieldStatement {
			name: Idiom::from("test"),
			what: Ident::from("test"),
			..Default::default()
		});
		let enc: Vec<u8> = revision::to_vec(&stm).unwrap();
		assert_eq!(30, enc.len());
	}
}
