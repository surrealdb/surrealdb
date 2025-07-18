use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::val::Value;
use anyhow::Result;
use reblessive::tree::Stk;
use revision::{Revisioned, revisioned};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

mod field;
mod sequence;
mod table;

pub use field::{AlterDefault, AlterFieldStatement};
pub use sequence::AlterSequenceStatement;
pub use table::AlterTableStatement;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum AlterKind<T> {
	#[default]
	None,
	Set(T),
	Drop,
}

impl<T> Revisioned for AlterKind<T> {
	fn revision() -> u16 {
		1
	}

	fn serialize_revisioned<W: std::io::Write>(
		&self,
		w: &mut W,
	) -> std::result::Result<(), revision::Error> {
		// TODO: implement this
		todo!()
	}

	fn deserialize_revisioned<R: std::io::Read>(
		r: &mut R,
	) -> std::result::Result<Self, revision::Error>
	where
		Self: Sized,
	{
		todo!()
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum AlterStatement {
	Table(AlterTableStatement),
	Sequence(AlterSequenceStatement),
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
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		match self {
			Self::Table(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Sequence(v) => v.compute(ctx, opt).await,
			Self::Field(v) => v.compute(stk, ctx, opt, doc).await,
		}
	}
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

#[cfg(test)]
mod tests {

	use super::*;
	use crate::expr::{Ident, Idiom};

	#[test]
	fn check_alter_serialize_table() {
		let stm = AlterStatement::Table(AlterTableStatement {
			name: Ident::new("test".to_owned()).unwrap(),
			..Default::default()
		});
		let enc: Vec<u8> = revision::to_vec(&stm).unwrap();
		assert_eq!(15, enc.len());
	}

	#[test]
	fn check_alter_serialize_sequence() {
		let stm = AlterStatement::Sequence(AlterSequenceStatement {
			name: Ident::new("test".to_owned()).unwrap(),
			..Default::default()
		});
		let enc: Vec<u8> = revision::to_vec(&stm).unwrap();
		assert_eq!(11, enc.len());
	}

	#[test]
	fn check_alter_serialize_field() {
		let stm = AlterStatement::Field(AlterFieldStatement {
			name: Idiom::field(Ident::new("test".to_owned()).unwrap()),
			what: Ident::new("test".to_owned()).unwrap(),
			..Default::default()
		});
		let enc: Vec<u8> = revision::to_vec(&stm).unwrap();
		assert_eq!(30, enc.len());
	}
}
