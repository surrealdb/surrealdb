use std::fmt::{self, Display};

use anyhow::Result;
use reblessive::tree::Stk;
use revision::Revisioned;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::val::Value;

mod field;
mod sequence;
mod table;

pub use field::{AlterDefault, AlterFieldStatement};
pub use sequence::AlterSequenceStatement;
pub use table::AlterTableStatement;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub enum AlterKind<T> {
	#[default]
	None,
	Set(T),
	Drop,
}

impl<T: Revisioned> Revisioned for AlterKind<T> {
	fn revision() -> u16 {
		1
	}

	fn serialize_revisioned<W: std::io::Write>(
		&self,
		w: &mut W,
	) -> std::result::Result<(), revision::Error> {
		Self::revision().serialize_revisioned(w)?;
		match self {
			AlterKind::None => 0u32.serialize_revisioned(w)?,
			AlterKind::Set(x) => {
				1u32.serialize_revisioned(w)?;
				x.serialize_revisioned(w)?;
			}
			AlterKind::Drop => {
				2u32.serialize_revisioned(w)?;
			}
		}
		Ok(())
	}

	fn deserialize_revisioned<R: std::io::Read>(
		r: &mut R,
	) -> std::result::Result<Self, revision::Error>
	where
		Self: Sized,
	{
		match u16::deserialize_revisioned(r)? {
			1 => {
				let variant = u32::deserialize_revisioned(r)?;
				match variant {
					0 => Ok(AlterKind::None),
					1 => Ok(AlterKind::Set(Revisioned::deserialize_revisioned(r)?)),
					2 => Ok(AlterKind::Drop),
					x => Err(revision::Error::Deserialize(format!(
						"Unknown variant `{x}` for AlterKind"
					))),
				}
			}
			x => Err(revision::Error::Deserialize(format!("Unknown revision `{x}` for AlterKind"))),
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum AlterStatement {
	Table(AlterTableStatement),
	Sequence(AlterSequenceStatement),
	Field(AlterFieldStatement),
}

impl AlterStatement {
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
