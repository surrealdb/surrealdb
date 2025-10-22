use std::fmt::{self, Display};

use anyhow::Result;
use reblessive::tree::Stk;
use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned};

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::val::Value;

mod field;
mod sequence;
mod table;

pub(crate) use field::{AlterDefault, AlterFieldStatement};
pub(crate) use sequence::AlterSequenceStatement;
pub(crate) use table::AlterTableStatement;

use crate::expr::Expr;
use crate::expr::expression::VisitExpression;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) enum AlterKind<T> {
	#[default]
	None,
	Set(T),
	Drop,
}

impl<T: Revisioned> Revisioned for AlterKind<T> {
	fn revision() -> u16 {
		1
	}
}

impl<T: Revisioned + SerializeRevisioned> SerializeRevisioned for AlterKind<T> {
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		w: &mut W,
	) -> std::result::Result<(), revision::Error> {
		SerializeRevisioned::serialize_revisioned(&Self::revision(), w)?;
		match self {
			AlterKind::None => SerializeRevisioned::serialize_revisioned(&0u32, w)?,
			AlterKind::Set(x) => {
				SerializeRevisioned::serialize_revisioned(&1u32, w)?;
				SerializeRevisioned::serialize_revisioned(x, w)?;
			}
			AlterKind::Drop => {
				SerializeRevisioned::serialize_revisioned(&2u32, w)?;
			}
		}
		Ok(())
	}
}

impl<T: Revisioned + DeserializeRevisioned> DeserializeRevisioned for AlterKind<T> {
	fn deserialize_revisioned<R: std::io::Read>(
		r: &mut R,
	) -> std::result::Result<Self, revision::Error>
	where
		Self: Sized,
	{
		match DeserializeRevisioned::deserialize_revisioned(r)? {
			1u16 => {
				let variant: u32 = DeserializeRevisioned::deserialize_revisioned(r)?;
				match variant {
					0 => Ok(AlterKind::None),
					1 => Ok(AlterKind::Set(DeserializeRevisioned::deserialize_revisioned(r)?)),
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
pub(crate) enum AlterStatement {
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
			Self::Sequence(v) => v.compute(stk, ctx, opt, doc).await,
			Self::Field(v) => v.compute(stk, ctx, opt, doc).await,
		}
	}
}

impl VisitExpression for AlterStatement {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		if let AlterStatement::Field(AlterFieldStatement {
			name,
			..
		}) = self
		{
			name.visit(visitor);
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
