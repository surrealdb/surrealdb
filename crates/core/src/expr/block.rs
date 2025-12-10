use std::ops::Deref;

use reblessive::tree::Stk;
use revision::{DeserializeRevisioned, Revisioned, SerializeRevisioned};
use surrealdb_types::ToSql;

use super::FlowResult;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct Block(pub(crate) Vec<Expr>);

impl Revisioned for Block {
	fn revision() -> u16 {
		1
	}
}

impl SerializeRevisioned for Block {
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		self.to_sql().serialize_revisioned(writer)?;
		Ok(())
	}
}

impl DeserializeRevisioned for Block {
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		let query: String = DeserializeRevisioned::deserialize_revisioned(reader)?;

		let expr = crate::syn::block(&query)
			.map_err(|err| revision::Error::Conversion(err.to_string()))?;
		Ok(expr.into())
	}
}

impl Deref for Block {
	type Target = [Expr];
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Block {
	/// Check if this block does only reads.
	pub(crate) fn read_only(&self) -> bool {
		self.0.iter().all(|x| x.read_only())
	}

	/// Process this type returning a computed simple Value
	#[instrument(level = "trace", name = "Block::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		// Duplicate context
		let mut ctx = Some(Context::new(ctx).freeze());
		// Loop over the statements
		let mut res = Value::None;
		for v in self.iter() {
			match v {
				Expr::Let(x) => res = x.compute(stk, &mut ctx, opt, doc).await?,
				v => {
					res = stk
						.run(|stk| {
							v.compute(
								stk,
								ctx.as_ref().expect("context should be initialized"),
								opt,
								doc,
							)
						})
						.await?
				}
			}
		}
		// Return nothing
		Ok(res)
	}
}

impl ToSql for Block {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		let block: crate::sql::Block = self.clone().into();
		block.fmt_sql(f, fmt);
	}
}

impl InfoStructure for Block {
	fn structure(self) -> Value {
		Value::String(self.to_sql())
	}
}
