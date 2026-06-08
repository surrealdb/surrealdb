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

impl revision::SkipRevisioned for Block {
	fn skip_revisioned<R: std::io::Read>(reader: &mut R) -> Result<(), revision::Error> {
		<String as revision::SkipRevisioned>::skip_revisioned(reader)
	}
}

impl revision::WalkRevisioned for Block {
	type Walker<'r, R: revision::BorrowedReader + 'r> = revision::LeafWalker<'r, Block, R>;

	fn walk_revisioned<'r, R: revision::BorrowedReader>(
		reader: &'r mut R,
	) -> Result<Self::Walker<'r, R>, revision::Error> {
		Ok(revision::LeafWalker::new(reader))
	}
}

impl revision::LengthPrefixedBytes for Block {}

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
		let mut ctx = Some(Context::new_child(ctx).freeze());
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
		Value::String(self.to_sql().into())
	}
}

#[cfg(test)]
mod length_prefixed_bytes_tests {
	use revision::{SerializeRevisioned, WalkRevisioned};
	use surrealdb_types::ToSql;

	use super::Block;

	#[test]
	fn block_with_bytes_matches_serialize() {
		let block = Block::default();
		let mut bytes = Vec::new();
		block.serialize_revisioned(&mut bytes).unwrap();
		let wire_text = block.to_sql();
		let mut r = bytes.as_slice();
		let walker = Block::walk_revisioned(&mut r).unwrap();
		let observed = walker.with_bytes(|raw| raw.to_vec()).unwrap();
		assert_eq!(observed.as_slice(), wire_text.as_bytes());
		assert!(r.is_empty());
	}
}
