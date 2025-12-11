//! Index operation helpers for constructing and maintaining KV index entries.
//!
//! This module orchestrates index mutations for a single document: removing old
//! entries and inserting new ones across different index types (UNIQUE, regular,
//! search, fulltext, etc.). Index keys are built with key::index and field
//! values are encoded via key::value::Array.
//!
//! Numeric normalization in keys:
//! - Array normalizes Number values (Int/Float/Decimal) using a lexicographic numeric encoding so
//!   that byte-wise order matches numeric order. As a result, numerically equal values (e.g., 0,
//!   0.0, 0dec) map to the same key bytes.
//! - UNIQUE index behavior leverages this: equal numerics across variants will collide on the same
//!   index key and cause a uniqueness violation.
//!
//! Range scans and lookups benefit because a single probe/range can be used for
//! numeric predicates without fanning out per numeric variant.

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::{DatabaseDefinition, IndexDefinition, TableDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::{Force, Options};
use crate::doc::{CursorDoc, Document};
use crate::expr::FlowResultExt as _;
use crate::idx::index::IndexOperation;
use crate::kvs::index::ConsumeResult;
use crate::val::{RecordId, Value};

impl Document {
	pub(super) async fn store_index_data(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<()> {
		// Collect indexes or skip
		let ixs = match &opt.force {
			Force::All => self.ix(ctx, opt).await?,
			_ if self.changed() => self.ix(ctx, opt).await?,
			_ => return Ok(()),
		};
		// Check if the table is a view
		let tb = self.tb(ctx, opt).await?;
		if tb.drop {
			return Ok(());
		}

		let db = self.db(ctx, opt).await?;

		// Get the record id
		let rid = self.id()?;
		// Loop through all index statements
		for ix in ixs.iter() {
			// Decommissioned indexes are ignored
			if ix.prepare_remove {
				continue;
			}
			// Calculate old values
			let o = Self::build_opt_values(stk, ctx, opt, ix, &self.initial).await?;

			// Calculate new values
			let n = Self::build_opt_values(stk, ctx, opt, ix, &self.current).await?;

			// Update the index entries
			if o != n {
				Self::one_index(&db, &tb, stk, ctx, opt, ix, o, n, &rid).await?;
			}
		}
		// Carry on
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn one_index(
		db: &DatabaseDefinition,
		tb: &TableDefinition,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		ix: &IndexDefinition,
		o: Option<Vec<Value>>,
		n: Option<Vec<Value>>,
		rid: &RecordId,
	) -> Result<()> {
		let (o, n) = if let Some(ib) = ctx.get_index_builder() {
			match ib.consume(db, ctx, ix, o, n, rid).await? {
				// The index builder consumed the value, which means it is currently building the
				// index asynchronously, we don't index the document and let the index builder
				// do it later.
				ConsumeResult::Enqueued => return Ok(()),
				// The index builder is done, the index has been built; we can proceed normally
				ConsumeResult::Ignored(o, n) => (o, n),
			}
		} else {
			(o, n)
		};

		// Store all the variables and parameters required by the index operation
		let mut ic = IndexOperation::new(
			ctx,
			opt,
			db.namespace_id,
			db.database_id,
			tb.table_id,
			ix,
			o,
			n,
			rid,
		);
		// Keep track of compaction requests, we need to trigger them after the index operation
		let mut require_compaction = false;
		// Execute the index operation
		ic.compute(stk, &mut require_compaction).await?;
		// Did any compaction request have to be triggered?
		if require_compaction {
			ic.trigger_compaction().await?;
		}
		Ok(())
	}

	/// Extract from the given document, the values required by the index and put then in an array.
	/// Eg. IF the index is composed of the columns `name` and `instrument`
	/// Given this doc: { "id": 1, "instrument":"piano", "name":"Tobie" }
	/// It will return: ["Tobie", "piano"]
	pub(crate) async fn build_opt_values(
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		ix: &IndexDefinition,
		doc: &CursorDoc,
	) -> Result<Option<Vec<Value>>> {
		if doc.doc.as_ref().is_nullish() {
			return Ok(None);
		}
		let mut o = Vec::with_capacity(ix.cols.len());
		for i in ix.cols.iter() {
			let v = i.compute(stk, ctx, opt, Some(doc)).await.catch_return()?;
			o.push(v);
		}
		Ok(Some(o))
	}
}
