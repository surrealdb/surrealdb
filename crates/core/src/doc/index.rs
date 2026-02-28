use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::{Force, Statement};
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::idx::index::IndexOperation;
use crate::kvs::index::ConsumeResult;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Thing, Value};
use reblessive::tree::Stk;

impl Document {
	pub(super) async fn store_index_data(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Collect indexes or skip
		let ixs = match &opt.force {
			Force::All => self.ix(ctx, opt).await?,
			_ if self.changed() => self.ix(ctx, opt).await?,
			_ => return Ok(()),
		};
		// Check if the table is a view
		if self.tb(ctx, opt).await?.drop {
			return Ok(());
		}
		// Get the record id
		let rid = self.id()?;
		// Loop through all index statements
		for ix in ixs.iter() {
			// Calculate old values
			let o = Self::build_opt_values(stk, ctx, opt, ix, &self.initial).await?;

			// Calculate new values
			let n = Self::build_opt_values(stk, ctx, opt, ix, &self.current).await?;

			// Update the index entries
			if o != n {
				Self::one_index(stk, ctx, opt, ix, o, n, &rid).await?;
			}
		}
		// Carry on
		Ok(())
	}

	async fn one_index(
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		ix: &DefineIndexStatement,
		o: Option<Vec<Value>>,
		n: Option<Vec<Value>>,
		rid: &Thing,
	) -> Result<(), Error> {
		let (o, n) = if let Some(ib) = ctx.get_index_builder() {
			match ib.consume(ctx, opt, ix, o, n, rid).await? {
				// The index builder consumed the value, which means it is currently building the index asynchronously,
				// we don't index the document and let the index builder do it later.
				ConsumeResult::Enqueued => return Ok(()),
				// The index builder is done, the index has been built, we can proceed normally
				ConsumeResult::Ignored(o, n) => (o, n),
			}
		} else {
			(o, n)
		};

		// Store all the variables and parameters required by the index operation
		let mut ic = IndexOperation::new(ctx, opt, ix, o, n, rid);
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
		ctx: &Context,
		opt: &Options,
		ix: &DefineIndexStatement,
		doc: &CursorDoc,
	) -> Result<Option<Vec<Value>>, Error> {
		if !doc.doc.as_ref().is_some() {
			return Ok(None);
		}
		let mut o = Vec::with_capacity(ix.cols.len());
		for i in ix.cols.iter() {
			let v = i.compute(stk, ctx, opt, Some(doc)).await?;
			o.push(v);
		}
		Ok(Some(o))
	}
}
