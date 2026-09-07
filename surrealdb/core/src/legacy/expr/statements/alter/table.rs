use std::borrow::Cow;
use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use tracing::instrument;

use crate::catalog::providers::TableProvider;
use crate::catalog::{Error, TableType};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::alter::table::AlterTableStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::schema::{EdgeCachePrefix, RefCachePrefix, TblRoot};
use crate::legacy::expr_to_ident;
use crate::val::{TableName, Value};

/// Computes the effect of the `ALTER TABLE` statement.
///
/// Permissions: requires `Action::Edit` on `ResourceKind::Table`.
///
/// Side effects:
/// - May write table definition metadata
/// - May compact the underlying storage if `compact` is true
/// - May create relation helper fields when switching to `RELATION`
#[instrument(level = "trace", name = "AlterTableStatement::compute", skip_all)]
pub(crate) async fn alter_table_statement_compute(
	this: &AlterTableStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Table, Base::Db)?;
	let name = TableName::new(expr_to_ident(stk, ctx, opt, doc, &this.name, "table name").await?);
	// Get the NS and DB
	let (ns_name, db_name) = opt.ns_db()?;
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	// Fetch the transaction
	let txn = ctx.tx();

	// Get the table definition
	let previous = match txn.get_tb(ns, db, &name, None).await? {
		Some(tb) => tb,
		None => {
			if this.if_exists {
				return Ok(Value::None);
			} else {
				return Err(Error::TbNotFound {
					name: name.clone(),
				}
				.into());
			}
		}
	};
	let mut dt = previous.deref().clone();
	// Process the statement
	match this.schemafull {
		AlterKind::Set(_) => dt.schemafull = true,
		AlterKind::Drop => dt.schemafull = false,
		AlterKind::None => {}
	}

	if let Some(permissions) = &this.permissions {
		dt.permissions = permissions.clone();
	}

	let mut changefeed_replaced = false;
	// Distinct from `changefeed_replaced`: setting a retention on a table that had
	// none also raises its database's collection watermark, and only a *set* can
	// extend it — dropping one can only lower the maximum, which strands nothing.
	let mut changefeed_set = false;
	match this.changefeed {
		AlterKind::Set(x) => {
			changefeed_replaced = dt.changefeed.is_some();
			changefeed_set = true;
			dt.changefeed = Some(x)
		}
		AlterKind::Drop => dt.changefeed = None,
		AlterKind::None => {}
	}

	match this.comment {
		AlterKind::Set(ref x) => dt.comment = Some(x.clone()),

		AlterKind::Drop => dt.comment = None,
		AlterKind::None => {}
	}

	let old_edges_cap = dt.inline_edges_cap;
	let old_refs_cap = dt.inline_refs_cap;
	for (kind, cap) in [
		(&this.inline_edges_cap, &mut dt.inline_edges_cap),
		(&this.inline_refs_cap, &mut dt.inline_refs_cap),
	] {
		match kind {
			AlterKind::Set(x) => {
				// The caps rewrite a whole cache value per maintained entry,
				// so their cost grows quadratically with the cap; the hard
				// bound keeps that below the cursor cost the cache replaces.
				anyhow::ensure!(
					*x <= crate::idx::inline_cache::MAX_INLINE_CACHE_CAP,
					crate::exec::Error::Thrown(format!(
						"an INLINE cache cap cannot exceed {}",
						crate::idx::inline_cache::MAX_INLINE_CACHE_CAP
					))
				);
				*cap = Some(*x);
			}
			AlterKind::Drop => *cap = None,
			AlterKind::None => {}
		}
	}

	if let Some(kind) = &this.kind {
		dt.table_type = kind.clone();
	}

	// Validate (and normalise) a lightweight relation definition against
	// the table's previous shape and current contents.
	crate::legacy::expr::statements::define::table::validate_lightweight_table(
		&txn,
		ns,
		db,
		Some(previous.deref()),
		&mut dt,
	)
	.await?;
	// A type change away from RELATION must not orphan INLINE fields.
	crate::legacy::expr::statements::define::table::validate_inline_fields_survive_type_change(
		&txn,
		ns,
		db,
		Some(previous.deref()),
		&dt,
	)
	.await?;

	// Add table relational fields
	if matches!(this.kind, Some(TableType::Relation(_))) {
		crate::legacy::define_table_statement_add_in_out_fields(&txn, ns, db, &mut dt).await?;
	}

	// Record definition change
	if changefeed_replaced {
		txn.changefeed_buffer_table_change(ns, db, &name, &dt.to_stored());
	}

	if this.compact {
		let key = TblRoot {
			ns,
			db,
			tb: Cow::Borrowed(&name),
		};
		txn.compact(&key).await?;
	}

	// Set the table definition
	txn.replace_tb(ns_name, db_name, &dt).await?;
	// A retention written here makes the changefeed collector's watermark stale,
	// so the fence is bumped in the same transaction: a collection page that
	// armed against it is rejected rather than deleting under the old policy.
	if changefeed_set {
		txn.bump_changefeed_retention_fence(ns, db).await?;
	}

	// A cache is only trustworthy under the cap regime it was built under,
	// so any change to a cap — set, raised, lowered or dropped — deletes the
	// table's cache subspace in this transaction. Caches re-materialise
	// lazily from the first eligible write.
	//
	// This delete only removes the keys its own scan observes at this
	// transaction's snapshot; it carries no range-level conflict of its
	// own, on any backend. A first-eligible-write backfill running
	// concurrently under the old cap reads and writes only the one cache
	// key it backfills, which this scan has no reason to touch when that
	// key does not exist yet — so if that backfill's write commits after
	// this scan has already passed its key by, the new entry survives
	// into the new cap regime unremoved, and is then read as though it
	// belonged to it. Closing this fully needs either a range-conflict
	// primitive this repository's transaction API does not have, or a
	// generation stamped into the cache key or value that a reader can
	// compare against the table's current cap generation and ignore on
	// mismatch — both are schema/infrastructure changes broader than this
	// statement.
	if old_edges_cap != dt.inline_edges_cap {
		let key = EdgeCachePrefix {
			ns,
			db,
			tb: Cow::Borrowed(&name),
		};
		txn.del_prefix_key(&key).await?;
	}
	if old_refs_cap != dt.inline_refs_cap {
		let key = RefCachePrefix {
			ns,
			db,
			tb: Cow::Borrowed(&name),
		};
		txn.del_prefix_key(&key).await?;
	}

	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
