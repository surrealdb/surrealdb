use std::borrow::Cow;

use anyhow::Result;
use surrealdb_strand::TableName;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::cf::{ChangeSet, DatabaseMutation, TableMutations};
use crate::exec::Error as ExecError;
use crate::expr::statements::show::ShowSince;
use crate::key::schema::{ChangeFeedKey, ChangeFeedPrefix};
use crate::key::{KVKeyDecode, KVValue};
use crate::kvs::Transaction;

/// The changes `SHOW CHANGES` reports: the change sets of the selected
/// database, or of one table in it, from `this.since`, at most `this.limit`
/// of them, for a principal allowed to view the database's tables.
pub(crate) async fn show_changes(
	this: &crate::expr::statements::show::ShowStatement,
	ctx: &crate::ctx::FrozenContext,
	opt: &crate::dbs::Options,
) -> Result<crate::val::Value> {
	let changes = changes(ctx, opt, this.table.as_ref(), this.since.clone(), this.limit).await?;
	Ok(changes.into())
}

/// The change sets `SHOW CHANGES` reports, each as the value it shows: the session's permission
/// to view the database's tables is checked first, then the selected namespace and database are
/// resolved, raising where either is missing, then the feed is read as [`read`] reads it.
pub(crate) async fn changes(
	ctx: &crate::ctx::FrozenContext,
	opt: &crate::dbs::Options,
	table: Option<&TableName>,
	since: ShowSince,
	limit: Option<u32>,
) -> Result<Vec<crate::val::Value>> {
	use crate::iam::{Action, ResourceKind};
	ctx.is_allowed(opt, Action::View, ResourceKind::Table, crate::expr::Base::Db)?;
	let txn = ctx.tx();
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let changes = read(&txn, ns, db, table, since, limit).await?;
	changes.iter().cloned().map(|change| change.into_value()).collect()
}

// Reads the change feed for a specific database or a table,
// starting from a specific timestamp or version number.
//
// The limit parameter is the maximum number of change sets to return.
// If the limit is not specified, the default is 100.
//
// You can use this to read the change feed in chunks.
// The second call would start from the last timestamp/version + 1 of the first call.
pub async fn read(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	tb: Option<&TableName>,
	start: ShowSince,
	limit: Option<u32>,
) -> Result<Vec<ChangeSet>> {
	let ts_impl = tx.timestamp_impl();

	// Calculate the start of the changefeed range
	let ts = match start {
		ShowSince::Versionstamp(x) => {
			ts_impl.create_from_versionstamp(x as u128).ok_or_else(|| ExecError::Query {
				message: format!(
					"Invalid versionstamp `{x}`, outside of range for kv-store timestamps"
				),
			})?
		}
		ShowSince::Timestamp(x) => {
			ts_impl.create_from_datetime(x.0).ok_or_else(|| ExecError::Query {
				message: format!(
					"Invalid versionstamp `{x}`, outside of range for kv-store timestamps"
				),
			})?
		}
	};

	let buf = &mut [0u8; _];
	let ts_bytes = ts.encode(buf);

	// Everything from the requested timestamp to the end of this database's feed.
	let range = ChangeFeedPrefix {
		ns,
		db,
	}
	.range_where(Cow::Borrowed(ts_bytes)..)?;

	// The row limit defaults to 100 and is capped at 1000.
	let limit = limit.unwrap_or(100).min(1000);
	// Timestamp of the change set being built, or None before the first entry.
	let mut current_ts: Option<Vec<u8>> = None;
	// Table mutations of the change set being built.
	let mut buf: Vec<TableMutations> = Vec::new();
	// Completed change sets, in scan order.
	let mut res = Vec::<ChangeSet>::new();
	// Debug-only guard: the scan is ascending so versionstamps must be monotonic.
	#[cfg(debug_assertions)]
	let mut prev_ts: Option<Vec<u8>> = None;

	// Entries arrive in ascending versionstamp order. Values stay encoded until the table
	// filter below has run, so an entry for another table is never decoded and cannot
	// fail a read of the requested table.
	for (k, v) in tx.scan_raw(range, limit, 0, None).await? {
		#[cfg(debug_assertions)]
		trace!("Reading change feed entry: {}", crate::key::Key::from(k.as_slice()));

		// Decode the changefeed entry key
		let key = ChangeFeedKey::decode_key(&k)?;

		// Invariant: each entry's versionstamp is >= the previous entry's. Stamps are
		// monotonic within a node, and the backend timestamp oracle orders them across nodes.
		#[cfg(debug_assertions)]
		{
			if let Some(p) = &prev_ts {
				assert!(
					key.ts.as_ref() >= p.as_slice(),
					"changefeed scan returned out-of-order versionstamps"
				);
			}
			prev_ts = Some(key.ts.to_vec());
		}

		// Check the change is for the desired table
		if tb.is_some_and(|tb| *tb != *key.tb) {
			continue;
		}
		// Decode the mutations only for entries that passed the table filter
		let v = TableMutations::kv_decode_value(&v, ())?;
		// Entries with the same timestamp form one change set. A new timestamp closes
		// the previous set.
		match current_ts {
			Some(ref x) => {
				if key.ts != x.as_slice() {
					let db_mut = DatabaseMutation(buf);
					// Convert timestamp bytes to version number
					let version = ts_impl.decode(x)?.as_versionstamp();
					res.push(ChangeSet(version, db_mut));
					buf = Vec::new();
					current_ts = Some(key.ts.into_owned())
				}
			}
			None => {
				current_ts = Some(key.ts.into_owned());
			}
		}
		buf.push(v);
	}
	// Collect all mutations together
	if !buf.is_empty() {
		let db_mut = DatabaseMutation(buf);
		// Convert timestamp bytes to version number
		let ts_bytes = current_ts.expect("timestamp should be set when mutations exist");
		let version = ts_impl.decode(ts_bytes.as_slice())?.as_versionstamp();
		res.push(ChangeSet(version, db_mut));
	}
	// Return the results
	Ok(res)
}
