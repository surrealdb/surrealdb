use std::borrow::Cow;

use anyhow::Result;
use surrealdb_strand::TableName;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::cf::{ChangeSet, DatabaseMutation, TableMutations};
use crate::exec::Error as ExecError;
use crate::expr::statements::show::ShowSince;
use crate::key::schema::{ChangeFeedKey, ChangeFeedPrefix};
use crate::key::{KVKeyDecode, KVValue, Resumable};
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

/// The change sets `SHOW CHANGES` reports, each as the value it shows.
///
/// `SHOW CHANGES` returns changefeed entries as stored, without the per-field `PERMISSIONS FOR
/// select` pass that a `SELECT` applies, so it is an administrator-only read. The caller must both
/// hold an actor level that covers the database and be exempt from `View` permission checks (root,
/// or a namespace or database user holding the Viewer role or above); a permission-checked
/// principal is refused, so the feed never discloses a field value such a caller could not
/// `SELECT`. The selected namespace and database are resolved after the checks, raising where
/// either is missing, before the feed is read as [`read`] reads it.
pub(crate) async fn changes(
	ctx: &crate::ctx::FrozenContext,
	opt: &crate::dbs::Options,
	table: Option<&TableName>,
	since: ShowSince,
	limit: Option<u32>,
) -> Result<Vec<crate::val::Value>> {
	use crate::iam::{Action, PolicyError, ResourceKind};
	// Level gate: the caller's actor level must cover the database.
	ctx.is_allowed(opt, Action::View, ResourceKind::Table, crate::expr::Base::Db)?;
	// Administrator gate: a caller whose `View` is permission-checked is not an exempt
	// administrator, and the raw feed bypasses the field permissions such a caller is subject to,
	// so it is refused here rather than served a value it could not `SELECT`.
	if ctx.check_perms(opt, Action::View)? {
		let (ns, db) = opt.ns_db()?;
		return Err(crate::err::Error::IamError(PolicyError::NotAllowed {
			actor: opt.auth.actor().to_string(),
			action: Action::View.to_string(),
			resource: ResourceKind::Table.on_db(ns, db).to_string(),
		})
		.into());
	}
	let txn = ctx.tx();
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let changes = match ctx.changefeed_source() {
		Some(source) => source.show_changes(&txn, ns, db, table, since, limit).await?,
		None => read(&txn, ns, db, table, since, limit).await?,
	};
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
	let mut range = ChangeFeedPrefix {
		ns,
		db,
	}
	.range_where(Cow::Borrowed(ts_bytes)..)?;

	// The change-set limit defaults to 100 and is capped at 1000.
	let limit = limit.unwrap_or(100).min(1000);
	if limit == 0 {
		return Ok(Vec::new());
	}
	let batch_size = limit.clamp(64, 1024);
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
	// fail a read of the requested table. Scan in batches so `limit` bounds completed
	// change sets after filtering by `tb` and grouping multi-table commits at the same
	// timestamp.
	'scan: loop {
		let batch = tx.scan_raw(range.clone(), batch_size, 0, None).await?;
		let batch_len = batch.len();
		let Some((last_key, _)) = batch.last().map(|(k, v)| (k.clone(), v)) else {
			break;
		};
		for (k, v) in batch {
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
						let db_mut = DatabaseMutation(std::mem::take(&mut buf));
						// Convert timestamp bytes to version number
						let version = ts_impl.decode(x)?.as_versionstamp();
						res.push(ChangeSet(version, db_mut));
						if res.len() >= limit as usize {
							break 'scan;
						}
						current_ts = Some(key.ts.into_owned());
					}
				}
				None => {
					current_ts = Some(key.ts.into_owned());
				}
			}
			buf.push(v);
		}
		if (batch_len as u32) < batch_size {
			break;
		}
		range = range.resume_after(&last_key, surrealdb_kvs::Direction::Forward);
	}
	// Collect all mutations together
	if !buf.is_empty() && res.len() < limit as usize {
		let db_mut = DatabaseMutation(buf);
		// Convert timestamp bytes to version number
		let ts_bytes = current_ts.expect("timestamp should be set when mutations exist");
		let version = ts_impl.decode(ts_bytes.as_slice())?.as_versionstamp();
		res.push(ChangeSet(version, db_mut));
	}
	// Return the results
	Ok(res)
}

#[cfg(test)]
mod tests {
	use std::borrow::Cow;

	use surrealdb_strand::TableName;

	use super::read;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::cf::TableMutations;
	use crate::expr::statements::show::ShowSince;
	use crate::key::schema::ChangeFeedKey;
	use crate::kvs::Datastore;
	use crate::kvs::TransactionType::Write;

	#[tokio::test]
	async fn table_filter_and_multi_table_grouping_respect_changeset_limit() {
		let ds = Datastore::new("memory").await.unwrap();
		let ns = NamespaceId(1);
		let db = DatabaseId(1);
		let tb_a = TableName::from("tb_a");
		let tb_b = TableName::from("tb_b");

		let tx = ds.transaction(Write).await.unwrap();
		let ts_impl = tx.timestamp_impl();
		let mut buf = [0u8; _];

		// Write 80 changefeed entries for `tb_a` at versionstamps 1..=80 so a raw scan
		// capped at `limit = 1` or `batch_size = 64` would miss `tb_b` without batching.
		for vs in 1..=80u128 {
			let ts = ts_impl.create_from_versionstamp(vs).unwrap();
			let ts_bytes = ts.encode(&mut buf).to_vec();
			let k = ChangeFeedKey::new(ns, db, Cow::Borrowed(&ts_bytes), Cow::Borrowed(&tb_a));
			let v = TableMutations::new(tb_a.clone());
			tx.set_key(&k, &v).await.unwrap();
		}

		// At versionstamp 81, mutate both `tb_a` and `tb_b` in the same commit.
		let ts_81 = ts_impl.create_from_versionstamp(81).unwrap();
		let ts_81_bytes = ts_81.encode(&mut buf).to_vec();
		for tb in [&tb_a, &tb_b] {
			let k = ChangeFeedKey::new(ns, db, Cow::Borrowed(&ts_81_bytes), Cow::Borrowed(tb));
			let v = TableMutations::new(tb.clone());
			tx.set_key(&k, &v).await.unwrap();
		}

		// Table-filtered read for `tb_b` with limit = 1 must scan past all 80 `tb_a` entries.
		let b_changes =
			read(&tx, ns, db, Some(&tb_b), ShowSince::Versionstamp(1), Some(1)).await.unwrap();
		assert_eq!(b_changes.len(), 1);
		assert_eq!(b_changes[0].0, 81);
		assert_eq!(b_changes[0].1.0.len(), 1);
		assert_eq!(b_changes[0].1.0[0].0, tb_b);

		// Database-wide read starting at versionstamp 81 with limit = 1 must include both
		// tables from the commit at versionstamp 81 in a single ChangeSet.
		let db_changes =
			read(&tx, ns, db, None, ShowSince::Versionstamp(81), Some(1)).await.unwrap();
		assert_eq!(db_changes.len(), 1);
		assert_eq!(db_changes[0].0, 81);
		assert_eq!(db_changes[0].1.0.len(), 2);
		tx.cancel().await.unwrap();
	}
}
