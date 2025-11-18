use anyhow::Result;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::cf::{ChangeSet, DatabaseMutation, TableMutations};
use crate::expr::statements::show::ShowSince;
use crate::key::change;
#[cfg(debug_assertions)]
use crate::key::debug::Sprintable;
use crate::kvs::{KVKey, KVValue, Transaction};
use crate::vs::VersionStamp;

// Reads the change feed for a specific database or a table,
// starting from a specific timestamp.
//
// The limit parameter is the maximum number of change sets to return.
// If the limit is not specified, the default is 100.
//
// You can use this to read the change feed in chunks.
// The second call would start from the last timestamp + 1 of the first call.
pub async fn read(
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	tb: Option<&str>,
	start: ShowSince,
	limit: Option<u32>,
) -> Result<Vec<ChangeSet>> {
	// Calculate the start of the changefeed range
	let beg = match start {
		ShowSince::Versionstamp(x) => change::prefix_ts(ns, db, x),
		ShowSince::Timestamp(x) => {
			let ts = x.timestamp() as u64;
			change::prefix_ts(ns, db, ts)
		}
	}
	.encode_key()?;
	// Calculate the end of the changefeed range
	let end = change::suffix(ns, db).encode_key()?;
	// Limit the changefeed results with a default
	let limit = limit.unwrap_or(100).min(1000);
	// Create an empty buffer for the timestamp
	let mut current_ts: Option<u64> = None;
	// Create an empty buffer for the table mutations
	let mut buf: Vec<TableMutations> = Vec::new();
	// Create an empty buffer for the final changesets
	let mut res = Vec::<ChangeSet>::new();

	// iterate over _x and put decoded elements to r
	for (k, v) in tx.scan(beg..end, limit, None).await? {
		#[cfg(debug_assertions)]
		trace!("Reading change feed entry: {}", k.sprint());

		// Decode the changefeed entry key
		let dec = crate::key::change::Cf::decode_key(&k)?;

		// Check the change is for the desired table
		if tb.is_some_and(|tb| tb != dec.tb) {
			continue;
		}
		// Decode the byte array into a vector of operations
		let tb_muts = TableMutations::kv_decode_value(v)?;
		// Get the timestamp of the changefeed entry
		match current_ts {
			Some(x) => {
				if dec.ts != x {
					let db_mut = DatabaseMutation(buf);
					// Convert timestamp to VersionStamp for compatibility with existing ChangeSet structure
					res.push(ChangeSet(VersionStamp::from_u64(x), db_mut));
					buf = Vec::new();
					current_ts = Some(dec.ts)
				}
			}
			None => {
				current_ts = Some(dec.ts);
			}
		}
		buf.push(tb_muts);
	}
	// Collect all mutations together
	if !buf.is_empty() {
		let db_mut = DatabaseMutation(buf);
		// Convert timestamp to VersionStamp for compatibility with existing ChangeSet structure
		res.push(ChangeSet(
			VersionStamp::from_u64(
				current_ts.expect("timestamp should be set when mutations exist"),
			),
			db_mut,
		));
	}
	// Return the results
	Ok(res)
}
