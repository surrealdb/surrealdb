use anyhow::Result;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::cf::{ChangeSet, DatabaseMutation, TableMutations};
use crate::expr::statements::show::ShowSince;
use crate::key::change;
#[cfg(debug_assertions)]
use crate::key::debug::Sprintable;
use crate::kvs::{KVKey, KVValue, Timestamp, Transaction};
use crate::val::TableName;

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
	// Calculate the start of the changefeed range
	let ts_bytes = match start {
		ShowSince::Versionstamp(x) => tx.timestamp_bytes_from_versionstamp(x as u128).await?,
		ShowSince::Timestamp(x) => tx.timestamp_bytes_from_datetime(x.0).await?,
	};
	let beg = change::prefix_ts(ns, db, &ts_bytes).encode_key()?;
	// Calculate the end of the changefeed range
	let end = change::suffix(ns, db).encode_key()?;
	// Limit the changefeed results with a default
	let limit = limit.unwrap_or(100).min(1000);
	// Create an empty buffer for the timestamp
	let mut current_ts: Option<Vec<u8>> = None;
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
		if tb.is_some_and(|tb| *tb != *dec.tb) {
			continue;
		}
		// Decode the byte array into a vector of operations
		let tb_muts = TableMutations::kv_decode_value(v)?;
		// Get the timestamp of the changefeed entry
		match current_ts {
			Some(ref x) => {
				let ts_bytes: &[u8] = dec.ts.as_ref();
				if ts_bytes != x.as_slice() {
					let db_mut = DatabaseMutation(buf);
					// Convert timestamp bytes to version number
					let version =
						<u64 as Timestamp>::from_ts_bytes(x.as_slice())?.to_versionstamp();
					res.push(ChangeSet(version, db_mut));
					buf = Vec::new();
					current_ts = Some(dec.ts.into_owned())
				}
			}
			None => {
				current_ts = Some(dec.ts.into_owned());
			}
		}
		buf.push(tb_muts);
	}
	// Collect all mutations together
	if !buf.is_empty() {
		let db_mut = DatabaseMutation(buf);
		// Convert timestamp bytes to version number
		let ts_bytes = current_ts.expect("timestamp should be set when mutations exist");
		let version = <u64 as Timestamp>::from_ts_bytes(ts_bytes.as_slice())?.to_versionstamp();
		res.push(ChangeSet(version, db_mut));
	}
	// Return the results
	Ok(res)
}
