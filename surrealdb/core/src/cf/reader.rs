use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::cf::{ChangeSet, DatabaseMutation, TableMutations};
use crate::err::Error;
use crate::expr::statements::show::ShowSince;
use crate::key::database::all::DatabaseRoot;
use crate::key::{KVKeyDecode, KVRange, KVValue, change};
use crate::kvs::Transaction;
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
	let ts_impl = tx.timestamp_impl();

	// Calculate the start of the changefeed range
	let ts = match start {
		ShowSince::Versionstamp(x) => {
			ts_impl.create_from_versionstamp(x as u128).ok_or_else(|| Error::Query {
				message: format!(
					"Invalid versionstamp `{x}`, outside of range for kv-store timestamps"
				),
			})?
		}
		ShowSince::Timestamp(x) => {
			ts_impl.create_from_datetime(x.0).ok_or_else(|| Error::Query {
				message: format!(
					"Invalid versionstamp `{x}`, outside of range for kv-store timestamps"
				),
			})?
		}
	};

	let buf = &mut [0u8; _];
	let ts_bytes = ts.encode(buf);

	let beg = change::ChangeFeedTsPrefix {
		prefix: DatabaseRoot {
			ns,
			db,
		},
		ts: Cow::Borrowed(ts_bytes),
	}
	.encode_bound()?;

	// Calculate the end of the changefeed range
	let end = change::ChangeFeedPrefix {
		prefix: DatabaseRoot {
			ns,
			db,
		},
	}
	.encode_bound()?
	.next_neighbour_expect();

	let range = (beg..end).into();

	// Limit the changefeed results with a default
	let limit = limit.unwrap_or(100).min(1000);
	// Create an empty buffer for the timestamp
	let mut current_ts: Option<Vec<u8>> = None;
	// Create an empty buffer for the table mutations
	let mut buf: Vec<TableMutations> = Vec::new();
	// Create an empty buffer for the final changesets
	let mut res = Vec::<ChangeSet>::new();
	// Debug-only guard: the scan is ascending so versionstamps must be monotonic.
	#[cfg(debug_assertions)]
	let mut prev_ts: Option<Vec<u8>> = None;

	// iterate over _x and put decoded elements to r
	for (k, v) in tx.scan(range, limit, 0, None).await? {
		#[cfg(debug_assertions)]
		trace!("Reading change feed entry: {}", crate::key::Key::from(k.as_slice()));

		// Decode the changefeed entry key
		let key = crate::key::change::ChangeFeed::decode_key(&k)?;

		// Invariant: scan order is ascending, so each entry's versionstamp must be
		// >= the previous one. Process-local HLC stamping is monotonic within a node;
		// cross-node ordering is provided by the backend timestamp oracle.
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
		// Decode the byte array into a vector of operations
		let tb_muts = TableMutations::kv_decode_value(&v, ())?;
		// Get the timestamp of the changefeed entry
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
		buf.push(tb_muts);
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
