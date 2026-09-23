use crate::cf::{ChangeSet, DatabaseMutation, TableMutations};
use crate::err::Error;
use crate::key::change;
#[cfg(debug_assertions)]
use crate::key::debug::Sprintable;
use crate::kvs::{KeyDecode, Transaction};
use crate::sql::statements::show::ShowSince;
use crate::vs::VersionStamp;
#[cfg(not(target_family = "wasm"))]
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(target_family = "wasm")]
use wasmtimer::std::{SystemTime, UNIX_EPOCH};

/// Seconds since the Unix epoch, on the clock a database's changefeed
/// timestamps are recorded against.
fn now() -> Result<u64, Error> {
	Ok(SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_err(|e| Error::Internal(format!("Clock may have gone backwards: {:?}", e.duration())))?
		.as_secs())
}

// Reads the change feed for a specific database or a table,
// starting from a specific versionstamp.
//
// The limit parameter is the maximum number of change sets to return.
// If the limit is not specified, the default is 100.
//
// You can use this to read the change feed in chunks.
// The second call would start from the last versionstamp + 1 of the first call.
pub async fn read(
	tx: &Transaction,
	ns: &str,
	db: &str,
	tb: Option<&str>,
	start: ShowSince,
	limit: Option<u32>,
) -> Result<Vec<ChangeSet>, Error> {
	// Calculate the start of the changefeed range
	let beg = match start {
		ShowSince::Versionstamp(x) => change::prefix_ts(ns, db, VersionStamp::from_u64(x))?,
		ShowSince::Timestamp(x) => {
			// Timestamps are recorded as seconds since the epoch, and a request
			// from before it is a request from before anything recorded
			let ts = x.0.timestamp().max(0) as u64;
			let vs = tx.lock().await.get_versionstamp_from_timestamp(ts, ns, db).await?;
			// The lookup finds the greatest recorded timestamp at or before the
			// one asked for, so nothing is found when the request predates every
			// timestamp this database has recorded, and equally when it has
			// recorded none — which is what a changefeed defined less than one
			// collection tick ago looks like, and what one on a database with no
			// retention looks like for as long as it keeps none.
			//
			// A request the timeline cannot place is answered against the clock
			// the timestamps are recorded on. Everything retained is at or after
			// a request from the past, so the scan starts at the beginning of the
			// range rather than failing; nothing can be at or after a request
			// from the future, so there is nothing to scan for.
			match vs {
				Some(vs) => change::prefix_ts(ns, db, vs)?,
				None if ts > now()? => return Ok(Vec::new()),
				None => change::prefix_ts(ns, db, VersionStamp::ZERO)?,
			}
		}
	};
	// Calculate the end of the changefeed range
	let end = change::suffix(ns, db)?;
	// Limit the changefeed results with a default
	let limit = limit.unwrap_or(100).min(1000);
	// Create an empty buffer for the versionstamp
	let mut vs: Option<VersionStamp> = None;
	// Create an empty buffer for the table mutations
	let mut buf: Vec<TableMutations> = Vec::new();
	// Create an empty buffer for the final changesets
	let mut res = Vec::<ChangeSet>::new();
	// iterate over _x and put decoded elements to r
	for (k, v) in tx.scan(beg..end, limit, None).await? {
		#[cfg(debug_assertions)]
		trace!("Reading change feed entry: {}", k.sprint());
		// Decode the changefeed entry key
		let dec = crate::key::change::Cf::decode(&k).unwrap();
		// Check the change is for the desired table
		if tb.is_some_and(|tb| tb != dec.tb) {
			continue;
		}
		// Decode the byte array into a vector of operations
		let tb_muts: TableMutations = revision::from_slice(&v)?;
		// Get the timestamp of the changefeed entry
		match vs {
			Some(x) => {
				if dec.vs != x {
					let db_mut = DatabaseMutation(buf);
					res.push(ChangeSet(x, db_mut));
					buf = Vec::new();
					vs = Some(dec.vs)
				}
			}
			None => {
				vs = Some(dec.vs);
			}
		}
		buf.push(tb_muts);
	}
	// Collect all mutations together
	if !buf.is_empty() {
		let db_mut = DatabaseMutation(buf);
		res.push(ChangeSet(vs.unwrap(), db_mut));
	}
	// Return the results
	Ok(res)
}
