use crate::cf::{ChangeSet, DatabaseMutation, TableMutations};
use crate::err::Error;
use crate::key::change;
#[cfg(debug_assertions)]
use crate::key::debug::Sprintable;
use crate::kvs::Transaction;
use crate::sql::statements::show::ShowSince;
use crate::vs;

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
		ShowSince::Versionstamp(x) => change::prefix_ts(ns, db, vs::u64_to_versionstamp(x)),
		ShowSince::Timestamp(x) => {
			let ts = x.0.timestamp() as u64;
			let vs = tx.lock().await.get_versionstamp_from_timestamp(ts, ns, db).await?;
			match vs {
				Some(vs) => change::prefix_ts(ns, db, vs),
				None => {
					return Err(Error::Internal(
						"no versionstamp associated to this timestamp exists yet".to_string(),
					))
				}
			}
		}
	};
	// Calculate the end of the changefeed range
	let end = change::suffix(ns, db);
	// Limit the changefeed results with a default
	let limit = limit.unwrap_or(100).min(1000);
	// Create an empty buffer for the versionstamp
	let mut vs: Option<[u8; 10]> = None;
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
		let tb_muts: TableMutations = v.into();
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
