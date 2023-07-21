use crate::cf::{ChangeSet, DatabaseMutation, TableMutations};
use crate::err::Error;
use crate::key::change;
use crate::key::database;
use crate::kvs::Transaction;
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
	tx: &mut Transaction,
	ns: &str,
	db: &str,
	tb: Option<&str>,
	start: Option<u64>,
	limit: Option<u32>,
) -> Result<Vec<ChangeSet>, Error> {
	// Get the current timestamp
	let seq = database::vs::new(ns, db);

	let beg = match start {
		Some(x) => change::prefix_ts(ns, db, vs::u64_to_versionstamp(x)),
		None => {
			let ts = tx.get_timestamp(seq, false).await?;
			change::prefix_ts(ns, db, ts)
		} // None => dc::prefix(ns, db),
	};
	let end = change::suffix(ns, db);

	let limit = limit.unwrap_or(100);

	let _x = tx.scan(beg..end, limit).await?;

	let mut vs: Option<[u8; 10]> = None;
	let mut buf: Vec<TableMutations> = Vec::new();

	let mut r = Vec::<ChangeSet>::new();
	// iterate over _x and put decoded elements to r
	for (k, v) in _x {
		trace!("read change feed; {k:?}");

		let dec = crate::key::change::Cf::decode(&k).unwrap();

		if let Some(tb) = tb {
			if dec.tb != tb {
				continue;
			}
		}

		let _tb = dec.tb;
		let ts = dec.vs;

		// Decode the byte array into a vector of operations
		let tb_muts: TableMutations = v.into();

		match vs {
			Some(x) => {
				if ts != x {
					let db_mut = DatabaseMutation(buf);
					r.push(ChangeSet(x, db_mut));
					buf = Vec::new();
					vs = Some(ts)
				}
			}
			None => {
				vs = Some(ts);
			}
		}
		buf.push(tb_muts);
	}

	if !buf.is_empty() {
		let db_mut = DatabaseMutation(buf);
		r.push(ChangeSet(vs.unwrap(), db_mut));
	}

	Ok(r)
}
