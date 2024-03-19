use crate::cf::{ChangeSet, DatabaseMutation, TableMutations};
use crate::err::Error;
use crate::key::change;
#[cfg(debug_assertions)]
use crate::key::debug::sprint_key;
use crate::kvs::{Limit, ScanPage, Transaction};
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
	tx: &mut Transaction,
	ns: &str,
	db: &str,
	tb: Option<&str>,
	start: ShowSince,
	limit: Option<u32>,
) -> Result<Vec<ChangeSet>, Error> {
	let beg = match start {
		ShowSince::Versionstamp(x) => change::prefix_ts(ns, db, vs::u64_to_versionstamp(x)),
		ShowSince::Timestamp(x) => {
			let ts = x.0.timestamp() as u64;
			let vs = tx.get_versionstamp_from_timestamp(ts, ns, db, true).await?;
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
	let end = change::suffix(ns, db);

	let limit = limit.unwrap_or(100);

	let scan = tx
		.scan_paged(
			ScanPage {
				range: beg..end,
				limit: Limit::Limited(limit),
			},
			limit,
		)
		.await?;

	let mut vs: Option<[u8; 10]> = None;
	let mut buf: Vec<TableMutations> = Vec::new();

	let mut r = Vec::<ChangeSet>::new();
	// iterate over _x and put decoded elements to r
	for (k, v) in scan.values {
		#[cfg(debug_assertions)]
		trace!("read change feed; {}", sprint_key(&k));

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
