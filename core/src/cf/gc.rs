use crate::err::Error;
use crate::key::change;
use crate::kvs::Transaction;
use crate::vs;
use crate::vs::Versionstamp;
use std::str;

// gc_all_at deletes all change feed entries that become stale at the given timestamp.
#[allow(unused)]
pub async fn gc_all_at(tx: &mut Transaction, ts: u64, limit: Option<u32>) -> Result<(), Error> {
	let nses = tx.all_ns().await?;
	let nses = nses.as_ref();
	for ns in nses {
		gc_ns(tx, ns.name.as_str(), limit, ts).await?;
	}
	Ok(())
}

// gc_ns deletes all change feed entries in the given namespace that are older than the given watermark.
#[allow(unused)]
pub async fn gc_ns(
	tx: &mut Transaction,
	ns: &str,
	limit: Option<u32>,
	ts: u64,
) -> Result<(), Error> {
	let dbs = tx.all_db(ns).await?;
	let dbs = dbs.as_ref();
	for db in dbs {
		let db_cf_expiry = match &db.changefeed {
			None => 0,
			Some(cf) => cf.expiry.as_secs(),
		};
		let tbs = tx.all_tb(ns, db.name.as_str()).await?;
		let tbs = tbs.as_ref();
		let max_tb_cf_expiry = tbs.iter().fold(0, |acc, tb| match &tb.changefeed {
			None => acc,
			Some(cf) => {
				if cf.expiry.is_zero() {
					acc
				} else {
					acc.max(cf.expiry.as_secs())
				}
			}
		});
		let cf_expiry = db_cf_expiry.max(max_tb_cf_expiry);
		if ts < cf_expiry {
			continue;
		}
		let watermark_ts = ts - cf_expiry;
		let watermark_vs =
			tx.get_versionstamp_from_timestamp(watermark_ts, ns, db.name.as_str(), true).await?;
		if let Some(watermark_vs) = watermark_vs {
			gc_db(tx, ns, db.name.as_str(), watermark_vs, limit).await?;
		}
	}
	Ok(())
}

// gc_db deletes all change feed entries in the given database that are older than the given watermark.
pub async fn gc_db(
	tx: &mut Transaction,
	ns: &str,
	db: &str,
	watermark: Versionstamp,
	limit: Option<u32>,
) -> Result<(), Error> {
	let beg: Vec<u8> = change::prefix_ts(ns, db, vs::u64_to_versionstamp(0));
	let end = change::prefix_ts(ns, db, watermark);

	let limit = limit.unwrap_or(100);

	tx.delr(beg..end, limit).await?;

	Ok(())
}
