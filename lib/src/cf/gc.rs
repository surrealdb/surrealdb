use crate::err::Error;
use crate::key::change;
use crate::kvs::Transaction;
use crate::vs;
use crate::vs::Versionstamp;
use std::str;

// gc_all deletes all change feed entries that are older than the given watermark.
#[allow(unused)]
pub async fn gc_all(tx: &mut Transaction, limit: Option<u32>) -> Result<(), Error> {
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map_err(|e| Error::Internal(e.to_string()))?;
	gc_all_at(tx, now.as_secs(), limit).await?;
	Ok(())
}

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
		match &db.changefeed {
			None => continue,
			Some(cf) => {
				if cf.expiry.is_zero() {
					continue;
				}
				let c = ts - cf.expiry.as_secs();
				let watermark_vs =
					tx.get_versionstamp_from_timestamp(ts, ns, db.name.as_str(), true).await?;
				if let Some(watermark_vs) = watermark_vs {
					gc_db(tx, ns, db.name.as_str(), watermark_vs, limit).await?;
				}
			}
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
