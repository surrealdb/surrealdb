use crate::err::Error;
use crate::key::change;
use crate::kvs::Transaction;
use crate::vs;
use std::str;

// gc_all deletes all change feed entries that are older than the given watermark.
#[allow(unused)]
pub async fn gc_all(tx: &mut Transaction, watermark: u64, limit: Option<u32>) -> Result<(), Error> {
	let nses = tx.all_ns().await?;
	let nses = nses.as_ref();
	for ns in nses {
		gc_ns(tx, ns.name.as_str(), watermark, limit).await?;
	}
	Ok(())
}

// gc_ns deletes all change feed entries in the given namespace that are older than the given watermark.
#[allow(unused)]
pub async fn gc_ns(
	tx: &mut Transaction,
	ns: &str,
	watermark: u64,
	limit: Option<u32>,
) -> Result<(), Error> {
	let dbs = tx.all_db(ns).await?;
	let dbs = dbs.as_ref();
	for db in dbs {
		gc_db(tx, ns, db.name.as_str(), watermark, limit).await?;
	}
	Ok(())
}

// gc_db deletes all change feed entries in the given database that are older than the given watermark.
pub async fn gc_db(
	tx: &mut Transaction,
	ns: &str,
	db: &str,
	watermark: u64,
	limit: Option<u32>,
) -> Result<(), Error> {
	let beg: Vec<u8> = change::prefix_ts(ns, db, vs::u64_to_versionstamp(0));
	let end = change::prefix_ts(ns, db, vs::u64_to_versionstamp(watermark));

	let limit = limit.unwrap_or(100);

	tx.delr(beg..end, limit).await?;

	Ok(())
}
