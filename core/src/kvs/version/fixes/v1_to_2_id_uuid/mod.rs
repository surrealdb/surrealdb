mod deps;
use deps::*;
use std::sync::Arc;

use crate::{err::Error, kvs::Transaction};

pub async fn v1_to_2_id_uuid(tx: Arc<Transaction>) -> Result<(), Error> {
	for ns in tx.all_ns().await?.iter() {
		let ns = ns.name.as_str();
		for db in tx.all_db(ns).await?.iter() {
			let db = db.name.as_str();
			for tb in tx.all_tb(ns, db).await?.iter() {
				let tb = tb.name.as_str();
				migrate_tb_records(tx.clone(), ns, db, tb).await?;
				migrate_tb_edges(tx.clone(), ns, db, tb).await?;
			}
		}
	}
	Ok(())
}

async fn migrate_tb_records(
	tx: Arc<Transaction>,
	ns: &str,
	db: &str,
	tb: &str,
) -> Result<(), Error> {
	// mutable beg, as we update it each iteration to the last record id + a null byte
	let mut beg = crate::key::thing::prefix(ns, db, tb);
	let end = crate::key::thing::suffix(ns, db, tb);

	// We need to scan ALL keys and queue them first,
	// because if we fix them as we iterate, the pagination is off
	let mut queue: Vec<Vec<u8>> = Vec::new();

	'scan: loop {
		let keys = tx.keys(beg.clone()..end.clone(), 1000).await?;
		if keys.is_empty() {
			break 'scan;
		}

		// We suffix the last id with a null byte, to prevent scanning it twice (which would result in an infinite loop)
		beg.clone_from(keys.last().unwrap());
		beg.extend_from_slice(b"\0");

		for enc in keys.into_iter() {
			let dec = key::Thing::decode(&enc).unwrap();
			// Check if the id is affected
			if dec.id.is_affected() {
				// This ID needs fixing, add to queue
				queue.push(enc);
			}
		}
	}

	for enc in queue.iter() {
		let broken = key::Thing::decode(enc).unwrap();
		// Get a fixed id
		let fixed = broken.fix().unwrap();
		// Get the value for the old key. We can unwrap the option, as we know that the key exists in the KV store
		let val = tx.get(broken.clone().to_owned(), None).await?.unwrap();
		// Delete the old key
		tx.del(broken.to_owned()).await?;
		// Set the fixed key
		tx.set(fixed, val, None).await?;
	}

	Ok(())
}

async fn migrate_tb_edges(tx: Arc<Transaction>, ns: &str, db: &str, tb: &str) -> Result<(), Error> {
	// mutable beg, as we update it each iteration to the last record id + a null byte
	let mut beg = crate::key::table::all::new(ns, db, tb).encode()?;
	beg.extend_from_slice(&[b'~', 0x00]);
	let mut end = crate::key::table::all::new(ns, db, tb).encode()?;
	end.extend_from_slice(&[b'~', 0xff]);

	// We need to scan ALL keys and queue them first,
	// because if we fix them as we iterate, the pagination is off
	let mut queue: Vec<Vec<u8>> = Vec::new();

	'scan: loop {
		let keys = tx.keys(beg.clone()..end.clone(), 1000).await?;
		if keys.is_empty() {
			break 'scan;
		}

		// We suffix the last id with a null byte, to prevent scanning it twice (which would result in an infinite loop)
		beg.clone_from(keys.last().unwrap());
		beg.extend_from_slice(b"\0");

		for enc in keys.into_iter() {
			let dec = key::Graph::decode(&enc).unwrap();
			// Check if the id is affected
			if dec.id.is_affected() || dec.fk.is_affected() {
				// This ID needs fixing, add to queue
				queue.push(enc);
			}
		}
	}

	for enc in queue.iter() {
		let broken = key::Graph::decode(enc).unwrap();
		// Get a fixed id
		let fixed = broken.fix().unwrap();
		// Get the value for the old key. We can unwrap the option, as we know that the key exists in the KV store
		let val = tx.get(broken.clone().to_owned(), None).await?.unwrap();
		// Delete the old key
		tx.del(broken.to_owned()).await?;
		// Set the fixed key
		tx.set(fixed, val, None).await?;
	}

	Ok(())
}
