use std::sync::Arc;

use crate::{err::Error, kvs::Transaction};

pub async fn v1_to_2_id_uuid(tx: Arc<Transaction>) -> Result<(), Error> {
	for ns in tx.all_ns().await?.iter() {
		let ns = ns.name.as_str();
		for db in tx.all_db(ns).await?.iter() {
			let db = db.name.as_str();
			for tb in tx.all_tb(ns, db).await?.iter() {
				let tb = tb.name.as_str();
				let mut beg = crate::key::thing::prefix(ns, db, tb);
				let end = crate::key::thing::suffix(ns, db, tb);
				let pos = 15 + ns.len() + db.len() + tb.len();
				let threshold = 2 as u8;

				'inner: loop {
					let ids = tx.keys(beg.clone()..end.clone(), 1000).await?;
					if ids.is_empty() {
						break 'inner;
					}

					for id in ids.iter() {
						match id.get(pos) {
							// Check to see if the id in the key is affected
							Some(entry) if entry.to_owned() > threshold => {
								// Bump the enum entry by 1
								let mut fixed = id.clone();
								fixed.insert(pos, entry + 1);
								// Get the value, delete the old key, and set the new key
								let val = tx.get(id.clone().to_owned(), None).await?.unwrap();
								tx.del(id.to_owned()).await?;
								tx.set(fixed.clone(), val, None).await?;
								// Update which key to scan from for the next iteration
								// This is important, because the original key was changed,
								// we will otherwise scan the new key we just stored in the database.
								beg = fixed;
								beg.extend_from_slice(&[b'\0']);
							}
							_ => {
								beg = id.clone();
								beg.extend_from_slice(&[b'\0']);
							}
						}
					}
				}
			}
		}
	}
	Ok(())
}
