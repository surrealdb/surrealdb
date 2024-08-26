use std::sync::Arc;

use crate::{err::Error, kvs::Transaction};

pub async fn v1_to_2_id_uuid(tx: Arc<Transaction>) -> Result<(), Error> {
	for ns in tx.all_ns().await?.iter() {
		let ns = ns.name.as_str();
		for db in tx.all_db(ns).await?.iter() {
			let db = db.name.as_str();
			for tb in tx.all_tb(ns, db).await?.iter() {
				let tb = tb.name.as_str();
				// mutable beg, as we update it each iteration to the last record id + a null byte
				let mut beg = crate::key::thing::prefix(ns, db, tb);
				let end = crate::key::thing::suffix(ns, db, tb);
				// queue of record ids to fix
				let mut queue: Vec<Vec<u8>> = Vec::new();

				// Explanation for these numbers:
				//
				// Before the Id enum: /*{NS}\0*{DB}\0*{TB}\0*
				// We are counting:    ^^    ^ ^    ^ ^    ^ ^
				//
				// Looking at the first four bytes for Id::Array (revision 1), we find: [0, 0, 0, 2]
				// First 3 bytes can be discarded, that 2 is the enum entry which we need to fix.
				// This totals to 11 bytes, plus the lengths of the bytes for namespace + database + tablename.
				//
				// For revision 2 of the Id enum, we added Uuid in index 2 (after number and string)
				// This means that any entry which was previously 2 or higher, now needs to be 3 or higher.
				// Resulting in a threshold of 2 (as a u8), used down below.
				//
				let pos = 11 + ns.as_bytes().len() + db.as_bytes().len() + tb.as_bytes().len();
				let threshold = 2_u8;

				'scan: loop {
					let keys = tx.keys(beg.clone()..end.clone(), 1000).await?;
					if keys.is_empty() {
						break 'scan;
					}

					// We suffix the last id with a null byte, to prevent scanning it twice (which would result in an infinite loop)
					beg = keys.last().unwrap().clone();
					beg.extend_from_slice(&[b'\0']);

					for key in keys.iter() {
						// Check if the id is affected
						if key.get(pos).is_some_and(|b| b >= &threshold) {
							// This ID needs fixing, add to queue
							queue.push(key.to_owned());
						}
					}
				}

				for key in queue.iter() {
					// Bump the enum entry by 1
					let mut fixed = key.clone();
					// This is safe, because we previously obtained the byte from the original id
					unsafe { *fixed.get_unchecked_mut(pos) += 1 };
					// Get the value for the old key. We can unwrap the option, as we know that the key exists in the KV store
					let val = tx.get(key.clone().to_owned(), None).await?.unwrap();
					// Delete the old key
					tx.del(key.to_owned()).await?;
					// Set the fixed key
					tx.set(fixed, val, None).await?;
				}
			}
		}
	}
	Ok(())
}
