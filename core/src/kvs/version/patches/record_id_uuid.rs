use super::VersionPatch;
use crate::{
	err::Error,
	kvs::{version::Version, Datastore, LockType, TransactionType},
};

pub struct PatchRecordIdUuid;

impl VersionPatch for PatchRecordIdUuid {
	async fn apply(ds: Datastore, version: Version) -> Result<(), Error> {
		match &version.0 {
			1 => {
				let tx = ds.transaction(TransactionType::Write, LockType::Pessimistic).await?;

				for ns in tx.all_ns().await?.iter() {
					let ns = ns.name.as_str();
					for db in tx.all_db(ns).await?.iter() {
						let db = db.name.as_str();
						for tb in tx.all_tb(ns, db).await?.iter() {
							let tb = tb.name.as_str();
							let mut beg = crate::key::thing::prefix(ns, db, tb);
							let end = crate::key::thing::suffix(ns, db, tb);

							'inner: loop {
								let ids = tx.keys(beg.clone()..end.clone(), 1000).await?;
								if ids.is_empty() {
									break 'inner;
								}

								beg = ids.last().unwrap().clone();
								for id in ids.iter() {
									id.
								}
							}
						}
					}
				}

				tx.commit().await?;
			}
			_ => {}
		}
		Ok(())
	}
}
