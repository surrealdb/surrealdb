use anyhow::Result;

use crate::ctx::Context;
use crate::dbs::{Options, Statement};
use crate::doc::Document;
use crate::err::Error;

impl Document {
	pub(super) async fn store_record_data(
		&mut self,
		ctx: &Context,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<()> {
		// Check if changed
		if !self.changed() {
			return Ok(());
		}
		// Check if the table is a view
		if self.tb(ctx, opt).await?.drop {
			return Ok(());
		}
		// Get the record id
		let rid = self.id()?;
		// Get NS & DB
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Store the record data
		let key = crate::key::thing::new(ns, db, &rid.table, &rid.key);
		// Remove the id field from the doc so that it's not duplicated,
		// because it's always present as a key in the underlying key-value
		// datastore. When the doc is read from the datastore, the key is set
		// as its id field.
		// The cloning of the doc is required because the resulting doc
		// must be returned to the caller with the id present.
		let mut doc_without_id = self.current.doc.clone();
		if let crate::val::Value::Object(obj) = doc_without_id.to_mut() {
			obj.0.remove("id");
		}
		// Match the statement type
		match stm {
			// This is a INSERT statement so try to insert the key.
			// For INSERT statements we don't first check for the
			// entry from the storage engine, so when we attempt
			// to store the record value, we presume that the key
			// does not exist. If the record value exists then we
			// attempt to run the ON DUPLICATE KEY UPDATE clause but
			// at this point the current document is not empty so we
			// set and update the key, without checking if the key
			// already exists in the storage engine.
			Statement::Insert(_) if self.is_iteration_initial() => {
				match ctx.tx().put(&key, &doc_without_id, opt.version).await {
					// The key already exists, so return an error
					Err(e) => {
						if matches!(e.downcast_ref(), Some(Error::TxKeyAlreadyExists)) {
							Err(anyhow::Error::new(Error::RecordExists {
								thing: rid.as_ref().to_owned(),
							}))
						} else {
							Err(e)
						}
					}
					// Return other values
					x => x,
				}
			}
			// This is a UPSERT statement so try to insert the key.
			// For UPSERT statements we don't first check for the
			// entry from the storage engine, so when we attempt
			// to store the record value, we must ensure that the
			// key does not exist.  If the record value exists then we
			// retry and attempt to update the record which exists.
			Statement::Upsert(_) if self.is_iteration_initial() => {
				match ctx.tx().put(&key, &doc_without_id, opt.version).await {
					// The key already exists, so return an error
					Err(e) => {
						if matches!(e.downcast_ref(), Some(Error::TxKeyAlreadyExists)) {
							Err(anyhow::Error::new(Error::RecordExists {
								thing: rid.as_ref().to_owned(),
							}))
						} else {
							Err(e)
						}
					}
					// Return other values
					x => x,
				}
			}
			// This is a CREATE statement so try to insert the key.
			// For CREATE statements we don't first check for the
			// entry from the storage engine, so when we attempt
			// to store the record value, we must ensure that the
			// key does not exist. If it already exists, then we
			// return an error, and the statement fails.
			Statement::Create(_) => {
				match ctx.tx().put(&key, &doc_without_id, opt.version).await {
					// The key already exists, so return an error
					Err(e) => {
						if matches!(e.downcast_ref(), Some(Error::TxKeyAlreadyExists)) {
							Err(anyhow::Error::new(Error::RecordExists {
								thing: rid.as_ref().to_owned(),
							}))
						} else {
							Err(e)
						}
					}
					x => x,
				}
			}
			// Let's update the stored value for the specified key
			_ => ctx.tx().set(&key, &doc_without_id, opt.version).await,
		}?;
		// Update the cache
		ctx.tx().set_record_cache(ns, db, &rid.table, &rid.key, doc_without_id.into_read_only())?;
		// Carry on
		Ok(())
	}
}
