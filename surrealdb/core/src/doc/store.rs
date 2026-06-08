use anyhow::Result;

use crate::catalog::providers::TableProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;

impl Document {
	pub(super) async fn store_record_data(
		&mut self,
		ctx: &FrozenContext,
		stm: &Statement<'_>,
	) -> Result<()> {
		// Check if changed
		if !self.is_modified() {
			return Ok(());
		}
		// Get the document table
		let tb = self.doc_ctx.tb()?;
		// Check if the table is DROP
		if tb.drop {
			return Ok(());
		}
		// Get the record id
		let rid = self.id()?;
		// Get the namespace id
		let ns = self.doc_ctx.ns().namespace_id;
		// Get the database id
		let db = self.doc_ctx.db().database_id;
		// Prep the doc
		let doc = self.current.doc.clone().into_read_only();
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
				match ctx.tx().put_record(ns, db, &rid.table, &rid.key, doc).await {
					// The key already exists, so return an error
					Err(e) => {
						if matches!(
							e.downcast_ref(),
							Some(Error::Kvs(crate::kvs::Error::TransactionKeyAlreadyExists))
						) {
							Err(anyhow::Error::new(Error::RecordExists {
								record: rid.as_ref().to_owned(),
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
				match ctx.tx().put_record(ns, db, &rid.table, &rid.key, doc).await {
					// The key already exists, so return an error
					Err(e) => {
						if matches!(
							e.downcast_ref(),
							Some(Error::Kvs(crate::kvs::Error::TransactionKeyAlreadyExists))
						) {
							Err(anyhow::Error::new(Error::RecordExists {
								record: rid.as_ref().to_owned(),
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
				match ctx.tx().put_record(ns, db, &rid.table, &rid.key, doc).await {
					// The key already exists, so return an error
					Err(e) => {
						if matches!(
							e.downcast_ref(),
							Some(Error::Kvs(crate::kvs::Error::TransactionKeyAlreadyExists))
						) {
							Err(anyhow::Error::new(Error::RecordExists {
								record: rid.as_ref().to_owned(),
							}))
						} else {
							Err(e)
						}
					}
					x => x,
				}
			}
			// SECURITY: a RELATE that resolved to a new edge (no existing
			// record loaded into `self.initial`) must NOT overwrite a
			// pre-existing record at the same id. `Document::relate`
			// chooses the create path based on `self.current.doc.is_nullish()`
			// *before* the SET / CONTENT / MERGE clause is applied, so an
			// attacker-controlled `id = edge:existing` would otherwise reach
			// `set_record` and silently overwrite the existing edge under
			// create permissions. Use the create-only `put_record` here too.
			Statement::Relate(_) if self.initial.doc.as_ref().is_nullish() => {
				match ctx.tx().put_record(ns, db, &rid.table, &rid.key, doc).await {
					Err(e) => {
						if matches!(
							e.downcast_ref(),
							Some(Error::Kvs(crate::kvs::Error::TransactionKeyAlreadyExists))
						) {
							Err(anyhow::Error::new(Error::RecordExists {
								record: rid.as_ref().to_owned(),
							}))
						} else {
							Err(e)
						}
					}
					x => x,
				}
			}
			// Let's update the stored value for the specified key
			_ => ctx.tx().set_record(ns, db, &rid.table, &rid.key, doc).await,
		}?;
		// KV write succeeded; mark the document as mutated so the
		// per-statement affected-row counter (bumped from
		// `Document::process`) reflects this row.
		self.mutated = true;
		Ok(())
	}
}
