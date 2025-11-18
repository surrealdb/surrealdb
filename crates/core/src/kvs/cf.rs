use anyhow::Result;

use super::tasklease::LeaseHandler;
use crate::kvs::Datastore;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;

impl Datastore {
	/// Deletes all change feed entries that are older than the timestamp.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self, lh))]
	pub(crate) async fn changefeed_cleanup(
		&self,
		lh: Option<&LeaseHandler>,
		ts: u64,
	) -> Result<()> {
		// Create a new transaction
		let txn = self.transaction(Write, Optimistic).await?;
		// Perform the garbage collection
		catch!(txn, crate::cf::gc_all_at(lh, &txn, ts).await);
		// Commit the changes
		catch!(txn, txn.commit().await);
		// Everything ok
		Ok(())
	}
}
