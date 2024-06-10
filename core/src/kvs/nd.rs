use crate::dbs::node::Node;
use crate::err::Error;
use crate::kvs::Datastore;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;

impl Datastore {
	//
	pub async fn register_node(&self, id: uuid::Uuid) -> Result<(), Error> {
		let txn = self.transaction(Write, Optimistic).await?;
		let key = crate::key::root::nd::Nd::new(id);
		let val = Node {
			id,
			heartbeat: self.clock.now().await,
		};
		match txn.put(key, val).await {
			// There was an error with the request
			Err(err) => {
				// Ensure the transaction is cancelled
				let _ = txn.cancel().await;
				// Check what the error was with inserting
				match err {
					// This node registration already exists
					Error::TxKeyAlreadyExists => Err(Error::ClAlreadyExists {
						value: id.to_string(),
					}),
					// There was a different error
					err => Err(err),
				}
			}
			// Everything is ok
			Ok(_) => {
				// Commit the transaction
				txn.commit().await
			}
		}
	}
	//
	// pub async fn cleanup_nodes
}
