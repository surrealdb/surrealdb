use crate::cnf::NORMAL_FETCH_SIZE;
use crate::dbs::node::Node;
use crate::err::Error;
use crate::kvs::Datastore;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;
use crate::sql::statements::LiveStatement;
use std::time::Duration;

impl Datastore {
	/// Inserts a node for the first time into the cluster.
	///
	/// This function should be run at server or database startup.
	///
	/// This function ensures that this node is entered into the clister
	/// membership entries. This function must be run at server or database
	/// startup, in order to write the initial entry and timestamp to storage.
	pub async fn insert_node(&self, id: uuid::Uuid) -> Result<(), Error> {
		let txn = self.transaction(Write, Optimistic).await?;
		let key = crate::key::root::nd::Nd::new(id);
		let now = self.clock.now().await;
		let val = Node::new(id, now, false);
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
			Ok(_) => txn.commit().await,
		}
	}

	/// Updates an already existing node in the cluster.
	///
	/// This function should be run periodically at a regular interval.
	///
	/// This function updates the entry for this node with an up-to-date
	/// timestamp. This ensures that the node is not marked as expired by any
	/// garbage collection tasks, preventing any data cleanup for this node.
	pub async fn update_node(&self, id: uuid::Uuid) -> Result<(), Error> {
		let txn = self.transaction(Write, Optimistic).await?;
		let key = crate::key::root::nd::new(id);
		let now = self.clock.now().await;
		let val = Node::new(id, now, false);
		match txn.set(key, val).await {
			// There was an error with the request
			Err(err) => {
				let _ = txn.cancel().await;
				Err(err)
			}
			// Everything is ok
			Ok(_) => txn.commit().await,
		}
	}

	/// Deletes a node from the cluster.
	///
	/// This function should be run when a node is shutting down.
	///
	/// This function marks the node as archived, ready for garbage collection.
	/// Later on when garbage collection is running the live queries assigned
	/// to this node will be removed, along with the node itself.
	pub async fn delete_node(&self, id: uuid::Uuid) -> Result<(), Error> {
		let txn = self.transaction(Write, Optimistic).await?;
		let key = crate::key::root::nd::new(id);
		let val = txn.get_node(id).await?;
		let val = val.as_ref().archive();
		match txn.set(key, val).await {
			// There was an error with the request
			Err(err) => {
				let _ = txn.cancel().await;
				Err(err)
			}
			// Everything is ok
			Ok(_) => txn.commit().await,
		}
	}

	/// Expires nodes which have timedout from the cluster.
	///
	/// This function should be run periodically at an interval.
	///
	/// This function marks the node as archived, ready for garbage collection.
	/// Later on when garbage collection is running the live queries assigned
	/// to this node will be removed, along with the node itself.
	pub async fn expire_nodes(&self) -> Result<(), Error> {
		let txn = self.transaction(Write, Optimistic).await?;
		let now = self.clock.now().await;
		let nds = txn.all_nodes().await?;
		for nd in nds.iter() {
			// Check that the node is active
			if nd.is_active() {
				// Check if the node has expired
				if nd.hb < now - Duration::from_secs(30) {
					// Mark the node as archived
					let val = nd.archive();
					// Get the key for the node entry
					let key = crate::key::root::nd::new(nd.id);
					// Update the node entry
					if let Err(err) = txn.set(key, val).await {
						// There was an error with the request
						let _ = txn.cancel().await;
						return Err(err);
					}
				}
			}
		}
		// Commit the changes
		txn.commit().await
	}

	/// Cleans up nodes which are no longer in this cluster.
	///
	/// This function should be run periodically at an interval.
	///
	/// This function clears up all nodes which have been marked as archived.
	/// When a matching node is found, all node queries, and table queries are
	/// garbage collected, before the node itself is completely deleted.
	pub async fn cleanup_nodes(&self) -> Result<(), Error> {
		// Fetch all of the expired nodes
		let expired = {
			let txn = self.transaction(Read, Optimistic).await?;
			let nds = txn.all_nodes().await?;
			// Filter the archived nodes
			nds.iter().filter_map(Node::archived).collect::<Vec<_>>()
		};
		// Delete the live queries
		{
			for id in expired.iter() {
				let txn = self.transaction(Write, Optimistic).await?;
				let beg = crate::key::node::lq::prefix(*id);
				let end = crate::key::node::lq::suffix(*id);
				let mut next = Some(beg..end);
				while let Some(rng) = next {
					let res = txn.batch(rng, *NORMAL_FETCH_SIZE, true).await?;
					next = res.next;
					for (k, v) in res.values.iter() {
						// Decode the table for this live query
						if let Ok(tb) = std::str::from_utf8(v) {
							// Get the key for this node live query
							let nlq = crate::key::node::lq::Lq::decode(k)?;
							// Check that the node for this query is archived
							if expired.contains(&nlq.nd) {
								// Get the key for this table live query
								let tlq = crate::key::table::lq::new(nlq.ns, nlq.db, tb, nlq.lq);
								// Delete the table live query
								if let Err(e) = txn.del(tlq).await {
									let _ = txn.cancel().await;
									return Err(e);
								}
								// Delete the node live query
								if let Err(e) = txn.del(nlq).await {
									let _ = txn.cancel().await;
									return Err(e);
								}
							}
						}
					}
				}
				// Commit the changes
				txn.commit().await?;
			}
		}
		// Delete the expired nodes
		{
			let txn = self.transaction(Write, Optimistic).await?;
			// Loop over the nodes and delete
			for id in expired.iter() {
				// Get the key for the node entry
				let key = crate::key::root::nd::new(*id);
				// Delete the cluster node entry
				if let Err(e) = txn.del(key).await {
					let _ = txn.cancel().await;
					return Err(e);
				}
			}
			// Commit the changes
			txn.commit().await?;
		}
		// Everything was successful
		Ok(())
	}

	/// Clean up all other miscellaneous data
	pub async fn garbage_collect(&self) -> Result<(), Error> {
		// Fetch expired nodes
		let expired = {
			let txn = self.transaction(Read, Optimistic).await?;
			let nds = txn.all_nodes().await?;
			// Filter the archived nodes
			nds.iter().filter_map(Node::archived).collect::<Vec<_>>()
		};
		// Fetch all namespaces
		let nss = {
			let txn = self.transaction(Read, Optimistic).await?;
			txn.all_ns().await?
		};
		// Loop over all namespaces
		for ns in nss.iter() {
			// Fetch all databases
			let dbs = {
				let txn = self.transaction(Read, Optimistic).await?;
				txn.all_db(&ns.name).await?
			};
			// Loop over all databases
			for db in dbs.iter() {
				// Fetch all tables
				let tbs = {
					let txn = self.transaction(Read, Optimistic).await?;
					txn.all_tb(&ns.name, &db.name).await?
				};
				// Loop over all tables
				for tb in tbs.iter() {
					let txn = self.transaction(Write, Optimistic).await?;
					let beg = crate::key::table::lq::prefix(&ns.name, &db.name, &tb.name);
					let end = crate::key::table::lq::suffix(&ns.name, &db.name, &tb.name);
					let mut next = Some(beg..end);
					while let Some(rng) = next {
						let res = txn.batch(rng, *NORMAL_FETCH_SIZE, false).await?;
						next = res.next;
						for (k, v) in res.values.iter() {
							//
							let stm: LiveStatement = v.into();
							// Get the key for this node live query
							let tlq = crate::key::table::lq::Lq::decode(k)?;
							// Get the node id and the live query id
							let (nid, lid) = (stm.node.0, stm.id.0);
							// Check that the node for this query is archived
							if expired.contains(&stm.node) {
								// Get the key for this table live query
								let nlq = crate::key::node::lq::new(nid, lid, &ns.name, &db.name);
								// Delete the node live query
								if let Err(e) = txn.del(nlq).await {
									let _ = txn.cancel().await;
									return Err(e);
								}
								// Delete the table live query
								if let Err(e) = txn.del(tlq).await {
									let _ = txn.cancel().await;
									return Err(e);
								}
							}
						}
					}
					// Commit the changes
					txn.commit().await?;
				}
			}
		}
		Ok(())
	}
}
