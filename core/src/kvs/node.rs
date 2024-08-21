use crate::cnf::NORMAL_FETCH_SIZE;
use crate::dbs::node::Node;
use crate::err::Error;
use crate::kvs::Datastore;
use crate::kvs::Live;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;
use crate::sql::statements::LiveStatement;
use std::time::Duration;

const TARGET: &str = "surrealdb::core::kvs::node";

impl Datastore {
	/// Inserts a node for the first time into the cluster.
	///
	/// This function should be run at server or database startup.
	///
	/// This function ensures that this node is entered into the clister
	/// membership entries. This function must be run at server or database
	/// startup, in order to write the initial entry and timestamp to storage.
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::node", skip(self))]
	pub async fn insert_node(&self, id: uuid::Uuid) -> Result<(), Error> {
		// Log when this method is run
		trace!(target: TARGET, "Inserting node in the cluster");
		// Open transaction and set node data
		let txn = self.transaction(Write, Optimistic).await?;
		let key = crate::key::root::nd::Nd::new(id);
		let now = self.clock_now().await;
		let val = Node::new(id, now, false);
		match run!(txn, txn.put(key, val, None)) {
			Err(Error::TxKeyAlreadyExists) => Err(Error::ClAlreadyExists {
				value: id.to_string(),
			}),
			other => other,
		}
	}

	/// Updates an already existing node in the cluster.
	///
	/// This function should be run periodically at a regular interval.
	///
	/// This function updates the entry for this node with an up-to-date
	/// timestamp. This ensures that the node is not marked as expired by any
	/// garbage collection tasks, preventing any data cleanup for this node.
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::node", skip(self))]
	pub async fn update_node(&self, id: uuid::Uuid) -> Result<(), Error> {
		// Log when this method is run
		trace!(target: TARGET, "Updating node in the cluster");
		// Open transaction and set node data
		let txn = self.transaction(Write, Optimistic).await?;
		let key = crate::key::root::nd::new(id);
		let now = self.clock_now().await;
		let val = Node::new(id, now, false);
		run!(txn, txn.set(key, val))
	}

	/// Deletes a node from the cluster.
	///
	/// This function should be run when a node is shutting down.
	///
	/// This function marks the node as archived, ready for garbage collection.
	/// Later on when garbage collection is running the live queries assigned
	/// to this node will be removed, along with the node itself.
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::node", skip(self))]
	pub async fn delete_node(&self, id: uuid::Uuid) -> Result<(), Error> {
		// Log when this method is run
		trace!(target: TARGET, "Archiving node in the cluster");
		// Open transaction and set node data
		let txn = self.transaction(Write, Optimistic).await?;
		let key = crate::key::root::nd::new(id);
		let val = txn.get_node(id).await?;
		let val = val.as_ref().archive();
		run!(txn, txn.set(key, val))
	}

	/// Expires nodes which have timedout from the cluster.
	///
	/// This function should be run periodically at an interval.
	///
	/// This function marks the node as archived, ready for garbage collection.
	/// Later on when garbage collection is running the live queries assigned
	/// to this node will be removed, along with the node itself.
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::node", skip(self))]
	pub async fn expire_nodes(&self) -> Result<(), Error> {
		// Log when this method is run
		trace!(target: TARGET, "Archiving expired nodes in the cluster");
		// Open transaction and fetch nodes
		let txn = self.transaction(Write, Optimistic).await?;
		let now = self.clock_now().await;
		let nds = catch!(txn, txn.all_nodes());
		for nd in nds.iter() {
			// Check that the node is active
			if nd.is_active() {
				// Check if the node has expired
				if nd.hb < now - Duration::from_secs(30) {
					// Log the live query scanning
					trace!(target: TARGET, id = %nd.id, "Archiving node in the cluster");
					// Mark the node as archived
					let val = nd.archive();
					// Get the key for the node entry
					let key = crate::key::root::nd::new(nd.id);
					// Update the node entry
					catch!(txn, txn.set(key, val));
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
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::node", skip(self))]
	pub async fn cleanup_nodes(&self) -> Result<(), Error> {
		// Log when this method is run
		trace!(target: TARGET, "Cleaning up archived nodes in the cluster");
		// Fetch all of the expired nodes
		let expired = {
			let txn = self.transaction(Read, Optimistic).await?;
			let nds = catch!(txn, txn.all_nodes());
			// Filter the archived nodes
			nds.iter().filter_map(Node::archived).collect::<Vec<_>>()
		};
		// Delete the live queries
		{
			for id in expired.iter() {
				// Log the live query scanning
				trace!(target: TARGET, id = %id, "Deleting live queries for node");
				// Scan the live queries for this node
				let txn = self.transaction(Write, Optimistic).await?;
				let beg = crate::key::node::lq::prefix(*id);
				let end = crate::key::node::lq::suffix(*id);
				let mut next = Some(beg..end);
				while let Some(rng) = next {
					let res = catch!(txn, txn.batch(rng, *NORMAL_FETCH_SIZE, true));
					next = res.next;
					for (k, v) in res.values.iter() {
						// Decode the data for this live query
						let val: Live = v.into();
						// Get the key for this node live query
						let nlq = crate::key::node::lq::Lq::decode(k)?;
						// Check that the node for this query is archived
						if expired.contains(&nlq.nd) {
							// Get the key for this table live query
							let tlq = crate::key::table::lq::new(&val.ns, &val.db, &val.tb, nlq.lq);
							// Delete the table live query
							catch!(txn, txn.del(tlq));
							// Delete the node live query
							catch!(txn, txn.del(nlq));
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
				// Log the node deletion
				trace!(target: TARGET, id = %id, "Deleting node from the cluster");
				// Get the key for the node entry
				let key = crate::key::root::nd::new(*id);
				// Delete the cluster node entry
				catch!(txn, txn.del(key));
			}
			// Commit the changes
			txn.commit().await?;
		}
		// Everything was successful
		Ok(())
	}

	/// Clean up all other miscellaneous data.
	///
	/// This function should be run periodically at an interval.
	///
	/// This function clears up all data which might have been missed from
	/// previous cleanup runs, or when previous runs failed. This function
	/// currently deletes all live queries, for nodes which no longer exist
	/// in the cluster, from all namespaces, databases, and tables. It uses
	/// a number of transactions in order to prevent failure of large or
	/// long-running transactions on distributed storage engines.
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::node", skip(self))]
	pub async fn garbage_collect(&self) -> Result<(), Error> {
		// Log the node deletion
		trace!(target: TARGET, "Garbage collecting all miscellaneous data");
		// Fetch expired nodes
		let expired = {
			let txn = self.transaction(Read, Optimistic).await?;
			let nds = catch!(txn, txn.all_nodes());
			// Filter the archived nodes
			nds.iter().filter_map(Node::archived).collect::<Vec<_>>()
		};
		// Fetch all namespaces
		let nss = {
			let txn = self.transaction(Read, Optimistic).await?;
			catch!(txn, txn.all_ns())
		};
		// Loop over all namespaces
		for ns in nss.iter() {
			// Log the namespace
			trace!(target: TARGET, "Garbage collecting data in namespace {}", ns.name);
			// Fetch all databases
			let dbs = {
				let txn = self.transaction(Read, Optimistic).await?;
				catch!(txn, txn.all_db(&ns.name))
			};
			// Loop over all databases
			for db in dbs.iter() {
				// Log the namespace
				trace!(target: TARGET, "Garbage collecting data in database {}/{}", ns.name, db.name);
				// Fetch all tables
				let tbs = {
					let txn = self.transaction(Read, Optimistic).await?;
					catch!(txn, txn.all_tb(&ns.name, &db.name))
				};
				// Loop over all tables
				for tb in tbs.iter() {
					// Log the namespace
					trace!(target: TARGET, "Garbage collecting data in table {}/{}/{}", ns.name, db.name, tb.name);
					// Iterate over the table live queries
					let txn = self.transaction(Write, Optimistic).await?;
					let beg = crate::key::table::lq::prefix(&ns.name, &db.name, &tb.name);
					let end = crate::key::table::lq::suffix(&ns.name, &db.name, &tb.name);
					let mut next = Some(beg..end);
					while let Some(rng) = next {
						let res = catch!(txn, txn.batch(rng, *NORMAL_FETCH_SIZE, true));
						next = res.next;
						for (k, v) in res.values.iter() {
							// Decode the LIVE query statement
							let stm: LiveStatement = v.into();
							// Get the key for this node live query
							let tlq = crate::key::table::lq::Lq::decode(k)?;
							// Get the node id and the live query id
							let (nid, lid) = (stm.node.0, stm.id.0);
							// Check that the node for this query is archived
							if expired.contains(&stm.node) {
								// Get the key for this table live query
								let nlq = crate::key::node::lq::new(nid, lid);
								// Delete the node live query
								catch!(txn, txn.del(nlq));
								// Delete the table live query
								catch!(txn, txn.del(tlq));
							}
						}
					}
					// Commit the changes
					txn.commit().await?;
				}
			}
		}
		// All ok
		Ok(())
	}

	/// Clean up the live queries for a disconnected connection.
	///
	/// This function should be run when a WebSocket disconnects.
	///
	/// This function clears up the live queries on the current node, which
	/// are specified by uique live query UUIDs. This is necessary when a
	/// WebSocket disconnects, and any associated live queries need to be
	/// cleaned up and removed.
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::node", skip(self))]
	pub async fn delete_queries(&self, ids: Vec<uuid::Uuid>) -> Result<(), Error> {
		// Log the node deletion
		trace!(target: TARGET, "Deleting live queries for a connection");
		// Fetch expired nodes
		let txn = self.transaction(Write, Optimistic).await?;
		// Loop over the live query unique ids
		for id in ids.into_iter() {
			// Get the key for this node live query
			let nlq = crate::key::node::lq::new(self.id(), id);
			// Fetch the LIVE meta data node entry
			if let Some(val) = catch!(txn, txn.get(nlq, None)) {
				// Decode the data for this live query
				let lq: Live = val.into();
				// Get the key for this node live query
				let nlq = crate::key::node::lq::new(self.id(), id);
				// Get the key for this table live query
				let tlq = crate::key::table::lq::new(&lq.ns, &lq.db, &lq.tb, id);
				// Delete the table live query
				catch!(txn, txn.del(tlq));
				// Delete the node live query
				catch!(txn, txn.del(nlq));
			}
		}
		// Commit the changes
		txn.commit().await?;
		// All ok
		Ok(())
	}
}
