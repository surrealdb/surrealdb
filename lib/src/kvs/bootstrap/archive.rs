use std::time::Duration;

use rand::Rng;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::err::Error;
use crate::kvs::bootstrap::{TxRequestOneshot, TxResponseOneshot};
use crate::kvs::{ds, BootstrapOperationResult};
use crate::sql::Uuid;

/// This task will read input live queries from a receiver in batches and
/// archive them and finally send them to the output channel.
/// The task terminates if there is an irrecoverable error or if the input
/// channel has been closed (dropped, from previous task).
pub(crate) async fn archive_live_queries(
	tx_req: mpsc::Sender<TxRequestOneshot>,
	node_id: Uuid,
	mut scan_recv: mpsc::Receiver<BootstrapOperationResult>,
	sender: mpsc::Sender<BootstrapOperationResult>,
	batch_size: usize,
	batch_latency: &Duration,
) -> Result<(), Error> {
	let mut msg: Vec<BootstrapOperationResult> = Vec::with_capacity(batch_size);
	loop {
		match tokio::time::timeout(*batch_latency, scan_recv.recv()).await {
			Ok(Some(bor)) => {
				let bootstrap_operation_result_is_err = bor.1.is_some();
				if bootstrap_operation_result_is_err {
					// send any errors further on, because we don't need to process them
					// unless we can handle them. Currently we can't.
					// if we error on send, then we bubble up because this shouldn't happen
					sender.send(bor).await.expect(
						"The bootstrap archive task was unable to send a bootstrap operation error",
					);
				} else {
					msg.push(bor);
					if msg.len() >= batch_size {
						let results =
							archive_live_query_batch(tx_req.clone(), node_id, &mut msg).await?;
						for boresult in results {
							sender.send(boresult).await.expect("The bootstrap archive task was unable to send a bootstrap operation message as part of batch");
						}
						// msg should always be drained but in case it isn't, we clear
						msg.clear();
					}
				}
			}
			Ok(None) => {
				// Channel closed, process whatever is remaining
				match archive_live_query_batch(tx_req.clone(), node_id, &mut msg).await {
					Ok(results) => {
						for boresult in results {
							sender.send(boresult).await.expect("The bootstrap archive task was unable to send a bootstrap operation message after the input channel was closed")
						}
						break;
					}
					Err(e) => {
						error!("Failed to archive live queries: {:?}", e);
					}
				}
			}
			Err(_elapsed) => {
				// Timeout expired
				let results = archive_live_query_batch(tx_req.clone(), node_id, &mut msg).await?;
				for boresult in results {
					sender.send(boresult).await.expect("The bootstrap archive task was unable to send a bootstrap operation message after the timeout expired");
				}
				// msg should always be drained but in case it isn't, we clear
				msg.clear();
			}
		}
		trace!("Finished archive task, waiting for next message")
	}
	Ok(())
}

/// Given a batch of messages that indicate live queries to archive,
/// try to mark them as archived and send to the sender channel
/// for further processing.
async fn archive_live_query_batch(
	tx_req: mpsc::Sender<TxRequestOneshot>,
	node_id: Uuid,
	msg: &mut Vec<BootstrapOperationResult>,
) -> Result<Vec<BootstrapOperationResult>, Error> {
	let mut ret: Vec<BootstrapOperationResult> = vec![];
	let mut last_err = None;
	for _ in 0..ds::BOOTSTRAP_TX_RETRIES {
		for (lq, e) in ret.drain(..) {
			msg.push((lq, e));
		}
		// Fast-return
		if msg.is_empty() {
			trace!("archive fast return because msg is empty");
			break;
		}
		trace!("Receiving a tx response in archive");
		let (tx_req_oneshot, tx_res_oneshot): (TxRequestOneshot, TxResponseOneshot) =
			oneshot::channel();
		tx_req
			.send(tx_req_oneshot)
			.await
			.expect("The bootstrap archive task was unable to send a transaction request");
		let mut tx = tx_res_oneshot
			.await
			.expect("Bootstrap archive task did not receive a transaction request response");
		trace!("Received tx in archive");
		// In case this is a retry, we re-hydrate the msg vector
		// Consume the input message vector of live queries to archive
		for (lq, _error_should_not_exist) in msg.drain(..) {
			// Retrieve the existing table live query
			let lv_res =
				tx.get_tb_live(lq.ns.as_str(), lq.db.as_str(), lq.tb.as_str(), &lq.lq).await;
			// Maybe it won't work. Not handled atm, so treat as valid error
			if let Err(e) = lv_res {
				ret.push((lq, Some(e)));
				continue;
			}
			let lv = lv_res.unwrap();
			// If the lq is already archived, we can remove it from bootstrap
			let already_archived = lv.archived.is_some();
			match already_archived {
				true => {
					// We don't need to do anything, but we do need to forward the result
					ret.push((lq, None))
				}
				false => {
					// Mark as archived by us (this node) and write back
					let archived_lvs = lv.clone().archive(node_id);
					match tx.putc_tblq(&lq.ns, &lq.db, &lq.tb, archived_lvs, Some(lv)).await {
						Ok(_) => {
							ret.push((lq, None));
						}
						Err(e) => {
							ret.push((lq, Some(e)));
						}
					}
				}
			}
		}
		if let Err(e) = tx.commit().await {
			error!("Error committing transaction during archive: {}", e);
			last_err = Some(e);
			if let Err(e) = tx.cancel().await {
				error!("Error cancelling transaction during archive: {}", e);
				last_err = Some(e);
			}
		} else {
			trace!("archive committed tx happy path");
			break;
		}
		trace!("archive committing tx second happy");
		if let Err(e) = tx.commit().await {
			error!("failed to commit tx: {:?}", e);
			last_err = Some(e);
			if let Err(e) = tx.cancel().await {
				error!("failed to rollback tx: {:?}", e);
				last_err = Some(e);
			}
		} else {
			break;
		}
		trace!("outside the commit check");
		// Check if we should retry
		if last_err.is_some() {
			// If there are 2 conflicting bootstraps, we don't want them to continue
			// colliding at the same time. So we scatter the retry sleep
			let scatter_sleep = rand::thread_rng()
				.gen_range(ds::BOOTSTRAP_TX_RETRY_LOW_MILLIS..ds::BOOTSTRAP_TX_RETRY_HIGH_MILLIS);
			tokio::time::sleep(Duration::from_millis(scatter_sleep)).await;
		} else {
			// Successful transaction 🎉
			break;
		}
	}
	if let Some(e) = last_err {
		return Err(e);
	}
	Ok(ret)
}

#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod test {
	use std::str::FromStr;
	use std::sync::Arc;
	use std::time::Duration;

	use futures_concurrency::future::FutureExt;
	use tokio::sync::mpsc;

	use crate::dbs::Session;
	use crate::err::Error;
	use crate::kvs::bootstrap::archive_live_queries;
	use crate::kvs::bootstrap::test_util::always_give_tx;
	use crate::kvs::LockType::Optimistic;
	use crate::kvs::TransactionType::Write;
	use crate::kvs::{BootstrapOperationResult, Datastore, LqValue};
	use crate::sql::{Uuid, Value};

	const RETRY_DURATION: Duration = Duration::from_millis(0);

	#[tokio::test]
	async fn test_empty_archive() {
		let ds = Arc::new(Datastore::new("memory").await.unwrap());
		let (tx_req, tx_res) = mpsc::channel(1);
		let tx_task = tokio::spawn(always_give_tx(ds, tx_res));

		// Declare the input and output channels of the task
		let (input_lq_send, input_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);
		let (output_lq_send, _output_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);

		// Start the task
		let node_id = Uuid::from_str("921f427a-e9d8-43ef-a419-e018711031cb").unwrap();
		let arch_task = tokio::spawn(archive_live_queries(
			tx_req,
			node_id,
			input_lq_recv,
			output_lq_send,
			10,
			&RETRY_DURATION,
		));

		// Deliberately close channel to indicate it finished
		drop(input_lq_send);

		// Wait for the task to complete, since we closed the input channel
		let (tx_task_res, arch_task_res) =
			tokio::time::timeout(Duration::from_millis(1000), tx_task.join(arch_task))
				.await
				.unwrap();

		// Validate the number of transaction requests
		let tx_req_count = tx_task_res.unwrap().unwrap();
		assert_eq!(tx_req_count, 0);

		// Validate there was no error
		arch_task_res.unwrap().unwrap();
	}

	#[tokio::test]
	async fn test_batch_invalid_scan() {
		let ds = Arc::new(Datastore::new("memory").await.unwrap());
		let (tx_req, tx_res) = mpsc::channel(1);
		let tx_task = tokio::spawn(always_give_tx(ds, tx_res));

		// Declare input and output channels
		let (input_lq_send, input_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);
		let (output_lq_send, mut output_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);

		// Declare the live query that we want to archive
		let node_id = Uuid::from_str("921f427a-e9d8-43ef-a419-e018711031cb").unwrap();
		let live_query_id = Uuid::from_str("fb063201-dc2f-4cb3-bcd8-db3cbf12affd").unwrap();
		let arch_task = tokio::spawn(archive_live_queries(
			tx_req,
			*&node_id,
			input_lq_recv,
			output_lq_send,
			10,
			&RETRY_DURATION,
		));

		// Send input request
		input_lq_send
			.send((
				LqValue {
					nd: Default::default(),
					ns: "some_namespace".to_string(),
					db: "some_database".to_string(),
					tb: "some_table".to_string(),
					lq: live_query_id,
				},
				None,
			))
			.await
			.unwrap();

		// Close channel for shutdown
		drop(input_lq_send);

		// Wait for tasks to complete
		let (tx_task_res, arch_task_res) =
			tokio::time::timeout(Duration::from_millis(1000), tx_task.join(arch_task))
				.await
				.unwrap();

		// Validate the number of transactions
		let tx_req_count = tx_task_res.unwrap().unwrap();
		assert_eq!(tx_req_count, 1);

		// Validate there was no error
		arch_task_res.unwrap().unwrap();

		// Validate the output messages in the channel
		let val = output_lq_recv.recv().await;
		assert!(val.is_some());
		let val = val.unwrap();

		// And the output message is a not found error
		assert!(val.1.is_some());
		let err = val.1.unwrap();
		match err {
			Error::LvNotFound {
				value,
			} => {
				assert_eq!(value, live_query_id.0.to_string());
			}
			_ => panic!("Expected LvNotFound error"),
		}
	}

	#[tokio::test]
	async fn test_task_archives() {
		let ds = Arc::new(Datastore::new("memory").await.unwrap());
		let (tx_req, tx_res) = mpsc::channel(1);
		let tx_task = tokio::spawn(always_give_tx(ds.clone(), tx_res));

		// Setup task input and output channels
		let (input_lq_send, input_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);
		let (output_lq_send, mut output_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);

		// Set up a valid live query to be archived
		let self_node_id = ds.id;
		let namespace = "sample-namespace";
		let database = "sample-db";
		let table = "sampleTable";
		let sess = Session::owner().with_rt(true).with_ns(namespace).with_db(database);
		let query = format!("LIVE SELECT * FROM {table}");
		let mut lq = ds.execute(&query, &sess, None).await.unwrap();
		assert_eq!(lq.len(), 1);
		let live_query_id = lq.remove(0).result.unwrap();
		let live_query_id = match live_query_id {
			Value::Uuid(u) => u,
			_ => {
				panic!("Expected Uuid")
			}
		};

		// Start the task
		let arch_task = tokio::spawn(archive_live_queries(
			tx_req,
			*&self_node_id,
			input_lq_recv,
			output_lq_send,
			10,
			&RETRY_DURATION,
		));

		// Send input request
		input_lq_send
			.send((
				LqValue {
					nd: self_node_id,
					ns: sess.ns.unwrap(),
					db: sess.db.unwrap(),
					tb: table.to_string(),
					lq: live_query_id,
				},
				None,
			))
			.await
			.unwrap();

		// Close channel to initiate shutdown
		drop(input_lq_send);

		// Wait for tasks to complete
		let (tx_task_res, arch_task_res) =
			tokio::time::timeout(Duration::from_millis(1000), tx_task.join(arch_task))
				.await
				.unwrap();

		// Validate the number of transactions
		let tx_req_count = tx_task_res.unwrap().unwrap();
		assert_eq!(tx_req_count, 1);

		// Validate the archive task completed without error
		arch_task_res.unwrap().unwrap();

		// Process output messages and validate no error
		let val = output_lq_recv.recv().await;
		assert!(val.is_some());
		let val = val.unwrap();
		assert!(val.1.is_none());
		assert_eq!(val.0.lq, live_query_id);
		assert_eq!(val.0.nd, self_node_id);
		assert_eq!(val.0.ns, namespace);
		assert_eq!(val.0.db, database);
		assert_eq!(val.0.tb, table);

		// Now verify that live query was in fact archived
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		let res = tx.get_tb_live(namespace, database, table, &live_query_id.0).await;
		tx.commit().await.unwrap();
		let live_stm = res.unwrap();
		assert_eq!(live_stm.archived, Some(self_node_id))
	}

	#[tokio::test]
	async fn test_task_still_forwards_already_archived_live_queries() {
		let ds = Arc::new(Datastore::new("memory").await.unwrap());
		let (tx_req, tx_res) = mpsc::channel(1);
		let tx_task = tokio::spawn(always_give_tx(ds.clone(), tx_res));

		// Setup task input and output channels
		let (input_lq_send, input_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);
		let (output_lq_send, mut output_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);

		// Set up a valid live query to be archived
		let self_node_id = ds.id;
		let namespace = "sample-namespace";
		let database = "sample-db";
		let table = "sampleTable";
		let sess = Session::owner().with_rt(true).with_ns(namespace).with_db(database);
		let query = format!("LIVE SELECT * FROM {table}");
		let mut lq = ds.execute(&query, &sess, None).await.unwrap();
		assert_eq!(lq.len(), 1);
		let live_query_id = lq.remove(0).result.unwrap();
		let live_query_id = match live_query_id {
			Value::Uuid(u) => u,
			_ => {
				panic!("Expected Uuid")
			}
		};

		// Start the task
		let arch_task = tokio::spawn(archive_live_queries(
			tx_req,
			*&self_node_id,
			input_lq_recv,
			output_lq_send,
			10,
			&RETRY_DURATION,
		));

		// Send input request
		input_lq_send
			.send((
				LqValue {
					nd: self_node_id,
					ns: sess.ns.unwrap(),
					db: sess.db.unwrap(),
					tb: table.to_string(),
					lq: live_query_id,
				},
				None,
			))
			.await
			.unwrap();

		// Close channel to initiate shutdown
		drop(input_lq_send);

		// Wait for tasks to complete
		let (tx_task_res, arch_task_res) =
			tokio::time::timeout(Duration::from_millis(1000), tx_task.join(arch_task))
				.await
				.unwrap();

		// Validate the number of transactions
		let tx_req_count = tx_task_res.unwrap().unwrap();
		assert_eq!(tx_req_count, 1);

		// Validate the archive task completed without error
		arch_task_res.unwrap().unwrap();

		// Process output messages and validate no error
		let val = output_lq_recv.recv().await;
		assert!(val.is_some());
		let val = val.unwrap();
		assert!(val.1.is_none());
		assert_eq!(val.0.lq, live_query_id);
		assert_eq!(val.0.nd, self_node_id);
		assert_eq!(val.0.ns, namespace);
		assert_eq!(val.0.db, database);
		assert_eq!(val.0.tb, table);

		// Now verify that live query was in fact archived
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		let res = tx.get_tb_live(namespace, database, table, &live_query_id.0).await;
		tx.commit().await.unwrap();
		let live_stm = res.unwrap();
		assert_eq!(live_stm.archived, Some(self_node_id))
	}
}
