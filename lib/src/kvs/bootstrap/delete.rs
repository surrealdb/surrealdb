use std::time::Duration;

use rand::Rng;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::err::BootstrapCause::{ChannelRecvError, ChannelSendError};
use crate::err::ChannelVariant::{BootstrapDelete, BootstrapTxSupplier};
use crate::err::Error;
use crate::kvs::bootstrap::{TxRequestOneshot, TxResponseOneshot};
use crate::kvs::{ds, BootstrapOperationResult};

/// Given a receiver channel of archived live queries,
/// Delete the node lq, table lq, and notifications
/// and send the results to the sender channel
pub(crate) async fn delete_live_queries(
	tx_req: mpsc::Sender<TxRequestOneshot>,
	mut archived_recv: mpsc::Receiver<BootstrapOperationResult>,
	sender: mpsc::Sender<BootstrapOperationResult>,
	batch_size: usize,
) -> Result<(), Error> {
	let mut msg: Vec<BootstrapOperationResult> = Vec::with_capacity(batch_size);
	loop {
		println!("[DELETE] Delete loop!");
		match tokio::time::timeout(ds::BOOTSTRAP_BATCH_LATENCY, archived_recv.recv()).await {
			Ok(Some(bor)) => {
				println!("[DELETE] In delete, got an operation result");
				if bor.1.is_some() {
					// There is an error, we do not process the entry only feed it down
					sender
						.send(bor)
						.await
						.map_err(|_e| Error::BootstrapError(ChannelSendError(BootstrapDelete)))?;
				} else {
					// No error, we process the entry by adding it to the buffer of messages we want to process
					msg.push(bor);
					// If the buffer size has exceeded for batch processing then we process
					if msg.len() >= batch_size {
						let results = delete_live_query_batch(tx_req.clone(), &mut msg).await?;
						println!("[DELETE] Sending {} results", results.len());
						for boresult in results {
							sender.send(boresult).await.map_err(|e| {
								error!("There was an error processing the batch, {}", e);
								Error::BootstrapError(ChannelSendError(BootstrapDelete))
							})?;
						}
						// msg should always be drained but in case it isn't, we clear
						msg.clear();
					}
				}
			}
			Ok(None) => {
				println!("[DELETE] Channel closed in delete live queries");
				// Channel closed, process whatever is remaining
				let results = delete_live_query_batch(tx_req.clone(), &mut msg).await?;
				for boresult in results {
					sender
						.send(boresult)
						.await
						.map_err(|_e| Error::BootstrapError(ChannelSendError(BootstrapDelete)))?;
				}
				break;
			}
			Err(_elapsed) => {
				println!("[DELETE] Timeout in delete live queries");
				// Timeout expired
				let results = delete_live_query_batch(tx_req.clone(), &mut msg).await?;
				for boresult in results {
					sender
						.send(boresult)
						.await
						.map_err(|_e| Error::BootstrapError(ChannelSendError(BootstrapDelete)))?;
				}
				// msg should always be drained but in case it isn't, we clear
				msg.clear();
			}
		}
	}
	Ok(())
}

/// Given a batch of archived live queries,
/// Delete the node lq, table lq, and notifications
async fn delete_live_query_batch(
	tx_req: mpsc::Sender<TxRequestOneshot>,
	msg: &mut Vec<BootstrapOperationResult>,
) -> Result<Vec<BootstrapOperationResult>, Error> {
	let mut ret: Vec<BootstrapOperationResult> = vec![];
	// TODO test failed tx retries
	let mut last_err = None;
	for i in 0..ds::BOOTSTRAP_TX_RETRIES {
		println!("[DELETE] Delete live query batch retry attempt {i}");
		// In case this is a retry, we re-hydrate the msg vector
		for (lq, e) in ret.drain(..) {
			println!("[DELETE] Delete adding {:?} to batch", lq);
			msg.push((lq, e));
		}
		// Fast-return
		if msg.is_empty() {
			println!("[DELETE] Delete fast return because msg is empty");
			trace!("Delete fast return because msg is empty");
			break;
		}
		println!("[DELETE] Requesting tx");
		let (tx_req_oneshot, tx_res_oneshot): (TxRequestOneshot, TxResponseOneshot) =
			oneshot::channel();
		if let Err(send_error) = tx_req.send(tx_req_oneshot).await {
			println!("[DELETE] Failed to send tx request: {}", send_error);
			error!("Failed to send tx request: {}", send_error);
			last_err = Some(Error::BootstrapError(ChannelSendError(BootstrapTxSupplier)));
			continue;
		}
		println!("[DELETE] Received tx response");
		trace!("Receiving a tx response in delete");
		match tx_res_oneshot.await {
			Ok(mut tx) => {
				println!("[DELETE] Tx response was good");
				trace!("Received tx in delete");
				// Consume the input message vector of live queries to archive
				for (lq, _e) in msg.drain(..) {
					// TODO check if e has error and send and skip
					// Delete the node live query
					println!("[DELETE] Deleting live query: {:?}", lq);
					// NOTE: deleting missing entries does not error
					if let Err(e) = tx.del_ndlq(*lq.nd, *lq.lq, &lq.ns, &lq.db).await {
						println!("[DELETE] Failed deleting node live query: {:?}", e);
						error!("Failed deleting node live query: {:?}", e);
						// TODO wrap error with context that this step failed; requires self-ref error
						ret.push((lq, Some(e)));
						continue;
					}
					// Delete the table live query
					if let Err(e) = tx.del_tblq(&lq.ns, &lq.db, &lq.tb, *lq.lq).await {
						println!("[DELETE] Failed deleting table live query: {:?}", e);
						error!("Failed deleting table live query: {:?}", e);
						// TODO wrap error with context that this step failed; requires self-ref error
						ret.push((lq, Some(e)));
						continue;
					}
					// We do not delete the notifications - they should not be written in the first
					// place, since that was not merged.
				}
				// TODO where can the above transaction hard fail? Every op needs rollback?
				println!("[DELETE] Committing transaction after {} writes", ret.len());
				trace!("Committing transaction after {} writes", ret.len());
				if let Err(e) = tx.commit().await {
					// TODO wrap?
					match tx.cancel().await {
						Ok(_) => {
							println!(
								"[DELETE] Commit failed, but rollback succeeded when deleting ndlq+tblq"
							);
							error!("Commit failed, but rollback succeeded when deleting ndlq+tblq");
							last_err = Some(e);
						}
						Err(e2) => {
							// TODO wrap?
							println!("[DELETE] Failed to rollback tx: {:?}, original: {:?}", e2, e);
							error!("Failed to rollback tx: {:?}, original: {:?}", e2, e);
							last_err = Some(e2);
						}
					}
					continue;
				} else {
					println!("[DELETE] delete lq committed tx happy path");
					trace!("delete lq committed tx happy path");
					break;
				}
			}
			Err(_recv_error) => {
				// Channel dropped without sending from other side
				last_err = Some(Error::BootstrapError(ChannelRecvError(BootstrapTxSupplier)));
			}
		}
		if last_err.is_some() {
			// If there are 2 conflicting bootstraps, we don't want them to continue
			// continue colliding at the same time. So we scatter the retry sleep
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

	use test_util::always_give_tx;

	use crate::dbs::Session;
	use crate::err::Error;
	use crate::kvs::bootstrap::{delete_live_queries, test_util};
	use crate::kvs::LockType::Optimistic;
	use crate::kvs::TransactionType::Write;
	use crate::kvs::{BootstrapOperationResult, Datastore, LqValue};
	use crate::sql::{Uuid, Value};

	// const RETRY_DURATION: Duration = Duration::from_millis(0);

	#[tokio::test]
	async fn test_empty_channel() {
		let ds = Arc::new(Datastore::new("memory").await.unwrap().with_notifications());
		let (tx_req, tx_res) = mpsc::channel(1);
		let tx_task = tokio::spawn(always_give_tx(ds, tx_res));

		// Input and output task channels
		let (input_lq_send, input_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);
		let (output_lq_send, mut output_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);

		// Start the task
		let arch_task =
			tokio::spawn(delete_live_queries(tx_req, input_lq_recv, output_lq_send, 10));

		// deliberately close channel
		drop(input_lq_send);

		// Wait for tasks to complete
		let (tx_task_res, arch_task_res) =
			tokio::time::timeout(Duration::from_millis(1000), tx_task.join(arch_task))
				.await
				.unwrap();

		// Validate no transactions requested
		let tx_req_count = tx_task_res.unwrap().unwrap();
		assert_eq!(tx_req_count, 0);

		// Archive task did not error
		arch_task_res.unwrap().unwrap();

		// Validate the output channel was closed
		assert!(output_lq_recv.try_recv().is_err());
	}

	#[tokio::test]
	async fn test_invalid_message() {
		let ds = Arc::new(Datastore::new("memory").await.unwrap().with_notifications());
		let (tx_req, tx_res) = mpsc::channel(1);
		let tx_task = tokio::spawn(always_give_tx(ds.clone(), tx_res));

		// input and output channels
		let (input_lq_send, input_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);
		let (output_lq_send, mut output_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);

		// Start delete task
		let live_query_id = Uuid::from_str("587bebb8-707a-4ae7-91cb-2edbae95423e").unwrap();
		let delete_task =
			tokio::spawn(delete_live_queries(tx_req, input_lq_recv, output_lq_send, 10));

		// Send input request
		input_lq_send
			.send((
				LqValue {
					nd: ds.id,
					ns: "some_namespace".to_string(),
					db: "some_database".to_string(),
					tb: "some_table".to_string(),
					lq: live_query_id,
				},
				None,
			))
			.await
			.unwrap();

		// End processing
		drop(input_lq_send);

		// Wait for tasks to complete
		let (tx_task_res, delete_task_res) =
			tokio::time::timeout(Duration::from_millis(1000), tx_task.join(delete_task))
				.await
				.unwrap();
		//  Validate number of transactions
		let tx_req_count = tx_task_res.unwrap().unwrap();
		assert_eq!(tx_req_count, 1);

		// Validate delete task completed successfully
		delete_task_res.unwrap().unwrap();

		// Validate a successfully deleted message (even if not exists) does not result in an event
		let val = output_lq_recv.try_recv();
		assert!(val.is_err());

		// And the deleted lq is actually deleted
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		let tb_res =
			tx.get_tb_live("some_namespace", "some_database", "some_table", &live_query_id.0).await;
		tx.cancel().await.unwrap();
		assert!(tb_res.is_err());
		println!("Finished test")
	}

	#[tokio::test]
	async fn test_handles_batches_correctly() {
		let ds = Arc::new(Datastore::new("memory").await.unwrap().with_notifications());
		let (tx_req, tx_res) = mpsc::channel(1);
		let tx_task = tokio::spawn(always_give_tx(ds.clone(), tx_res));

		// Setup input and output channels
		let (input_lq_send, input_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);
		let (output_lq_send, mut output_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);

		let self_node_id = Uuid::from_str("ac35aa6f-ab10-48a5-a3d9-d4439e1c91bc").unwrap();
		let namespace = "sampleNamespace";
		let database = "sampleDb";
		let table = "sampleTable";
		let sess = Session::owner().with_rt(true).with_ns(namespace).with_db(database);
		let delete_task =
			tokio::spawn(delete_live_queries(tx_req, input_lq_recv, output_lq_send, 10));

		let query = format!("USE NS {namespace} DB {database};");
		if let Some(e) = ds
			.execute(&query, &sess, None)
			.await
			.unwrap()
			.iter()
			.filter_map(|resp| match &resp.result {
				Ok(_) => None,
				Err(e) => Some(e),
			})
			.next()
		{
			panic!("Failed to use namespace and database: {:?}", e);
		}
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

		println!("Sent and waiting for delete");

		// Close channel for shutdown
		drop(input_lq_send);

		let msg = output_lq_recv.recv().await;
		assert!(msg.is_none(), "Expected channel to close without a message");

		// Wait for output
		let (tx_task_res, delete_task_res) =
			tokio::time::timeout(Duration::from_millis(1000), tx_task.join(delete_task))
				.await
				.unwrap();
		let tx_req_count = tx_task_res.unwrap().unwrap();
		assert_eq!(tx_req_count, 1);
		delete_task_res.unwrap().unwrap();

		// Now verify that it was in fact deleted
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		let tb_res = tx.get_tb_live(namespace, database, table, &live_query_id.0).await;
		let nd_res = tx.scan_ndlq(&self_node_id, 1000).await;
		tx.commit().await.unwrap();
		match tb_res {
			Ok(_) => {
				panic!("Expected error due to live query being deleted")
			}
			Err(e) => match e {
				Error::LvNotFound {
					value,
				} => {
					assert_eq!(value, live_query_id.0.to_string());
				}
				_ => panic!("Expected LvNotFound error"),
			},
		}
		let nd_res = nd_res.unwrap();
		assert_eq!(nd_res.len(), 0);
	}
}
