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
		match tokio::time::timeout(ds::BOOTSTRAP_BATCH_LATENCY, archived_recv.recv()).await {
			Ok(Some(bor)) => {
				if bor.1.is_some() {
					sender
						.send(bor)
						.await
						.map_err(|_e| Error::BootstrapError(ChannelSendError(BootstrapDelete)))?;
				} else {
					msg.push(bor);
					if msg.len() >= batch_size {
						let results = delete_live_query_batch(tx_req.clone(), &mut msg).await?;
						for boresult in results {
							sender.send(boresult).await.map_err(|_e| {
								Error::BootstrapError(ChannelSendError(BootstrapDelete))
							})?;
						}
						// msg should always be drained but in case it isn't, we clear
						msg.clear();
					}
				}
			}
			Ok(None) => {
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
	for _ in 0..ds::BOOTSTRAP_TX_RETRIES {
		// In case this is a retry, we re-hydrate the msg vector
		for (lq, e) in ret.drain(..) {
			msg.push((lq, e));
		}
		// Fast-return
		if msg.len() <= 0 {
			trace!("Delete fast return because msg.len() <= 0");
			break;
		}
		let (tx_req_oneshot, tx_res_oneshot): (TxRequestOneshot, TxResponseOneshot) =
			oneshot::channel();
		if let Err(_send_error) = tx_req.send(tx_req_oneshot).await {
			last_err = Some(Error::BootstrapError(ChannelSendError(BootstrapTxSupplier)));
			continue;
		}
		trace!("Receiving a tx response in delete");
		match tx_res_oneshot.await {
			Ok(mut tx) => {
				trace!("Received tx in delete");
				// Consume the input message vector of live queries to archive
				for (lq, _e) in msg.drain(..) {
					// Delete the node live query
					if let Err(e) = tx.del_ndlq(*(&lq).nd, *(&lq).lq, &lq.ns, &lq.db).await {
						error!("Failed deleting node live query: {:?}", e);
						// TODO wrap error with context that this step failed; requires self-ref error
						ret.push((lq, Some(e)));
						continue;
					}
					// Delete the table live query
					if let Err(e) = tx.del_tblq(&lq.ns, &lq.db, &lq.tb, *(&lq).lq).await {
						error!("Failed deleting table live query: {:?}", e);
						// TODO wrap error with context that this step failed; requires self-ref error
						ret.push((lq, Some(e)));
						continue;
					}
					// Delete the notifications
					// TODO hypothetical impl
					if let Err(e) = Ok(()) {
						error!("Failed deleting notifications: {:?}", e);
						// TODO wrap error with context that this step failed; requires self-ref error
						ret.push((lq, Some(e)));
					}
				}
				// TODO where can the above transaction hard fail? Every op needs rollback?
				if let Err(e) = tx.commit().await {
					// TODO wrap?
					match tx.cancel().await {
						Ok(_) => {
							trace!("Commit failed, but rollback succeeded when deleting ndlq+tblq");
							last_err = Some(e);
						}
						Err(e2) => {
							// TODO wrap?
							error!("Failed to rollback tx: {:?}, original: {:?}", e2, e);
							last_err = Some(e2);
						}
					}
					continue;
				} else {
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
	use crate::dbs::Session;
	use crate::err::Error;
	use futures_concurrency::future::FutureExt;
	use std::str::FromStr;
	use std::sync::Arc;
	use std::time::Duration;
	use tokio::sync::mpsc;

	use crate::kvs::bootstrap::{archive_live_queries, delete_live_queries, TxRequestOneshot};
	use crate::kvs::LockType::Optimistic;
	use crate::kvs::TransactionType::Write;
	use crate::kvs::{BootstrapOperationResult, Datastore, LqValue};
	use crate::sql::{Uuid, Value};

	const RETRY_DURATION: Duration = Duration::from_millis(0);

	#[tokio::test]
	async fn test_empty_channel() {
		let ds = Arc::new(Datastore::new("memory").await.unwrap());
		let (tx_req, tx_res) = mpsc::channel(1);
		let tx_task = tokio::spawn(always_give_tx(ds, tx_res));

		let (input_lq_send, input_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);
		let (output_lq_send, mut output_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);

		let arch_task =
			tokio::spawn(delete_live_queries(tx_req, input_lq_recv, output_lq_send, 10));

		// deliberately close channel
		drop(input_lq_send);

		// Wait for output
		assert!(output_lq_recv.recv().await.is_none());

		let (tx_task_res, arch_task_res) =
			tokio::time::timeout(Duration::from_millis(1000), tx_task.join(arch_task))
				.await
				.unwrap();
		let tx_req_count = tx_task_res.unwrap().unwrap();
		assert_eq!(tx_req_count, 0);
		arch_task_res.unwrap().unwrap();
	}

	#[tokio::test]
	async fn inbound_messages_invalid() {
		let ds = Arc::new(Datastore::new("memory").await.unwrap());
		let (tx_req, tx_res) = mpsc::channel(1);
		let tx_task = tokio::spawn(always_give_tx(ds, tx_res));

		let (input_lq_send, input_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);
		let (output_lq_send, mut output_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);

		let live_query_id = Uuid::from_str("587bebb8-707a-4ae7-91cb-2edbae95423e").unwrap();
		let arch_task =
			tokio::spawn(delete_live_queries(tx_req, input_lq_recv, output_lq_send, 10));

		// Send input request
		input_lq_send
			.send((
				LqValue {
					nd: Default::default(),
					ns: "".to_string(),
					db: "".to_string(),
					tb: "".to_string(),
					lq: live_query_id,
				},
				None,
			))
			.await
			.unwrap();

		// Wait for output
		let val = output_lq_recv.recv().await;
		assert!(val.is_some());
		let val = val.unwrap();

		// There is a not found error
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

		// Close channel for shutdown
		drop(input_lq_send);

		// Wait for output
		let (tx_task_res, arch_task_res) =
			tokio::time::timeout(Duration::from_millis(1000), tx_task.join(arch_task))
				.await
				.unwrap();
		let tx_req_count = tx_task_res.unwrap().unwrap();
		assert_eq!(tx_req_count, 1);
		arch_task_res.unwrap().unwrap();
	}

	#[tokio::test]
	async fn test_handles_batches_correctly() {
		let ds = Arc::new(Datastore::new("memory").await.unwrap());
		let (tx_req, tx_res) = mpsc::channel(1);
		let tx_task = tokio::spawn(always_give_tx(ds.clone(), tx_res));

		let (input_lq_send, input_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);
		let (output_lq_send, mut output_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);

		let self_node_id = Uuid::from_str("ac35aa6f-ab10-48a5-a3d9-d4439e1c91bc").unwrap();
		let namespace = "sample-namespace";
		let database = "sample-db";
		let table = "sampleTable";
		let sess = Session::owner().with_rt(true).with_ns(namespace).with_db(database);
		let arch_task = tokio::spawn(archive_live_queries(
			tx_req,
			*&self_node_id,
			input_lq_recv,
			output_lq_send,
			10,
			&RETRY_DURATION,
		));

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

		// Wait for output
		let val = output_lq_recv.recv().await;
		assert!(val.is_some());
		let val = val.unwrap();

		// There is a not found error
		assert!(val.1.is_none());

		// Close channel for shutdown
		drop(input_lq_send);

		// Wait for output
		let (tx_task_res, arch_task_res) =
			tokio::time::timeout(Duration::from_millis(1000), tx_task.join(arch_task))
				.await
				.unwrap();
		let tx_req_count = tx_task_res.unwrap().unwrap();
		assert_eq!(tx_req_count, 1);
		arch_task_res.unwrap().unwrap();

		// Now verify that it was in fact archived
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap();
		let res = tx.get_tb_live(namespace, database, table, &live_query_id.0).await;
		tx.commit().await.unwrap();
		match res {
			Ok(_) => {
				panic!("Expected not found error")
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
	}

	async fn always_give_tx(
		ds: Arc<Datastore>,
		mut tx_req_channel: mpsc::Receiver<TxRequestOneshot>,
	) -> Result<u32, Error> {
		let mut count = 0 as u32;
		loop {
			let req = tx_req_channel.recv().await;
			match req {
				None => break,
				Some(r) => {
					count += 1;
					let tx = ds.transaction(Write, Optimistic).await?;
					if let Err(mut tx) = r.send(tx) {
						// The other side of the channel was probably closed
						// Do not reduce count, because it was requested
						tx.cancel().await?;
					}
				}
			}
		}
		Ok(count)
	}
}
