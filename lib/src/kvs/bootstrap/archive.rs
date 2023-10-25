use std::time::Duration;

use rand::Rng;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::err::BootstrapCause::{ChannelRecvError, ChannelSendError};
use crate::err::ChannelVariant::{BootstrapArchive, BootstrapTxSupplier};
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
				if bor.1.is_some() {
					// send any errors further on, because we don't need to process them
					// unless we can handle them. Currently we can't.
					// if we error on send, then we bubble up because this shouldn't happen
					sender
						.send(bor)
						.await
						.map_err(|_| Error::BootstrapError(ChannelSendError(BootstrapArchive)))?;
				} else {
					msg.push(bor);
					if msg.len() >= batch_size {
						let results =
							archive_live_query_batch(tx_req.clone(), node_id, &mut msg).await?;
						for boresult in results {
							sender.send(boresult).await.map_err(|_| {
								Error::BootstrapError(ChannelSendError(BootstrapArchive))
							})?;
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
							sender.send(boresult).await.map_err(|_| {
								Error::BootstrapError(ChannelSendError(BootstrapArchive))
							})?;
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
					sender
						.send(boresult)
						.await
						.map_err(|_| Error::BootstrapError(ChannelSendError(BootstrapArchive)))?;
				}
				// msg should always be drained but in case it isn't, we clear
				msg.clear();
			}
		}
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
	// TODO test failed tx retries
	let mut last_err = None;
	for _ in 0..ds::BOOTSTRAP_TX_RETRIES {
		for (lq, e) in ret.drain(..) {
			msg.push((lq, e));
		}
		// Fast-return
		if msg.len() <= 0 {
			trace!("archive fast return because msg.len() <= 0");
			break;
		}
		trace!("Receiving a tx response in archive");
		let (tx_req_oneshot, tx_res_oneshot): (TxRequestOneshot, TxResponseOneshot) =
			oneshot::channel();
		if let Err(_send_error) = tx_req.send(tx_req_oneshot).await {
			last_err = Some(Error::BootstrapError(ChannelSendError(BootstrapTxSupplier)));
			continue;
		}
		match tx_res_oneshot.await {
			Ok(mut tx) => {
				trace!("Received tx in archive");
				// In case this is a retry, we re-hydrate the msg vector
				// Consume the input message vector of live queries to archive
				for (lq, _error_should_not_exist) in msg.drain(..) {
					// Retrieve the existing table live query
					let lv_res = tx
						.get_tb_live(lq.ns.as_str(), lq.db.as_str(), lq.tb.as_str(), &lq.lq)
						.await;
					// Maybe it won't work. Not handled atm, so treat as valid error
					if let Err(e) = lv_res {
						// TODO wrap error with context that this step failed; requires self-ref error
						ret.push((lq, Some(e)));
						continue;
					}
					let lv = lv_res.unwrap();
					// If the lq is already archived, we can remove it from bootstrap
					if !lv.archived.is_some() {
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
				// TODO where can the above transaction hard fail? Every op needs rollback?
				if let Err(e) = tx.commit().await {
					last_err = Some(e);
					if let Err(e) = tx.cancel().await {
						// TODO wrap?
						last_err = Some(e);
					}
				} else {
					trace!("archive committed tx happy path");
					break;
				}
				// TODO second happy path commit?
				trace!("archive committing tx second happy");
				if let Err(e) = tx.commit().await {
					trace!("failed to commit tx: {:?}", e);
					last_err = Some(e);
					if let Err(e) = tx.cancel().await {
						trace!("failed to rollback tx: {:?}", e);
						// TODO wrap?
						last_err = Some(e);
					}
				} else {
					break;
				}
				trace!("outside the commit check");
			}
			Err(e) => {
				error!("Failed to archive live queries: {:?}", e);
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
	use crate::err::Error;
	use futures_concurrency::future::FutureExt;
	use std::str::FromStr;
	use std::sync::Arc;
	use std::time::Duration;
	use tokio::sync::mpsc;

	use crate::kvs::bootstrap::{archive_live_queries, TxRequestOneshot};
	use crate::kvs::LockType::Optimistic;
	use crate::kvs::TransactionType::Write;
	use crate::kvs::{BootstrapOperationResult, Datastore, LqValue};
	use crate::sql::Uuid;

	const RETRY_DURATION: Duration = Duration::from_millis(0);

	#[tokio::test]
	async fn test_empty_archive() {
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

		let node_id = Uuid::from_str("921f427a-e9d8-43ef-a419-e018711031cb").unwrap();
		let arch_task = tokio::spawn(archive_live_queries(
			tx_req,
			node_id,
			input_lq_recv,
			output_lq_send,
			10,
			&RETRY_DURATION,
		));

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
	async fn test_batch_invalid_scan() {
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
		// test will require configurable batch size
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
