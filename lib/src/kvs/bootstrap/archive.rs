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
) -> Result<(), Error> {
	let mut msg: Vec<BootstrapOperationResult> = Vec::with_capacity(ds::BOOTSTRAP_BATCH_SIZE);
	loop {
		match tokio::time::timeout(ds::BOOTSTRAP_BATCH_LATENCY, scan_recv.recv()).await {
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
					if msg.len() >= ds::BOOTSTRAP_BATCH_SIZE {
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
					Err(e) => {}
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
		let (tx_req_oneshot, tx_res_oneshot): (TxRequestOneshot, TxResponseOneshot) =
			oneshot::channel();
		if let Err(_send_error) = tx_req.send(tx_req_oneshot).await {
			last_err = Some(Error::BootstrapError(ChannelSendError(BootstrapTxSupplier)));
			continue;
		}
		trace!("Receiving a tx response in archive");
		match tx_res_oneshot.await {
			Ok(mut tx) => {
				trace!("Received tx in archive");
				// In case this is a retry, we re-hydrate the msg vector
				for (lq, e) in ret.drain(..) {
					msg.push((lq, e));
				}
				// Fast-return
				if msg.len() <= 0 {
					trace!("archive fast return because msg.len() <= 0");
					last_err = tx.cancel().await.err();
					break;
				}
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
