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
) -> Result<(), Error> {
	let mut msg: Vec<BootstrapOperationResult> = Vec::with_capacity(ds::BOOTSTRAP_BATCH_SIZE);
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
					if msg.len() >= ds::BOOTSTRAP_BATCH_SIZE {
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
				// In case this is a retry, we re-hydrate the msg vector
				for (lq, e) in ret.drain(..) {
					msg.push((lq, e));
				}
				// Fast-return
				if msg.len() <= 0 {
					trace!("Delete fast return because msg.len() <= 0");
					last_err = tx.cancel().await.err();
					break;
				}
				// Consume the input message vector of live queries to archive
				if msg.len() > 0 {
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
								trace!(
									"Commit failed, but rollback succeeded when deleting ndlq+tblq"
								);
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
