use std::time::Duration;

use rand::Rng;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::err::BootstrapCause::{ChannelRecvError, ChannelSendError};
use crate::err::ChannelVariant::{
	BootstrapArchive, BootstrapDelete, BootstrapScan, BootstrapTxSupplier,
};
use crate::err::Error;
use crate::kvs::{ds, BootstrapOperationResult, Transaction, NO_LIMIT};
use crate::sql::Uuid;

type TxRequestOneshot = oneshot::Sender<Transaction>;
type TxResponseOneshot = oneshot::Receiver<Transaction>;

pub(crate) async fn scan_node_live_queries(
	tx_req: mpsc::Sender<TxRequestOneshot>,
	nodes: Vec<Uuid>,
	sender: mpsc::Sender<BootstrapOperationResult>,
) -> Result<(), Error> {
	let (tx_req_oneshot, tx_res_oneshot): (TxRequestOneshot, TxResponseOneshot) =
		oneshot::channel();
	if let Err(_send_error) = tx_req.send(tx_req_oneshot).await {
		return Err(Error::BootstrapError(ChannelSendError(BootstrapTxSupplier)));
	}
	match tx_res_oneshot.await {
		Ok(mut tx) => {
			for nd in nodes {
				let node_lqs = tx.scan_ndlq(&nd, NO_LIMIT).await?;
				for lq in node_lqs {
					sender
						.send((lq, None))
						.await
						.map_err(|_| Error::BootstrapError(ChannelSendError(BootstrapScan)))?;
				}
			}
			tx.commit().await
		}
		Err(_recv_error) => {
			// TODO wrap
			Err(Error::BootstrapError(ChannelRecvError(BootstrapTxSupplier)))
		}
	}
}

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
				let results = archive_live_query_batch(tx_req.clone(), node_id, &mut msg).await?;
				for boresult in results {
					sender
						.send(boresult)
						.await
						.map_err(|_| Error::BootstrapError(ChannelSendError(BootstrapArchive)))?;
				}
				break;
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
		match tx_res_oneshot.await {
			Ok(mut tx) => {
				// In case this is a retry, we re-hydrate the msg vector
				for (lq, e) in ret.drain(..) {
					msg.push((lq, e));
				}
				// Fast-return
				if msg.len() <= 0 {
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
					continue;
				} else {
					break;
				}
				if last_err.is_some() {
					// If there are 2 conflicting bootstraps, we don't want them to continue
					// continue colliding at the same time. So we scatter the retry sleep
					let scatter_sleep = rand::thread_rng().gen_range(
						ds::BOOTSTRAP_TX_RETRY_LOW_MILLIS..ds::BOOTSTRAP_TX_RETRY_HIGH_MILLIS,
					);
					tokio::time::sleep(Duration::from_millis(scatter_sleep)).await;
				} else {
					// Successful transaction 🎉
					break;
				}
			}
			Err(_) => {}
		}
	}
	if let Some(e) = last_err {
		return Err(e);
	}
	Ok(ret)
}

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
		match tx_res_oneshot.await {
			Ok(mut tx) => {
				// In case this is a retry, we re-hydrate the msg vector
				for (lq, e) in ret.drain(..) {
					msg.push((lq, e));
				}
				// Consume the input message vector of live queries to archive
				if msg.len() > 0 {
					for (lq, _e) in msg.drain(..) {
						// Delete the node live query
						if let Err(e) = tx.del_ndlq(*(&lq).nd, *(&lq).lq, &lq.ns, &lq.db).await {
							// TODO wrap error with context that this step failed; requires self-ref error
							ret.push((lq, Some(e)));
							continue;
						}
						// Delete the table live query
						if let Err(e) = tx.del_tblq(&lq.ns, &lq.db, &lq.tb, *(&lq).lq).await {
							// TODO wrap error with context that this step failed; requires self-ref error
							ret.push((lq, Some(e)));
							continue;
						}
						// Delete the notifications
						// TODO hypothetical impl
						if let Err(e) = Ok(()) {
							// TODO wrap error with context that this step failed; requires self-ref error
							ret.push((lq, Some(e)));
						}
					}
					// TODO where can the above transaction hard fail? Every op needs rollback?
					if let Err(e) = tx.commit().await {
						// TODO wrap?
						last_err = Some(e);
						match tx.cancel().await {
							Ok(_) => {}
							Err(e) => {
								// TODO wrap?
								last_err = Some(e);
							}
						}
						continue;
					} else {
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
