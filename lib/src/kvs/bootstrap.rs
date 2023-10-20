use crate::err::BootstrapCause::ChannelSendError;
use crate::err::ChannelVariant::{BootstrapArchive, BootstrapDelete, BootstrapScan};
use crate::err::Error;
use crate::kvs::LockType::Optimistic;
use crate::kvs::TransactionType::{Read, Write};
use crate::kvs::{ds, BootstrapOperationResult, Datastore, NO_LIMIT};
use crate::sql::Uuid;
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};

pub(crate) async fn scan_node_live_queries(
	ds: Arc<Datastore>,
	nodes: Vec<Uuid>,
	sender: Sender<BootstrapOperationResult>,
) -> Result<(), Error> {
	let mut tx = ds.transaction(Read, Optimistic).await?;
	for nd in nodes {
		let node_lqs = tx.scan_ndlq(&nd, NO_LIMIT).await?;
		for lq in node_lqs {
			sender
				.send((lq, None))
				.await
				.map_err(|e| Error::BootstrapError(ChannelSendError(BootstrapScan)))?;
		}
	}
	tx.cancel().await
}

/// This task will read input live queries from a receiver in batches and
/// archive them and finally send them to the output channel.
/// The task terminates if there is an irrecoverable error or if the input
/// channel has been closed (dropped, from previous task).
pub(crate) async fn archive_live_queries(
	ds: Arc<Datastore>,
	mut scan_recv: Receiver<BootstrapOperationResult>,
	sender: Sender<BootstrapOperationResult>,
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
						let results = archive_live_query_batch(ds.clone(), &mut msg).await?;
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
				let results = archive_live_query_batch(ds.clone(), &mut msg).await?;
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
				let results = archive_live_query_batch(ds.clone(), &mut msg).await?;
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
	ds: Arc<Datastore>,
	msg: &mut Vec<BootstrapOperationResult>,
) -> Result<Vec<BootstrapOperationResult>, Error> {
	let mut ret: Vec<BootstrapOperationResult> = vec![];
	// TODO test failed tx retries
	let mut last_err = None;
	for _ in 0..ds::BOOTSTRAP_TX_RETRIES {
		match ds.transaction(Write, Optimistic).await {
			// TODO
			Ok(mut tx) => {
				// In case this is a retry, we re-hydrate the msg vector
				for (lq, e) in ret.drain(..) {
					msg.push((lq, e));
				}
				// Consume the input message vector of live queries to archive
				if msg.len() > 0 {
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
							let archived_lvs = lv.clone().archive(ds.id);
							match tx.putc_tblq(&lq.ns, &lq.db, &lq.tb, archived_lvs, Some(lv)).await
							{
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
					tx.commit().await?;
				}
			}
			Err(e) => {
				last_err = Some(e);
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

/// Given a receiver channel of archived live queries,
/// Delete the node lq, table lq, and notifications
/// and send the results to the sender channel
pub(crate) async fn delete_live_queries(
	ds: Arc<Datastore>,
	mut archived_recv: Receiver<BootstrapOperationResult>,
	sender: Sender<BootstrapOperationResult>,
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
						let results = delete_live_query_batch(ds.clone(), &mut msg).await?;
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
				let results = delete_live_query_batch(ds.clone(), &mut msg).await?;
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
				let results = delete_live_query_batch(ds.clone(), &mut msg).await?;
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
	ds: Arc<Datastore>,
	msg: &mut Vec<BootstrapOperationResult>,
) -> Result<Vec<BootstrapOperationResult>, Error> {
	let mut ret: Vec<BootstrapOperationResult> = vec![];
	// TODO test failed tx retries
	let mut last_err = None;
	for _ in 0..ds::BOOTSTRAP_TX_RETRIES {
		match ds.transaction(Write, Optimistic).await {
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
						continue;
					} else {
						break;
					}
				}
			}
			Err(e) => {
				last_err = Some(e);
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
