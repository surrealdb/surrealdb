use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::err::BootstrapCause::{ChannelRecvError, ChannelSendError};
use crate::err::ChannelVariant::{BootstrapScan, BootstrapTxSupplier};
use crate::err::Error;
use crate::kvs::bootstrap::{TxRequestOneshot, TxResponseOneshot};
use crate::kvs::{BootstrapOperationResult, NO_LIMIT};
use crate::sql::Uuid;

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
	trace!("Receiving a tx response in scan");
	match tx_res_oneshot.await {
		Ok(mut tx) => {
			trace!("Received tx in scan");
			for nd in nodes {
				match tx.scan_ndlq(&nd, NO_LIMIT).await {
					Ok(node_lqs) => {
						for lq in node_lqs {
							sender.send((lq, None)).await.map_err(|_| {
								Error::BootstrapError(ChannelSendError(BootstrapScan))
							})?;
						}
					}
					Err(e) => {
						error!("Failed scanning node live queries: {:?}", e);
						tx.cancel().await?;
						return Err(e);
					}
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

#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod test {
	use crate::dbs::Session;
	use crate::kvs::bootstrap::scan_node_live_queries;
	use crate::kvs::bootstrap::test_util::{always_give_tx, asUuid};
	use crate::kvs::{BootstrapOperationResult, Datastore};
	use futures_concurrency::future::FutureExt;
	use std::sync::Arc;
	use std::time::Duration;
	use tokio::sync::mpsc;

	#[tokio::test]
	async fn scan_picks_up() {
		let ds = Arc::new(Datastore::new("memory").await.unwrap());
		let (tx_req, tx_res) = mpsc::channel(1);
		let tx_task = tokio::spawn(always_give_tx(ds, tx_res));

		// Create some nodes
		let sess = Session::owner().with_ns("namespaceTest").with_db("databaseTest").with_rt(true);
		let table = "testTable";
		let query = format!("LIVE SELECT * FROM {table}");
		let node_id = ds.id;
		let lq1 = asUuid(ds.execute(&query, &sess, None).await.unwrap());
		let lq2 = asUuid(ds.execute(&query, &sess, None).await.unwrap());

		let (output_lq_send, mut output_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);

		let scan_task =
			tokio::spawn(scan_node_live_queries(tx_req, vec![lq1, lq2], output_lq_send));

		// Wait for output
		assert_eq!(output_lq_recv.recv().await, Some((lq1, None)));
		assert_eq!(output_lq_recv.recv().await, Some((lq2, None)));
		assert!(output_lq_recv.recv().await.is_none());

		let (tx_task_res, arch_task_res) =
			tokio::time::timeout(Duration::from_millis(1000), tx_task.join(scan_task))
				.await
				.unwrap();
		let tx_req_count = tx_task_res.unwrap().unwrap();
		assert_eq!(tx_req_count, 0);
		arch_task_res.unwrap().unwrap();
	}

	#[tokio::test]
	async fn scan_dies_no_messages() {}

	#[tokio::test]
	async fn scan_batches() {}

	#[tokio::test]
	async fn scan_failed_tx() {}

	#[tokio::test]
	async fn scan_invalid_input() {}
}
