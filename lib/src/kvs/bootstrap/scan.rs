use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::err::BootstrapCause::{ChannelRecvError, ChannelSendError};
use crate::err::ChannelVariant::{BootstrapScan, BootstrapTxSupplier};
use crate::err::Error;
use crate::kvs::bootstrap::{TxRequestOneshot, TxResponseOneshot};
use crate::kvs::Limit::Unlimited;
use crate::kvs::{BootstrapOperationResult, LqValue, ScanPage};
use crate::sql::Uuid;

/// Scans the live queries belonging to the provided nodes
/// All the live queries on the provided nodes will be removed safely (archived, deleted)
/// using the other tasks
pub(crate) async fn scan_node_live_queries(
	tx_req: mpsc::Sender<TxRequestOneshot>,
	nodes: Vec<Uuid>,
	sender: mpsc::Sender<BootstrapOperationResult>,
	batch_size: u32,
) -> Result<(), Error> {
	let (tx_req_oneshot, tx_res_oneshot): (TxRequestOneshot, TxResponseOneshot) =
		oneshot::channel();
	if let Err(_send_error) = tx_req.send(tx_req_oneshot).await {
		return Err(Error::BootstrapError(ChannelSendError(BootstrapTxSupplier)));
	}
	trace!("Receiving a tx response in scan");
	match tx_res_oneshot.await {
		Ok(mut tx) => {
			trace!("Received tx in scan - {:?} nodes", nodes);
			for nd in nodes {
				let beg = crate::key::node::lq::prefix_nd(&nd.0);
				let end = crate::key::node::lq::suffix_nd(&nd.0);
				let mut next_page = Some(ScanPage {
					range: beg..end,
					limit: Unlimited,
				});
				while let Some(page) = next_page {
					match tx.scan_paged(page, batch_size).await {
						Ok(scan_result) => {
							next_page = scan_result.next_page;
							for (key, value) in scan_result.values {
								let lv = crate::key::node::lq::Lq::decode(key.as_slice())?;
								let tb: String = String::from_utf8(value).unwrap();
								let lq = LqValue {
									nd: lv.nd.into(),
									ns: lv.ns.to_string(),
									db: lv.db.to_string(),
									tb,
									lq: lv.lq.into(),
								};
								sender.send((lq, None)).await.map_err(|e| {
									error!("Failed to send message: {}", e);
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
			}
			tx.commit().await
		}
		Err(recv_error) => {
			error!("Failed receiving tx in scan node live queries: {:?}", recv_error);
			Err(Error::BootstrapError(ChannelRecvError(BootstrapTxSupplier)))
		}
	}
}

#[cfg(test)]
#[cfg(feature = "kv-mem")]
mod test {
	use crate::dbs::Session;
	use crate::kvs::bootstrap::scan_node_live_queries;
	use crate::kvs::bootstrap::test_util::{always_give_tx, as_uuid};
	use crate::kvs::{BootstrapOperationResult, Datastore};
	use futures_concurrency::future::FutureExt;
	use std::sync::Arc;
	use std::time::Duration;
	use tokio::sync::mpsc;

	#[tokio::test]
	async fn scan_picks_up() {
		let ds = Arc::new(Datastore::new("memory").await.unwrap());
		let (tx_req, tx_res) = mpsc::channel(1);
		let tx_task = tokio::spawn(always_give_tx(ds.clone(), tx_res));

		// Create some nodes
		let sess = Session::owner().with_ns("namespaceTest").with_db("databaseTest").with_rt(true);
		let table = "testTable";
		let query = format!("LIVE SELECT * FROM {table}");
		let lq1 = as_uuid(ds.execute(&query, &sess, None).await.unwrap());
		let lq2 = as_uuid(ds.execute(&query, &sess, None).await.unwrap());

		let (output_lq_send, mut output_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);

		let scan_task =
			tokio::spawn(scan_node_live_queries(tx_req, vec![ds.id], output_lq_send, 1000));

		// We don't know the order of the live queries because the lq id is random
		let mut lq_map = map! {lq1 => true, lq2 => true};

		// Validate first live query
		let boot_result = output_lq_recv.recv().await;
		let boot_result = boot_result.unwrap();
		assert!(boot_result.1.is_none());
		assert!(lq_map.remove(&boot_result.0.lq).is_some());
		// Validate second live query
		let boot_result = output_lq_recv.recv().await;
		let boot_result = boot_result.unwrap();
		assert!(boot_result.1.is_none());
		assert!(lq_map.remove(&boot_result.0.lq).is_some());
		// Validate no more live queries
		assert!(output_lq_recv.recv().await.is_none());

		let (tx_task_res, scan_task_res) =
			tokio::time::timeout(Duration::from_millis(1000), tx_task.join(scan_task))
				.await
				.unwrap();
		let tx_req_count = tx_task_res.unwrap().unwrap();
		assert_eq!(tx_req_count, 1);
		scan_task_res.unwrap().unwrap();
	}

	#[tokio::test]
	async fn scan_dies_no_messages() {
		let ds = Arc::new(Datastore::new("memory").await.unwrap());
		let (tx_req, tx_res) = mpsc::channel(1);
		let tx_task = tokio::spawn(always_give_tx(ds.clone(), tx_res));

		// Create some data
		let sess = Session::owner().with_ns("namespaceTest").with_db("databaseTest").with_rt(true);
		let table = "testTable";
		let query = format!("CREATE {table}");
		let _create_result = ds.execute(&query, &sess, None).await.unwrap();

		let (output_lq_send, mut output_lq_recv): (
			mpsc::Sender<BootstrapOperationResult>,
			mpsc::Receiver<BootstrapOperationResult>,
		) = mpsc::channel(10);

		let scan_task =
			tokio::spawn(scan_node_live_queries(tx_req, vec![ds.id], output_lq_send, 1000));

		// Wait for task to finish as it has nothing to do
		let (tx_task_res, scan_task_res) =
			tokio::time::timeout(Duration::from_millis(1000), tx_task.join(scan_task))
				.await
				.unwrap();
		let tx_req_count = tx_task_res.unwrap().unwrap();
		assert_eq!(tx_req_count, 1);
		scan_task_res.unwrap().unwrap();

		// Validate channel is empty and closed
		assert!(output_lq_recv.try_recv().is_err());
	}
}
