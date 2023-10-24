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
