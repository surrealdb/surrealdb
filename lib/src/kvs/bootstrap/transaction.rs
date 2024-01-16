use crate::kvs::LockType::Optimistic;
use crate::kvs::TransactionType::Write;
use crate::kvs::{Datastore, Transaction};
use tokio::sync::{mpsc, oneshot};

pub(crate) struct TxRequestHandler<'a> {
	pub(crate) ds: &'a Datastore,
	pub(crate) tx_req_recv: &'a mut mpsc::Receiver<oneshot::Sender<Transaction>>,
}

impl TxRequestHandler<'_> {
	pub async fn handle_tx_requests(&self) {
		loop {
			match self.tx_req_recv.recv().await {
				None => {
					// closed
					trace!("Transaction request channel closed, breaking out of bootstrap");
					break;
				}
				Some(sender) => {
					trace!("Received a transaction request");
					let tx = self.ds.transaction(Write, Optimistic).await.unwrap();
					if let Err(mut tx) = sender.send(tx) {
						// The receiver has been dropped, so we need to cancel the transaction
						trace!("Unable to send a transaction as response to task because the receiver is closed");
						tx.cancel().await.unwrap();
					}
				}
			};
		}
		trace!("Finished handling requests",);
	}
}
