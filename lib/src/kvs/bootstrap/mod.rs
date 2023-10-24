use rand::Rng;
use tokio::sync::oneshot;

use crate::kvs::Transaction;
pub(crate) use archive::archive_live_queries;
pub(crate) use delete::delete_live_queries;
pub(crate) use scan::scan_node_live_queries;

mod archive;
mod delete;
mod scan;

type TxRequestOneshot = oneshot::Sender<Transaction>;
type TxResponseOneshot = oneshot::Receiver<Transaction>;
