use crate::kvs;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) type Transaction = Arc<Mutex<kvs::Transaction>>;
