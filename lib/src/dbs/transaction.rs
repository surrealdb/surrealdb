use crate::kvs;
use futures::lock::Mutex;
use std::sync::Arc;

pub(crate) type Transaction = Arc<Mutex<kvs::Transaction>>;
