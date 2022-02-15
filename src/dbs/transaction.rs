use crate::kvs;
use futures::lock::Mutex;
use std::sync::Arc;

pub type Transaction = Arc<Mutex<kvs::Transaction>>;
