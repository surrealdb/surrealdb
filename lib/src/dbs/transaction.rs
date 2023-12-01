use crate::kvs;
use crate::sync::Mutex;
use std::sync::Arc;

pub(crate) type Transaction = Arc<Mutex<kvs::Transaction>>;
