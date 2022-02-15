use futures::lock::Mutex;
use std::sync::Arc;

pub type Transaction<'a> = Arc<Mutex<crate::kvs::Transaction<'a>>>;
