use crate::kvs;
use crate::sync::Mutex;
use std::sync::Arc;
use tracing_mutex::stdsync::Mutex as TracingMutex;

pub(crate) type Transaction = Arc<TracingMutex<kvs::Transaction>>;
