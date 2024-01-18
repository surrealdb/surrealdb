use crate::idx::trees::hnsw::HnswIndex;
use crate::kvs::Key;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) struct HnswIndexes(Arc<RwLock<HashMap<Key, Arc<RwLock<HnswIndex>>>>>);

impl Default for HnswIndexes {
	fn default() -> Self {
		Self(Arc::new(RwLock::new(HashMap::new())))
	}
}

impl HnswIndexes {}
