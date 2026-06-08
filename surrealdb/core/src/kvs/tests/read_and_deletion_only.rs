//! Tests for RocksDB SST file manager feature
//!
//! This module tests the SST file manager space monitoring feature that:
//! - Limits disk space usage for SST files via the `SURREAL_ROCKSDB_SST_MAX_ALLOWED_SPACE_USAGE`
//!   environment variable
//! - Transitions to read-and-deletion-only mode when the space limit is reached
//! - Allows read and delete operations during read-and-deletion-only mode (but blocks writes)
//! - Automatically recovers to normal mode when space drops below the limit after deletions and
//!   compaction

use crate::kvs::LockType::Optimistic;
use crate::kvs::TransactionType::*;
use crate::kvs::tests::CreateDs;
use crate::val::Uuid;

macro_rules! define_tests {
	($new_ds:ident) => {
		#[tokio::test]
		async fn read_and_deletion_only() {
			super::read_and_deletion_only::read_and_deletion_only($new_ds).await;
		}
	};
}
pub(crate) use define_tests;
