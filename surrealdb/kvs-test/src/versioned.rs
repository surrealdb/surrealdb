//! Versioned (MVCC / time-travel) read handling.
//!
//! No first-party backend exposes versioned reads through this suite anymore:
//! the memory backend dropped versioning in the surrealmx 0.23 upgrade, and the
//! other backends are registered here without it. So the behaviour exercised is
//! that every backend rejects a read carrying a version. (Backend-native
//! versioned reads for rocksdb/surrealkv are covered at the SDK and language
//! layers.)

use surrealdb_kvs::TransactionType::*;
use surrealdb_kvs::{Error, KeyRange};

use crate::{TestBackend, kvs_test};

/// Backends without versioning support must reject every read that passes
/// `version: Some(_)` with [`Error::UnsupportedVersionedQueries`].
async fn unsupported_error(b: &TestBackend) {
	let ds = b.create_ds().await;
	let tx = ds.transaction(Write).await.unwrap();
	tx.set(b"test".into(), b"value".to_vec()).await.unwrap();
	tx.commit().await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	let rng = KeyRange::from(&b"a"[..]..&b"z"[..]);
	assert!(matches!(
		tx.get(b"test".into(), Some(1)).await,
		Err(Error::UnsupportedVersionedQueries)
	));
	assert!(matches!(
		tx.exists(b"test".into(), Some(1)).await,
		Err(Error::UnsupportedVersionedQueries)
	));
	assert!(matches!(
		tx.keys(rng.as_borrowed(), 10, 0, Some(1)).await,
		Err(Error::UnsupportedVersionedQueries)
	));
	assert!(matches!(
		tx.scan(rng.as_borrowed(), 10, 0, Some(1)).await,
		Err(Error::UnsupportedVersionedQueries)
	));
	assert!(matches!(tx.count(rng, Some(1)).await, Err(Error::UnsupportedVersionedQueries)));
	tx.cancel().await.unwrap();
}

kvs_test!(unsupported_error);
