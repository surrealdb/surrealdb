use crate::kvs::{Datastore, LockType, TransactionType};

enum StorageVersion {
	V0,
	V1,
}

const latest_version: StorageVersion = StorageVersion::V1;

impl StorageVersion {
	fn from_version(version: Option<u32>) -> Self {
		match version {
			None | Some(0) => Ok(StorageVersion::V0),
			Some(1) => Ok(StorageVersion::V1),
			Some(v) => panic!("Found unsupported storage version {v}"),
		}
	}

	fn to_version(&self) -> u32 {
		match self {
			StorageVersion::V0 => 0,
			StorageVersion::V1 => 1,
		}
	}

	async fn from_ds(ds: Datastore) -> Self {
		let tx = ds.transaction(TransactionType::Read, LockType::Pessimistic).await.unwrap();
		let version_num = tx.get(crate::key::storage::version::new(), None);

		StorageVersion::V0
	}
}
