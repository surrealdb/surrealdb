use crate::err::Error;
use crate::key::root::tl::Tl;
use crate::kvs::ds::TransactionFactory;
use crate::kvs::{Key, KeyEncode, LockType, TransactionType};
use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use rand::{Rng, thread_rng};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

pub(crate) enum TaskLeaseType {
	ChangeFeedCleanup,
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[non_exhaustive]
struct TaskLease {
	owner: Uuid,
	expiration: DateTime<Utc>,
}
impl TaskLeaseType {
	pub(super) async fn has_lease(&self, node: &Uuid, tf: &TransactionFactory) -> Result<bool> {
		let key: Key = Tl::new(self).encode()?;
		// Use for exponential backoff
		let mut tempo = 4;
		const MAX_BACKOFF: u64 = 32_768;
		// Loop until we have a successful allocation.
		// We check the timeout inherited from the context
		while tempo < MAX_BACKOFF {
			if let Ok(r) = Self::check_lease(node, tf, &key).await {
				return Ok(r);
			}
			// exponential backoff with full jitter
			let sleep_ms = thread_rng().gen_range(1..=tempo);
			sleep(Duration::from_millis(sleep_ms)).await;
			tempo *= 2;
		}
		bail!(Error::QueryTimedout)
	}

	async fn check_lease(node: &Uuid, tf: &TransactionFactory, key: &Key) -> Result<bool> {
		// Is there already a non-expired lease?
		{
			let tx = tf.transaction(TransactionType::Read, LockType::Optimistic).await?;
			if let Some(l) = tx.get(key, None).await? {
				let l: TaskLease = revision::from_slice(&l)?;
				// Did the lease expire?
				if l.expiration < Utc::now() {
					// Return true if we are the owner
					return Ok(l.owner.eq(node));
				}
			}
		}
		// If not, we try to write a new lease
		let tx = tf.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let lease = TaskLease {
			owner: *node,
			expiration: Utc::now() + Duration::from_secs(300),
		};
		tx.set(key, revision::to_vec(&lease)?, None).await?;
		Ok(true)
	}
}
