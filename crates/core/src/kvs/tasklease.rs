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
	pub(super) async fn has_lease(
		&self,
		node: &Uuid,
		tf: &TransactionFactory,
		lt: &Duration,
	) -> Result<bool> {
		let key: Key = Tl::new(self).encode()?;
		// Use for exponential backoff
		let mut tempo = 4;
		const MAX_BACKOFF: u64 = 32_768;
		// Loop until we have a successful allocation.
		// We check the timeout inherited from the context
		while tempo < MAX_BACKOFF {
			if let Ok(r) = self.check_lease(node, tf, &key, lt).await {
				return Ok(r);
			}
			// exponential backoff with full jitter
			let sleep_ms = thread_rng().gen_range(1..=tempo);
			sleep(Duration::from_millis(sleep_ms)).await;
			tempo *= 2;
		}
		bail!(Error::QueryTimedout)
	}

	async fn check_lease(
		&self,
		node: &Uuid,
		tf: &TransactionFactory,
		key: &Key,
		lt: &Duration,
	) -> Result<bool> {
		// Is there already a non-expired lease?
		{
			let tx = tf.transaction(TransactionType::Read, LockType::Optimistic).await?;
			if let Some(l) = tx.get(key, None).await? {
				let l: TaskLease = revision::from_slice(&l)?;
				// Did the lease expire?
				if l.expiration > Utc::now() {
					return Ok(l.owner.eq(node));
				}
			}
		}
		// If not, we try to write a new lease
		let tx = tf.transaction(TransactionType::Write, LockType::Optimistic).await?;
		let lease = TaskLease {
			owner: *node,
			expiration: Utc::now() + *lt,
		};
		tx.set(key, revision::to_vec(&lease)?, None).await?;
		tx.commit().await?;
		Ok(true)
	}
}

#[cfg(test)]
mod tests {
	use crate::dbs::node::Timestamp;
	use crate::kvs::clock::{FakeClock, SizedClock};
	use crate::kvs::ds::{DatastoreFlavor, TransactionFactory};
	use crate::kvs::mem;
	use crate::kvs::tasklease::TaskLeaseType;
	use std::sync::Arc;
	use std::time::{Duration, Instant};
	use uuid::Uuid;

	#[derive(Default)]
	struct NodeResult {
		ok_true: usize,
		ok_false: usize,
		err: usize,
	}

	#[cfg(feature = "kv-mem")]
	async fn node_task_lease(
		id: Uuid,
		tf: Arc<TransactionFactory>,
		test_duration: Duration,
		lease_duration: Duration,
	) -> NodeResult {
		let mut result = NodeResult::default();
		let start_time = Instant::now();
		while start_time.elapsed() < test_duration {
			match TaskLeaseType::ChangeFeedCleanup.has_lease(&id, &tf, &lease_duration).await {
				Ok(true) => {
					result.ok_true += 1;
				}
				Ok(false) => {
					result.ok_false += 1;
				}
				Err(_e) => {
					result.err += 1;
				}
			}
		}
		result
	}

	async fn task_lease_concurrency<F: Fn(NodeResult, NodeResult, NodeResult)>(
		flavor: DatastoreFlavor,
		check: F,
	) {
		let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
		let tf = Arc::new(TransactionFactory::new(clock, flavor));
		let test_duration = Duration::from_secs(3);
		let lease_duration = Duration::from_secs(1);
		let new_node =
			|n| node_task_lease(Uuid::from_u128(n), tf.clone(), test_duration, lease_duration);
		let node1 = tokio::spawn(new_node(0));
		let node2 = tokio::spawn(new_node(1));
		let node3 = tokio::spawn(new_node(2));
		let (res1, res2, res3) = tokio::try_join!(node1, node2, node3).expect("Tasks failed");
		check(res1, res2, res3);
	}

	#[cfg(feature = "kv-mem")]
	#[tokio::test(flavor = "multi_thread")]
	async fn task_lease_concurrency_memory() {
		let flavor = mem::Datastore::new().await.map(DatastoreFlavor::Mem).unwrap();
		let check = |res1: NodeResult, res2: NodeResult, res3: NodeResult| {
			assert!(res1.ok_true + res2.ok_true + res3.ok_true > 0);
			assert!(res1.ok_false + res2.ok_false + res3.ok_false > 0);
			assert_eq!(res1.err + res2.err + res3.err, 0);
		};
		task_lease_concurrency(flavor, check).await;
	}
}
