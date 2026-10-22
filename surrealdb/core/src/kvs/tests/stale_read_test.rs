//! A backend that publishes a stale tier: `READ ONLY MAX STALENESS` is served
//! from its snapshots, and the fall-back policy decides what an unmet bound
//! does.

use std::any::{Any, TypeId};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::future::BoxFut;
use surrealdb_cnf::ConfigMap;
use surrealdb_kvs::TransactionType::Read;
use surrealdb_kvs::{
	Metrics, ReadOutcome, ReadSnapshotRequest, Result as KvsResult, SnapshotInfo, SnapshotPosition,
	StaleReads, StaleReadsHandle, Transactable, TransactionBuilder, TransactionType,
};
use tokio_util::sync::CancellationToken;

use crate::CommunityComposer;
use crate::catalog::providers::CatalogProvider;
use crate::dbs::Session;
use crate::kvs::{Datastore, ReadRequest, TransactionBuilderFactory, TransactionBuilderParts};

/// A stale tier whose newest snapshot has a settable age. Its snapshots are
/// read transactions over the wrapped backend.
struct FakeTier {
	inner: Arc<dyn TransactionBuilder>,
	newest_age: Mutex<Option<Duration>>,
	opened: Mutex<u32>,
}

impl StaleReads for FakeTier {
	fn min_staleness(&self) -> Duration {
		Duration::from_secs(1)
	}

	fn max_staleness(&self) -> Duration {
		Duration::from_secs(60)
	}

	fn auth_staleness(&self) -> Duration {
		Duration::from_secs(1)
	}

	fn open(&self, request: ReadSnapshotRequest) -> BoxFut<'_, KvsResult<ReadOutcome>> {
		Box::pin(async move {
			let newest = *self.newest_age.lock().unwrap();
			let Some(age) = newest.filter(|age| *age <= request.max_staleness) else {
				return Ok(ReadOutcome::BoundUnmet {
					newest_age: newest.unwrap_or(Duration::MAX),
				});
			};
			*self.opened.lock().unwrap() += 1;
			let (transaction, local) = self.inner.new_transaction(Read).await?;
			Ok(ReadOutcome::Opened {
				transaction,
				local,
				info: SnapshotInfo {
					position: SnapshotPosition::new(vec![7]),
					age,
					stale: true,
				},
			})
		})
	}
}

struct TieredBuilder {
	inner: Arc<dyn TransactionBuilder>,
	tier: Arc<FakeTier>,
}

impl TransactionBuilder for TieredBuilder {
	fn name(&self) -> &'static str {
		self.inner.name()
	}

	fn new_transaction(
		&self,
		write: TransactionType,
	) -> BoxFut<'_, KvsResult<(Box<dyn Transactable>, bool)>> {
		self.inner.new_transaction(write)
	}

	fn shutdown(&self) -> BoxFut<'_, KvsResult<()>> {
		self.inner.shutdown()
	}

	fn register_metrics(&self) -> Option<Metrics> {
		self.inner.register_metrics()
	}

	fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
		self.inner.collect_u64_metric(metric)
	}

	fn wait_until_serve_ready(&self) -> BoxFut<'_, KvsResult<()>> {
		self.inner.wait_until_serve_ready()
	}

	fn extension(&self, id: TypeId) -> Option<Arc<dyn Any + Send + Sync>> {
		match id == TypeId::of::<StaleReadsHandle>() {
			true => Some(Arc::new(StaleReadsHandle(Arc::clone(&self.tier) as Arc<dyn StaleReads>))
				as Arc<dyn Any + Send + Sync>),
			false => self.inner.extension(id),
		}
	}
}

struct TieredComposer(Arc<Mutex<Option<Arc<FakeTier>>>>);

impl TransactionBuilderFactory for TieredComposer {
	type RouterState = ();

	async fn new_transaction_builder(
		&self,
		path: &str,
		canceller: CancellationToken,
		config: surrealdb_cnf::ConfigMap,
	) -> anyhow::Result<TransactionBuilderParts<Self::RouterState>> {
		let parts = CommunityComposer().new_transaction_builder(path, canceller, config).await?;
		let inner: Arc<dyn TransactionBuilder> = Arc::from(parts.builder);
		let tier = Arc::new(FakeTier {
			inner: Arc::clone(&inner),
			newest_age: Mutex::new(None),
			opened: Mutex::new(0),
		});
		*self.0.lock().unwrap() = Some(Arc::clone(&tier));
		Ok(TransactionBuilderParts::without_router_state(Box::new(TieredBuilder {
			inner,
			tier,
		})))
	}

	fn path_valid(&self, v: &str) -> anyhow::Result<String> {
		CommunityComposer().path_valid(v)
	}
}

async fn tiered(fallback: Option<&str>) -> (Arc<Datastore>, Arc<FakeTier>) {
	let slot = Arc::new(Mutex::new(None));
	let mut config = ConfigMap::empty();
	if let Some(policy) = fallback {
		config = config.with_key_value("read_snapshot_fallback", policy);
	}
	let ds = Datastore::builder()
		.with_config(config)
		.build_with_factory_path("memory", TieredComposer(Arc::clone(&slot)))
		.await
		.unwrap();
	{
		let tx = ds.transaction(surrealdb_kvs::TransactionType::Write).await.unwrap();
		tx.ensure_ns_db(None, "test", "test").await.unwrap();
		tx.commit().await.unwrap();
	}
	let tier = slot.lock().unwrap().take().unwrap();
	(ds, tier)
}

fn request(secs: u64, strict: bool) -> ReadRequest {
	ReadRequest {
		max_staleness: Duration::from_secs(secs),
		strict,
	}
}

#[tokio::test]
async fn a_snapshot_within_the_bound_serves_the_read() {
	let (ds, tier) = tiered(None).await;
	*tier.newest_age.lock().unwrap() = Some(Duration::from_secs(3));
	let (tx, info) = ds.read_transaction(request(5, false)).await.unwrap();
	assert!(info.stale);
	// The snapshot may predate the settled catalog, so it never claims the
	// current catalog epoch.
	assert_eq!(tx.catalog_epoch(), None);
	assert_eq!(info.age, Duration::from_secs(3));
	assert_eq!(info.position, SnapshotPosition::new(vec![7]));
	assert_eq!(*tier.opened.lock().unwrap(), 1);
}

#[tokio::test]
async fn an_unmet_bound_reads_the_current_state_by_default() {
	let (ds, tier) = tiered(None).await;
	*tier.newest_age.lock().unwrap() = Some(Duration::from_secs(30));
	let (_tx, info) = ds.read_transaction(request(5, false)).await.unwrap();
	assert_eq!(info, SnapshotInfo::current());
	assert_eq!(*tier.opened.lock().unwrap(), 0);
}

#[tokio::test]
async fn strict_refuses_a_current_read() {
	let (ds, tier) = tiered(None).await;
	*tier.newest_age.lock().unwrap() = Some(Duration::from_secs(30));
	let err = ds.read_transaction(request(5, true)).await.err().expect("an error");
	assert!(err.to_string().contains("ReadSnapshotTooStale"), "{err}");
}

#[tokio::test]
async fn the_deny_policy_acts_as_strict_for_every_request() {
	let (ds, tier) = tiered(Some("deny")).await;
	*tier.newest_age.lock().unwrap() = Some(Duration::from_secs(30));
	let err = ds.read_transaction(request(5, false)).await.err().expect("an error");
	assert!(err.to_string().contains("ReadSnapshotTooStale"), "{err}");
	// A bound the tier meets is unaffected.
	let (_tx, info) = ds.read_transaction(request(60, false)).await.unwrap();
	assert!(info.stale);
}

#[tokio::test]
async fn the_rate_limited_policy_admits_fall_backs_up_to_its_rate() {
	let (ds, tier) = tiered(Some("rate:2")).await;
	*tier.newest_age.lock().unwrap() = Some(Duration::from_secs(30));
	let mut admitted = 0;
	for _ in 0..4 {
		if ds.read_transaction(request(5, false)).await.is_ok() {
			admitted += 1;
		}
	}
	// The window may roll over once while the loop runs.
	assert!((2..=4).contains(&admitted), "{admitted}");
	assert!(admitted < 4 || ds.read_transaction(request(5, false)).await.is_ok());
}

#[tokio::test]
async fn a_tier_with_no_snapshot_reports_the_bound_unmet() {
	let (ds, _tier) = tiered(Some("deny")).await;
	let err = ds.read_transaction(request(5, false)).await.err().expect("an error");
	assert!(err.to_string().contains("ReadSnapshotTooStale"), "{err}");
}

#[tokio::test]
async fn a_zero_bound_never_asks_the_tier() {
	let (ds, tier) = tiered(Some("deny")).await;
	*tier.newest_age.lock().unwrap() = Some(Duration::ZERO);
	let (_tx, info) = ds.read_transaction(request(0, true)).await.unwrap();
	assert_eq!(info, SnapshotInfo::current());
	assert_eq!(*tier.opened.lock().unwrap(), 0);
}

#[tokio::test]
async fn a_block_over_a_stale_snapshot_rejects_writes_and_names_a_refusal() {
	let (ds, tier) = tiered(Some("deny")).await;
	let ses = Session::owner().with_ns("test").with_db("test");
	*tier.newest_age.lock().unwrap() = Some(Duration::from_secs(2));
	let results = ds
		.execute("BEGIN READ ONLY MAX STALENESS 5s; CREATE item:one; COMMIT;", &ses, None)
		.await
		.unwrap();
	assert!(results.iter().any(|r| r.result.is_err()), "{results:?}");

	*tier.newest_age.lock().unwrap() = Some(Duration::from_secs(30));
	let results = ds
		.execute("BEGIN READ ONLY MAX STALENESS 5s; RETURN 1; COMMIT;", &ses, None)
		.await
		.unwrap();
	let message = results.iter().find_map(|r| r.result.as_ref().err()).unwrap().to_string();
	assert!(message.contains("ReadSnapshotTooStale"), "{message}");
}

#[tokio::test]
async fn viewless_builder_strips_views_from_stale_tier_transactions() {
	use surrealdb_kvs::snapshot::ViewCapabilities;
	use surrealdb_kvs::testing::ViewlessBuilder;

	let (ds, tier) = tiered(None).await;
	*tier.newest_age.lock().unwrap() = Some(Duration::from_secs(3));
	let (tx, _) = ds.read_transaction(request(5, false)).await.unwrap();
	assert!(tx.snapshot().is_some(), "unwrapped memory stale read exposes a snapshot");
	tx.cancel().await.unwrap();

	let wrapped = ViewlessBuilder(Box::new(TieredBuilder {
		inner: Arc::clone(&tier.inner),
		tier: Arc::clone(&tier),
	}));
	let handle = wrapped
		.extension(TypeId::of::<StaleReadsHandle>())
		.expect("stale tier extension")
		.downcast::<StaleReadsHandle>()
		.expect("StaleReadsHandle");
	let ReadOutcome::Opened {
		transaction,
		..
	} = handle
		.0
		.open(ReadSnapshotRequest {
			max_staleness: Duration::from_secs(5),
		})
		.await
		.unwrap()
	else {
		panic!("expected ReadOutcome::Opened");
	};
	assert!(transaction.snapshot().is_none());
	assert_eq!(transaction.view_capabilities(), ViewCapabilities::default());
	transaction.cancel().await.unwrap();
}
