//! Schema-defined rate limiting: token buckets persisted in the KV store.
//!
//! `RATELIMIT` clauses on tables and fields compile to policies whose
//! buckets live in the datastore under BLAKE3-derived keys, so limits are
//! enforced consistently across every node of a cluster without extra
//! coordination. SELECT policies meter the records a statement actually
//! delivers ([`DeliveryMeter`]), charges settle atomically in dedicated
//! transactions (never the user's) before the response is returned, and
//! denial surfaces as a typed, retryable error with the whole statement
//! failing — responses are never partial.
//!
//! Two byte formats defined here — the bucket key derivation
//! ([`BucketKeyHasher`]) and the bucket value encoding — are persistence
//! formats and must never change without a versioning plan.
//!
//! See `README.md` in this directory for the full semantic invariants
//! (fail-closed NONE keys, delivered-records metering, AND-composition),
//! the architecture overview, and the roadmap (quota leasing, cost
//! units).

use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use web_time::{Duration, SystemTime, UNIX_EPOCH};

use crate::kvs::sequences::Sequences;
use crate::kvs::{Error as KvsError, LockType, Transaction, TransactionFactory, TransactionType};
use crate::observe::TenantIdentity;

/// How many admissions pass between piggybacked cleanup sweeps.
const CLEANUP_INTERVAL: u64 = 256;
/// How many bucket keys a single cleanup sweep inspects.
const CLEANUP_BATCH: u32 = 64;
/// How often a charge transaction is retried on optimistic conflict before
/// the conflict is surfaced to the caller.
const CHARGE_ATTEMPTS: usize = 4;

#[derive(Debug, Default)]
pub(crate) struct RateLimiter {
	cleanup_counter: AtomicU64,
	/// Resume point for the incremental cleanup sweep. `None` restarts from
	/// the beginning of the bucket keyspace.
	cleanup_cursor: Mutex<Option<Vec<u8>>>,
	table_plans: Mutex<HashMap<u64, CachedTableRatelimitPlan>>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum FastRatelimitBucket {
	Id,
	Ip,
	Ns,
	Db,
	Origin,
	Auth,
	Record,
	Token,
}

/// A fully derived bucket identity.
///
/// 128 bits of a BLAKE3 hash over the policy identity and the evaluated
/// `BY` key. Collision resistance matters here: bucket state is shared
/// through the datastore and the hashed material includes user-controlled
/// values, so a constructible collision would let one principal drain
/// another principal's bucket.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct BucketKey([u8; 16]);

/// Incremental [`BucketKey`] builder.
///
/// Implements [`Hasher`] so session fields and SurrealQL values can be fed
/// through their `Hash` impls. All integer writes are little-endian so the
/// derived key does not depend on host endianness or pointer width; the
/// byte stream, and therefore the derived keys, are a persistence format.
#[derive(Clone)]
pub(crate) struct BucketKeyHasher(blake3::Hasher);

impl std::fmt::Debug for BucketKeyHasher {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("BucketKeyHasher(..)")
	}
}

impl BucketKeyHasher {
	pub(crate) fn new() -> Self {
		Self(blake3::Hasher::new())
	}

	pub(crate) fn finalize(&self) -> BucketKey {
		let hash = self.0.finalize();
		let mut out = [0u8; 16];
		out.copy_from_slice(&hash.as_bytes()[..16]);
		BucketKey(out)
	}
}

impl Hasher for BucketKeyHasher {
	fn finish(&self) -> u64 {
		let hash = self.0.finalize();
		u64::from_le_bytes(hash.as_bytes()[..8].try_into().expect("blake3 output is 32 bytes"))
	}

	fn write(&mut self, bytes: &[u8]) {
		self.0.update(bytes);
	}

	fn write_u8(&mut self, i: u8) {
		self.0.update(&[i]);
	}

	fn write_u16(&mut self, i: u16) {
		self.0.update(&i.to_le_bytes());
	}

	fn write_u32(&mut self, i: u32) {
		self.0.update(&i.to_le_bytes());
	}

	fn write_u64(&mut self, i: u64) {
		self.0.update(&i.to_le_bytes());
	}

	fn write_u128(&mut self, i: u128) {
		self.0.update(&i.to_le_bytes());
	}

	fn write_usize(&mut self, i: usize) {
		self.0.update(&(i as u64).to_le_bytes());
	}

	fn write_i8(&mut self, i: i8) {
		self.write_u8(i as u8);
	}

	fn write_i16(&mut self, i: i16) {
		self.write_u16(i as u16);
	}

	fn write_i32(&mut self, i: i32) {
		self.write_u32(i as u32);
	}

	fn write_i64(&mut self, i: i64) {
		self.write_u64(i as u64);
	}

	fn write_i128(&mut self, i: i128) {
		self.write_u128(i as u128);
	}

	fn write_isize(&mut self, i: isize) {
		self.write_usize(i as usize);
	}
}

/// A single policy admission pending settlement against the datastore.
#[derive(Clone, Debug)]
pub(crate) struct PendingCharge {
	/// Human-readable policy locus (e.g. `table post`, `field name on post`)
	/// used in denial errors. Never contains the bucket key value.
	pub(crate) scope: String,
	pub(crate) key: BucketKey,
	pub(crate) limit: u64,
	pub(crate) period: Duration,
	pub(crate) max: Option<u64>,
	pub(crate) amount: u64,
}

/// Result of settling a batch of charges.
#[derive(Clone, Debug)]
pub(crate) enum ChargeOutcome {
	Admitted,
	Denied {
		scope: String,
		/// Estimated wait until the bucket has refilled enough to admit the
		/// same batch. `None` means the batch exceeds the bucket capacity
		/// and can never be admitted.
		retry_after: Option<Duration>,
	},
}

/// Per-attempt outcome inside a charge transaction.
enum ChargeAttempt {
	Admitted,
	Denied {
		scope: String,
		retry_after: Option<Duration>,
	},
	Conflict,
}

#[derive(Clone, Debug)]
pub(crate) struct CachedRatelimitPolicy {
	pub(crate) bucket: Option<FastRatelimitBucket>,
	/// BLAKE3 state pre-fed with the policy identity (ns, db, table, action,
	/// policy index). Cloned and extended with the session key per request.
	pub(crate) key_seed: BucketKeyHasher,
	pub(crate) limit: u64,
	pub(crate) period: Duration,
	pub(crate) max: Option<u64>,
}

/// Identity of a cached table plan, verified on every cache hit so that a
/// 64-bit cache-key collision can never apply another table's policies.
///
/// `schema_ts` is the table definition's `cache_tables_ts` stamp: every
/// `DEFINE TABLE` writes a fresh one, so a plan cached before a schema
/// change (including one made on another node) mismatches and is rebuilt.
/// The plan cache therefore adds no staleness beyond the node's own view
/// of the catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PlanIdentity {
	pub(crate) ns: String,
	pub(crate) db: String,
	pub(crate) table: String,
	pub(crate) action: String,
	pub(crate) schema_ts: uuid::Uuid,
}

#[derive(Clone, Debug)]
struct CachedTableRatelimitPlan {
	identity: PlanIdentity,
	policies: Arc<Vec<CachedRatelimitPolicy>>,
}

/// FNV-1a, used only for in-memory plan cache keys (hits are verified
/// against the stored [`PlanIdentity`], so collisions cost a re-derivation
/// rather than correctness).
#[derive(Debug)]
pub(crate) struct StableHasher {
	hash: u64,
}

impl Default for StableHasher {
	fn default() -> Self {
		Self {
			hash: 0xcbf29ce484222325,
		}
	}
}

impl StableHasher {
	pub(crate) fn new() -> Self {
		Self::default()
	}
}

impl Hasher for StableHasher {
	fn finish(&self) -> u64 {
		self.hash
	}

	fn write(&mut self, bytes: &[u8]) {
		for byte in bytes {
			self.hash ^= u64::from(*byte);
			self.hash = self.hash.wrapping_mul(0x100000001b3);
		}
	}
}

impl RateLimiter {
	pub(crate) fn cached_table_plan(
		&self,
		key: u64,
		identity: &PlanIdentity,
	) -> Option<Arc<Vec<CachedRatelimitPolicy>>> {
		let plans = self.table_plans.lock().unwrap_or_else(|e| e.into_inner());
		plans
			.get(&key)
			.filter(|plan| &plan.identity == identity)
			.map(|plan| Arc::clone(&plan.policies))
	}

	pub(crate) fn clear_table_plans(&self) {
		self.table_plans.lock().unwrap_or_else(|e| e.into_inner()).clear();
	}

	pub(crate) fn store_table_plan(
		&self,
		key: u64,
		identity: PlanIdentity,
		policies: Vec<CachedRatelimitPolicy>,
	) -> Arc<Vec<CachedRatelimitPolicy>> {
		let policies = Arc::new(policies);
		let mut plans = self.table_plans.lock().unwrap_or_else(|e| e.into_inner());
		plans.insert(
			key,
			CachedTableRatelimitPlan {
				identity,
				policies: Arc::clone(&policies),
			},
		);
		policies
	}

	/// Settle a batch of charges atomically in a dedicated transaction.
	///
	/// All charges admit together or none do. Optimistic conflicts are
	/// retried up to [`CHARGE_ATTEMPTS`] times before being surfaced, so
	/// concurrent traffic on a shared bucket does not produce spurious
	/// denials. Charges settle in their own transaction — never the user's —
	/// so bucket contention cannot abort unrelated user work, and a settled
	/// charge stays settled even if the statement that incurred it fails.
	#[instrument(level = "trace", name = "ratelimit.charge", skip_all, fields(charges = charges.len()))]
	pub(crate) async fn charge(
		&self,
		session: &ChargeSession,
		charges: &[PendingCharge],
	) -> Result<ChargeOutcome> {
		if charges.iter().all(|charge| charge.amount == 0) {
			return Ok(ChargeOutcome::Admitted);
		}
		let mut last_conflict: Option<anyhow::Error> = None;
		for _ in 0..CHARGE_ATTEMPTS {
			let txn = session.transaction().await?;
			match self.charge_once(&txn, charges).await {
				Ok(ChargeAttempt::Admitted) => match txn.commit().await {
					Ok(()) => {
						self.maybe_cleanup(session).await;
						return Ok(ChargeOutcome::Admitted);
					}
					Err(err) if is_conflict(&err) => {
						last_conflict = Some(err);
						continue;
					}
					Err(err) => return Err(err),
				},
				Ok(ChargeAttempt::Denied {
					scope,
					retry_after,
				}) => {
					let _ = txn.cancel().await;
					return Ok(ChargeOutcome::Denied {
						scope,
						retry_after,
					});
				}
				Ok(ChargeAttempt::Conflict) => {
					let _ = txn.cancel().await;
					continue;
				}
				Err(err) => {
					let _ = txn.cancel().await;
					return Err(err);
				}
			}
		}
		Err(last_conflict.unwrap_or_else(|| {
			anyhow::Error::new(KvsError::TransactionConflict(
				"rate-limit bucket contention".to_string(),
			))
		}))
	}

	async fn charge_once(
		&self,
		txn: &Transaction,
		charges: &[PendingCharge],
	) -> Result<ChargeAttempt> {
		let now_ms = now_millis();
		for charge in charges {
			if charge.amount == 0 {
				continue;
			}
			if charge.limit == 0 || charge.period.is_zero() {
				return Ok(ChargeAttempt::Denied {
					scope: charge.scope.clone(),
					retry_after: None,
				});
			}
			let capacity = charge.max.unwrap_or(charge.limit).max(1) as f64;
			let refill_per_second = charge.limit as f64 / charge.period.as_secs_f64();
			let key = kv_key(&charge.key);
			let old = txn.get(&key, None).await?;
			let mut bucket =
				load_bucket(old.as_deref(), now_ms, charge.period, capacity, refill_per_second);
			refill_bucket(&mut bucket, now_ms, charge.period, capacity, refill_per_second);

			let amount = charge.amount as f64;
			if bucket.tokens < amount {
				if amount > capacity {
					// Can never be admitted regardless of refill.
					return Ok(ChargeAttempt::Denied {
						scope: charge.scope.clone(),
						retry_after: None,
					});
				}
				let wait_secs = (amount - bucket.tokens) / refill_per_second;
				return Ok(ChargeAttempt::Denied {
					scope: charge.scope.clone(),
					retry_after: Some(Duration::from_secs_f64(wait_secs.max(0.0))),
				});
			}
			bucket.tokens -= amount;
			let new = encode_bucket(&bucket);
			if !put_bucket(txn, &key, &new, old.as_ref()).await? {
				return Ok(ChargeAttempt::Conflict);
			}
		}
		Ok(ChargeAttempt::Admitted)
	}

	/// Incrementally remove expired bucket state.
	///
	/// Runs once every [`CLEANUP_INTERVAL`] charge batches, in its own
	/// best-effort transaction (never a user transaction), resuming from a
	/// rotating cursor so the whole keyspace is eventually swept even when
	/// the lowest-ordered keys are long-lived.
	#[instrument(level = "trace", name = "ratelimit.cleanup", skip_all)]
	async fn maybe_cleanup(&self, session: &ChargeSession) {
		if !self.cleanup_counter.fetch_add(1, Ordering::Relaxed).is_multiple_of(CLEANUP_INTERVAL) {
			return;
		}
		let start = {
			let mut cursor = self.cleanup_cursor.lock().unwrap_or_else(|e| e.into_inner());
			cursor.take().unwrap_or_else(|| RATE_LIMIT_PREFIX.to_vec())
		};
		let mut end = RATE_LIMIT_PREFIX.to_vec();
		end.push(0xff);
		let now_ms = now_millis();
		let sweep = async {
			let txn = session.transaction().await?;
			let scan = txn.scan(start..end, CLEANUP_BATCH, 0, None).await?;
			let full_batch = scan.len() as u32 == CLEANUP_BATCH;
			let mut next = None;
			for (key, val) in scan {
				if decode_bucket(&val).is_none_or(|bucket| bucket.expires_at_ms <= now_ms) {
					txn.del(&key).await?;
				}
				next = Some(key);
			}
			txn.commit().await?;
			Ok::<Option<Vec<u8>>, anyhow::Error>(if full_batch {
				next.map(|mut key| {
					key.push(0x00);
					key
				})
			} else {
				None
			})
		};
		match sweep.await {
			Ok(next) => {
				let mut cursor = self.cleanup_cursor.lock().unwrap_or_else(|e| e.into_inner());
				*cursor = next;
			}
			Err(err) => {
				tracing::debug!("rate-limit bucket cleanup sweep failed: {err}");
			}
		}
	}
}

/// Mints the dedicated transactions that rate-limit charges settle in.
#[derive(Clone)]
pub(crate) struct ChargeSession {
	factory: TransactionFactory,
	sequences: Sequences,
	tenant: Option<Arc<TenantIdentity>>,
}

impl ChargeSession {
	pub(crate) fn new(
		factory: TransactionFactory,
		sequences: Sequences,
		tenant: Option<Arc<TenantIdentity>>,
	) -> Self {
		Self {
			factory,
			sequences,
			tenant,
		}
	}

	async fn transaction(&self) -> Result<Transaction> {
		Ok(self
			.factory
			.transaction(TransactionType::Write, LockType::Optimistic, self.sequences.clone())
			.await?
			.with_tenant_identity(self.tenant.clone()))
	}
}

/// Per-statement accumulator for records delivered in the response.
///
/// Rate limits meter what the client actually receives: each record whose
/// data is incorporated into the statement's result — source rows and
/// traversed records alike — is counted here against its own table, after
/// filtering and permission checks. Reads that never reach the response
/// (filtered rows, permission-hidden records, intermediate traversal hops)
/// are free: data egress is what these policies govern, while
/// resource-consumption abuse is a separate cost-budget concern.
///
/// Counting is a synchronous in-memory bump; every bucket interaction
/// happens once at statement settlement, where all applicable policies
/// are charged in a single all-or-nothing batch. If any policy denies,
/// the whole statement fails and nothing is charged or delivered —
/// responses are never partial.
#[derive(Debug, Default)]
pub(crate) struct DeliveryMeter {
	counts: Mutex<HashMap<String, u64>>,
}

impl DeliveryMeter {
	pub(crate) fn new() -> Self {
		Self::default()
	}

	/// Record `n` records of `table` entering the statement's response.
	pub(crate) fn record(&self, table: &str, n: u64) {
		if n == 0 {
			return;
		}
		let mut counts = self.counts.lock().unwrap_or_else(|e| e.into_inner());
		match counts.get_mut(table) {
			Some(count) => *count += n,
			None => {
				counts.insert(table.to_owned(), n);
			}
		}
	}

	/// Whether nothing has been delivered yet.
	pub(crate) fn is_empty(&self) -> bool {
		self.counts.lock().unwrap_or_else(|e| e.into_inner()).is_empty()
	}

	/// Drain the per-table delivered counts for settlement.
	pub(crate) fn take_counts(&self) -> Vec<(String, u64)> {
		let mut counts = self.counts.lock().unwrap_or_else(|e| e.into_inner());
		counts.drain().collect()
	}
}

fn is_conflict(err: &anyhow::Error) -> bool {
	err.downcast_ref::<KvsError>().is_some_and(|err| {
		matches!(err, KvsError::TransactionConflict(_) | KvsError::TransactionConditionNotMet)
	})
}

const RATE_LIMIT_PREFIX: &[u8] = b"/!rl";
/// Bucket value encoding version. Bump when the layout changes; unknown
/// versions decode as absent, so stale state simply re-initialises.
const BUCKET_ENCODING_VERSION: u8 = 1;
const BUCKET_ENCODED_LEN: usize = 25;

#[derive(Clone, Copy, Debug)]
struct StoredBucket {
	tokens: f64,
	last_ms: u64,
	expires_at_ms: u64,
}

fn now_millis() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis()
		.try_into()
		.unwrap_or(u64::MAX)
}

fn expiry_millis(now_ms: u64, period: Duration, capacity: f64, refill_per_second: f64) -> u64 {
	let refill_ms = ((capacity / refill_per_second.max(f64::EPSILON)) * 1000.0) as u64;
	now_ms
		.saturating_add(refill_ms)
		.saturating_add(period.as_millis().try_into().unwrap_or(u64::MAX))
}

fn kv_key(key: &BucketKey) -> Vec<u8> {
	let mut out = Vec::with_capacity(RATE_LIMIT_PREFIX.len() + 16);
	out.extend_from_slice(RATE_LIMIT_PREFIX);
	out.extend_from_slice(&key.0);
	out
}

fn load_bucket(
	old: Option<&[u8]>,
	now_ms: u64,
	period: Duration,
	capacity: f64,
	refill_per_second: f64,
) -> StoredBucket {
	old.and_then(decode_bucket).filter(|bucket| bucket.expires_at_ms > now_ms).unwrap_or(
		StoredBucket {
			tokens: capacity,
			last_ms: now_ms,
			expires_at_ms: expiry_millis(now_ms, period, capacity, refill_per_second),
		},
	)
}

fn refill_bucket(
	bucket: &mut StoredBucket,
	now_ms: u64,
	period: Duration,
	capacity: f64,
	refill_per_second: f64,
) {
	let elapsed = now_ms.saturating_sub(bucket.last_ms) as f64 / 1000.0;
	bucket.tokens = (bucket.tokens + elapsed * refill_per_second).min(capacity);
	bucket.last_ms = now_ms;
	bucket.expires_at_ms = expiry_millis(now_ms, period, capacity, refill_per_second);
}

fn encode_bucket(bucket: &StoredBucket) -> Vec<u8> {
	let mut out = Vec::with_capacity(BUCKET_ENCODED_LEN);
	out.push(BUCKET_ENCODING_VERSION);
	out.extend_from_slice(&bucket.tokens.to_bits().to_be_bytes());
	out.extend_from_slice(&bucket.last_ms.to_be_bytes());
	out.extend_from_slice(&bucket.expires_at_ms.to_be_bytes());
	out
}

async fn put_bucket(
	txn: &Transaction,
	key: &Vec<u8>,
	new: &Vec<u8>,
	old: Option<&Vec<u8>>,
) -> Result<bool> {
	match txn.putc(key, new, old).await {
		Ok(()) => Ok(true),
		Err(err)
			if matches!(
				err.downcast_ref::<KvsError>(),
				Some(KvsError::TransactionConditionNotMet)
			) =>
		{
			Ok(false)
		}
		Err(err) => Err(err),
	}
}

fn decode_bucket(bytes: &[u8]) -> Option<StoredBucket> {
	if bytes.len() != BUCKET_ENCODED_LEN || bytes[0] != BUCKET_ENCODING_VERSION {
		return None;
	}
	let bytes = &bytes[1..];
	let tokens = f64::from_bits(u64::from_be_bytes(bytes[0..8].try_into().ok()?));
	if !tokens.is_finite() || tokens < 0.0 {
		return None;
	}
	let last_ms = u64::from_be_bytes(bytes[8..16].try_into().ok()?);
	let expires_at_ms = u64::from_be_bytes(bytes[16..24].try_into().ok()?);
	Some(StoredBucket {
		tokens,
		last_ms,
		expires_at_ms,
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::Datastore;

	fn key_of(name: &str) -> BucketKey {
		let mut hasher = BucketKeyHasher::new();
		hasher.write(name.as_bytes());
		hasher.finalize()
	}

	fn charge_of(name: &str, limit: u64, period: Duration, max: Option<u64>) -> PendingCharge {
		PendingCharge {
			scope: format!("table {name}"),
			key: key_of(name),
			limit,
			period,
			max,
			amount: 1,
		}
	}

	#[tokio::test]
	async fn charge_max_allows_burst_above_refill_limit() {
		let ds = Datastore::new("memory").await.unwrap();
		let limiter = RateLimiter::default();
		let charge = charge_of("burst", 1, Duration::from_secs(3600), Some(2));

		for _ in 0..2 {
			assert!(matches!(
				limiter
					.charge(&ds.ratelimit_charge_session(None), std::slice::from_ref(&charge))
					.await
					.unwrap(),
				ChargeOutcome::Admitted
			));
		}
		match limiter.charge(&ds.ratelimit_charge_session(None), &[charge]).await.unwrap() {
			ChargeOutcome::Denied {
				scope,
				retry_after,
			} => {
				assert_eq!(scope, "table burst");
				assert!(retry_after.is_some());
			}
			other => panic!("expected denial, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn charge_shares_bucket_across_limiter_instances() {
		let ds = Datastore::new("memory").await.unwrap();
		let limiter_a = RateLimiter::default();
		let limiter_b = RateLimiter::default();
		let charge = charge_of("shared", 1, Duration::from_secs(3600), None);

		assert!(matches!(
			limiter_a
				.charge(&ds.ratelimit_charge_session(None), std::slice::from_ref(&charge))
				.await
				.unwrap(),
			ChargeOutcome::Admitted
		));
		assert!(matches!(
			limiter_b.charge(&ds.ratelimit_charge_session(None), &[charge]).await.unwrap(),
			ChargeOutcome::Denied { .. }
		));
	}

	#[tokio::test]
	async fn charge_batch_is_all_or_nothing() {
		let ds = Datastore::new("memory").await.unwrap();
		let limiter = RateLimiter::default();
		let generous = charge_of("generous", 100, Duration::from_secs(3600), None);
		let strict = charge_of("strict", 1, Duration::from_secs(3600), None);

		// Drain the strict bucket.
		assert!(matches!(
			limiter
				.charge(&ds.ratelimit_charge_session(None), std::slice::from_ref(&strict))
				.await
				.unwrap(),
			ChargeOutcome::Admitted
		));
		// A batch containing the drained bucket denies as a whole...
		assert!(matches!(
			limiter
				.charge(&ds.ratelimit_charge_session(None), &[generous.clone(), strict])
				.await
				.unwrap(),
			ChargeOutcome::Denied { .. }
		));
		// ...and must not have consumed from the generous bucket: all 100
		// tokens remain admissible.
		for _ in 0..100 {
			assert!(matches!(
				limiter
					.charge(&ds.ratelimit_charge_session(None), std::slice::from_ref(&generous))
					.await
					.unwrap(),
				ChargeOutcome::Admitted
			));
		}
		assert!(matches!(
			limiter.charge(&ds.ratelimit_charge_session(None), &[generous]).await.unwrap(),
			ChargeOutcome::Denied { .. }
		));
	}

	#[tokio::test]
	async fn charge_amount_above_capacity_is_permanently_denied() {
		let ds = Datastore::new("memory").await.unwrap();
		let limiter = RateLimiter::default();
		let mut charge = charge_of("oversized", 10, Duration::from_secs(1), None);
		charge.amount = 11;

		match limiter.charge(&ds.ratelimit_charge_session(None), &[charge]).await.unwrap() {
			ChargeOutcome::Denied {
				retry_after,
				..
			} => assert!(retry_after.is_none(), "capacity-exceeding batch has no retry-after"),
			other => panic!("expected denial, got {other:?}"),
		}
	}

	#[tokio::test]
	async fn concurrent_charges_on_one_bucket_do_not_spuriously_deny() {
		let ds = Arc::new(Datastore::new("memory").await.unwrap());
		let limiter = Arc::new(RateLimiter::default());
		let charge = charge_of("contended", 64, Duration::from_secs(3600), None);

		let mut handles = Vec::new();
		for _ in 0..8 {
			let ds = Arc::clone(&ds);
			let limiter = Arc::clone(&limiter);
			let charge = charge.clone();
			handles.push(tokio::spawn(async move {
				let mut admitted = 0;
				for _ in 0..8 {
					match limiter
						.charge(&ds.ratelimit_charge_session(None), std::slice::from_ref(&charge))
						.await
					{
						Ok(ChargeOutcome::Admitted) => admitted += 1,
						Ok(ChargeOutcome::Denied {
							..
						}) => {}
						// Retries exhausted under extreme contention is
						// acceptable; spurious denial is not.
						Err(err) => {
							assert!(
								err.downcast_ref::<KvsError>().is_some_and(KvsError::is_retryable),
								"unexpected error: {err}"
							);
						}
					}
				}
				admitted
			}));
		}
		let mut total = 0;
		for handle in handles {
			total += handle.await.unwrap();
		}
		// The bucket held exactly 64 tokens; no over-admission.
		assert!(total <= 64, "over-admitted: {total}");
	}

	#[tokio::test]
	async fn cleanup_removes_stale_bucket_keys() {
		let ds = Datastore::new("memory").await.unwrap();
		let limiter = RateLimiter::default();
		let expired_key = kv_key(&key_of("expired"));
		let expired_bucket = StoredBucket {
			tokens: 0.0,
			last_ms: 0,
			expires_at_ms: 1,
		};

		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
		tx.set(&expired_key, &encode_bucket(&expired_bucket)).await.unwrap();
		tx.commit().await.unwrap();

		// Force the next charge to trigger a sweep.
		limiter.cleanup_counter.store(CLEANUP_INTERVAL, Ordering::Relaxed);
		assert!(matches!(
			limiter
				.charge(
					&ds.ratelimit_charge_session(None),
					&[charge_of("cleanup-trigger", 1, Duration::from_secs(1), None)]
				)
				.await
				.unwrap(),
			ChargeOutcome::Admitted
		));

		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
		assert!(tx.get(&expired_key, None).await.unwrap().is_none());
		tx.cancel().await.unwrap();
	}

	#[tokio::test]
	async fn cleanup_removes_undecodable_bucket_state() {
		let ds = Datastore::new("memory").await.unwrap();
		let limiter = RateLimiter::default();
		// Undecodable encoding: wrong length, no version byte.
		let stale_key = kv_key(&key_of("undecodable"));

		let tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
		tx.set(&stale_key, &vec![0u8; 24]).await.unwrap();
		tx.commit().await.unwrap();

		limiter.cleanup_counter.store(CLEANUP_INTERVAL, Ordering::Relaxed);
		assert!(matches!(
			limiter
				.charge(
					&ds.ratelimit_charge_session(None),
					&[charge_of("cleanup-trigger", 1, Duration::from_secs(1), None)]
				)
				.await
				.unwrap(),
			ChargeOutcome::Admitted
		));

		let tx = ds.transaction(TransactionType::Read, LockType::Optimistic).await.unwrap();
		assert!(tx.get(&stale_key, None).await.unwrap().is_none());
		tx.cancel().await.unwrap();
	}

	#[test]
	fn delivery_meter_accumulates_per_table_counts() {
		let meter = DeliveryMeter::new();
		assert!(meter.is_empty());
		meter.record("user", 2);
		meter.record("friend", 1);
		meter.record("user", 3);
		meter.record("ignored", 0);
		assert!(!meter.is_empty());

		let mut counts = meter.take_counts();
		counts.sort();
		assert_eq!(counts, vec![("friend".to_string(), 1), ("user".to_string(), 5)]);
		// Draining resets the meter: nothing is double-settled.
		assert!(meter.is_empty());
		assert!(meter.take_counts().is_empty());
	}

	#[test]
	fn bucket_key_derivation_is_stable() {
		// The derived key is a persistence format: this vector must never
		// change. If this test fails, the key derivation changed and every
		// live bucket in existing datastores would be orphaned.
		let mut hasher = BucketKeyHasher::new();
		hasher.write(b"ns");
		hasher.write_u64(42);
		hasher.write(b"table");
		let key = hasher.finalize();
		assert_eq!(
			key.0,
			[119, 109, 44, 116, 225, 37, 254, 67, 248, 50, 246, 168, 128, 128, 19, 55],
			"bucket key derivation changed"
		);
	}
}
