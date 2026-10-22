//! Test doubles for the backends of this crate's callers.
//!
//! [`ViewlessBuilder`] opens the transactions of the backend it wraps, and hands each out as a
//! [`ViewlessTransaction`]: one that answers every read and write as the backend's own does, but
//! gives no read view and declares none ([`Transactable::snapshot`],
//! [`Transactable::view_capabilities`]). A test whose subject is a datastore that gives workers
//! no view to read through runs over it, whatever the backend under it gives.

use std::any::{Any, TypeId};
use std::sync::Arc;
use std::time::Duration;

use crate::api::{BoxFut, ScanChunkStats};
use crate::err::Result;
use crate::snapshot::ViewCapabilities;
use crate::stale::{ReadOutcome, ReadSnapshotRequest, StaleReads, StaleReadsHandle};
use crate::timestamp::{BoxTimeStamp, BoxTimeStampImpl};
use crate::{
	Batch, CountCost, Direction, Estimate, GetMultiResult, Isolation, IsolationLevels,
	IsolationLevelsHandle, Key, KeyRange, KeysResult, Metrics, RangeKeyVisitor, ScanCursorKeys,
	ScanCursorVals, ScanResult, Snapshot, SplitTarget, Transactable, TransactionBuilder,
	TransactionType, Val,
};

/// A backend whose transactions give no read view. See the [module docs](self).
pub struct ViewlessBuilder(pub Box<dyn TransactionBuilder>);

/// A transaction that answers as the one it wraps, except that it gives no read view and
/// declares none. See the [module docs](self).
pub struct ViewlessTransaction(pub Box<dyn Transactable>);

impl Transactable for ViewlessTransaction {
	fn kind(&self) -> &'static str {
		self.0.kind()
	}
	fn sole_writer(&self) -> bool {
		self.0.sole_writer()
	}
	fn validates_reads(&self) -> bool {
		self.0.validates_reads()
	}
	fn parallel_count(&self) -> bool {
		self.0.parallel_count()
	}
	fn bounds_write_buffer(&self) -> bool {
		self.0.bounds_write_buffer()
	}
	fn count_cost(&self) -> CountCost {
		self.0.count_cost()
	}
	fn closed(&self) -> bool {
		self.0.closed()
	}
	fn writeable(&self) -> bool {
		self.0.writeable()
	}
	fn cancel<'a>(&'a self) -> BoxFut<'a, Result<()>> {
		self.0.cancel()
	}
	fn commit<'a>(&'a self) -> BoxFut<'a, Result<()>> {
		self.0.commit()
	}
	fn commit_unsynced<'a>(&'a self) -> BoxFut<'a, Result<()>> {
		self.0.commit_unsynced()
	}
	fn exists<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, Result<bool>> {
		self.0.exists(key, version)
	}
	fn get<'a>(&'a self, key: Key<'a>, version: Option<u64>) -> BoxFut<'a, Result<Option<Val>>> {
		self.0.get(key, version)
	}
	fn getu<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, Result<Option<Val>>> {
		self.0.getu(key)
	}
	fn shared_locked_reads(&self) -> bool {
		self.0.shared_locked_reads()
	}
	fn set<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, Result<()>> {
		self.0.set(key, val)
	}
	fn put<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, Result<()>> {
		self.0.put(key, val)
	}
	fn putc<'a>(&'a self, key: Key<'a>, val: Val, chk: Option<Val>) -> BoxFut<'a, Result<()>> {
		self.0.putc(key, val, chk)
	}
	fn del<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, Result<()>> {
		self.0.del(key)
	}
	fn delc<'a>(&'a self, key: Key<'a>, chk: Option<&'a [u8]>) -> BoxFut<'a, Result<()>> {
		self.0.delc(key, chk)
	}
	fn keys<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<KeysResult>> {
		self.0.keys(rng, limit, skip, version)
	}
	fn keysr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<KeysResult>> {
		self.0.keysr(rng, limit, skip, version)
	}
	fn scan<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<ScanResult>> {
		self.0.scan(rng, limit, skip, version)
	}
	fn scanr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<ScanResult>> {
		self.0.scanr(rng, limit, skip, version)
	}
	fn open_keys_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn ScanCursorKeys + 'a>>> {
		self.0.open_keys_cursor(rng, dir, skip, version)
	}
	fn visit_key_ranges<'a>(
		&'a self,
		ranges: &'a [KeyRange<'a>],
		limit: u32,
		version: Option<u64>,
		f: &'a mut dyn RangeKeyVisitor,
	) -> BoxFut<'a, Result<ScanChunkStats>> {
		self.0.visit_key_ranges(ranges, limit, version, f)
	}
	fn open_vals_cursor<'a>(
		&'a self,
		rng: KeyRange<'a>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Box<dyn ScanCursorVals + 'a>>> {
		self.0.open_vals_cursor(rng, dir, skip, version)
	}
	fn replace<'a>(&'a self, key: Key<'a>, val: Val) -> BoxFut<'a, Result<()>> {
		self.0.replace(key, val)
	}
	fn clr<'a>(&'a self, key: Key<'a>) -> BoxFut<'a, Result<()>> {
		self.0.clr(key)
	}
	fn clrc<'a>(&'a self, key: Key<'a>, chk: Option<&'a [u8]>) -> BoxFut<'a, Result<()>> {
		self.0.clrc(key, chk)
	}
	fn getm<'a>(
		&'a self,
		keys: &'a [Key<'a>],
		version: Option<u64>,
	) -> BoxFut<'a, Result<GetMultiResult>> {
		self.0.getm(keys, version)
	}
	fn getr<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
	) -> BoxFut<'a, Result<ScanResult>> {
		self.0.getr(rng, version)
	}
	fn delr<'a>(&'a self, rng: KeyRange<'a>) -> BoxFut<'a, Result<()>> {
		self.0.delr(rng)
	}
	fn clrr<'a>(&'a self, rng: KeyRange<'a>) -> BoxFut<'a, Result<()>> {
		self.0.clrr(rng)
	}
	fn count<'a>(&'a self, rng: KeyRange<'a>, version: Option<u64>) -> BoxFut<'a, Result<usize>> {
		self.0.count(rng, version)
	}
	fn count_bounded<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
		budget: u32,
	) -> BoxFut<'a, Result<(usize, Option<Key<'static>>)>> {
		self.0.count_bounded(rng, version, budget)
	}
	fn snapshot(&self) -> Option<Arc<dyn Snapshot>> {
		None
	}
	fn view_capabilities(&self) -> ViewCapabilities {
		ViewCapabilities::default()
	}
	fn plain_reads_tracked(&self) -> bool {
		self.0.plain_reads_tracked()
	}
	fn blind_write_conflicts(&self) -> bool {
		self.0.blind_write_conflicts()
	}
	fn scan_read_ahead(&self) -> bool {
		self.0.scan_read_ahead()
	}
	fn multi_read(&self) -> bool {
		self.0.multi_read()
	}
	fn estimate<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Estimate>> {
		self.0.estimate(rng, version)
	}
	fn estimate_many<'a>(
		&'a self,
		ranges: Vec<KeyRange<'a>>,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Vec<Estimate>>> {
		self.0.estimate_many(ranges, version)
	}
	fn approximate<'a>(
		&'a self,
		rng: KeyRange<'a>,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Option<Estimate>>> {
		self.0.approximate(rng, version)
	}
	fn split<'a>(
		&'a self,
		rng: KeyRange<'a>,
		target: SplitTarget,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Vec<Key<'static>>>> {
		self.0.split(rng, target, version)
	}
	fn batch_keys<'a>(
		&'a self,
		rng: KeyRange<'a>,
		batch: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Batch<Vec<u8>>>> {
		self.0.batch_keys(rng, batch, version)
	}
	fn batch_keys_vals<'a>(
		&'a self,
		rng: KeyRange<'a>,
		batch: u32,
		version: Option<u64>,
	) -> BoxFut<'a, Result<Batch<(Vec<u8>, Vec<u8>)>>> {
		self.0.batch_keys_vals(rng, batch, version)
	}
	fn new_save_point(&self) -> BoxFut<'_, Result<()>> {
		self.0.new_save_point()
	}
	fn release_last_save_point(&self) -> BoxFut<'_, Result<()>> {
		self.0.release_last_save_point()
	}
	fn rollback_to_save_point(&self) -> BoxFut<'_, Result<()>> {
		self.0.rollback_to_save_point()
	}
	fn timestamp(&self) -> BoxFut<'_, Result<BoxTimeStamp>> {
		self.0.timestamp()
	}
	fn safe_timestamp(&self) -> BoxFut<'_, Result<BoxTimeStamp>> {
		self.0.safe_timestamp()
	}
	fn timestamp_impl(&self) -> BoxTimeStampImpl {
		self.0.timestamp_impl()
	}
	fn version_impl(&self) -> BoxTimeStampImpl {
		self.0.version_impl()
	}
	fn compact<'a>(&'a self, range: Option<KeyRange<'a>>) -> BoxFut<'a, Result<()>> {
		self.0.compact(range)
	}
}

impl TransactionBuilder for ViewlessBuilder {
	fn name(&self) -> &'static str {
		self.0.name()
	}

	fn new_transaction(
		&self,
		write: TransactionType,
	) -> BoxFut<'_, Result<(Box<dyn Transactable>, bool)>> {
		Box::pin(async move {
			let (tx, flag) = self.0.new_transaction(write).await?;
			Ok((Box::new(ViewlessTransaction(tx)) as Box<dyn Transactable>, flag))
		})
	}

	fn shutdown(&self) -> BoxFut<'_, Result<()>> {
		self.0.shutdown()
	}

	fn register_metrics(&self) -> Option<Metrics> {
		self.0.register_metrics()
	}

	fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
		self.0.collect_u64_metric(metric)
	}

	fn wait_until_serve_ready(&self) -> BoxFut<'_, Result<()>> {
		self.0.wait_until_serve_ready()
	}

	fn extension(&self, id: TypeId) -> Option<Arc<dyn Any + Send + Sync>> {
		let ext = self.0.extension(id)?;
		if id == TypeId::of::<IsolationLevelsHandle>() {
			let handle = ext.downcast::<IsolationLevelsHandle>().ok()?;
			let levels: Arc<dyn IsolationLevels> =
				Arc::new(ViewlessIsolation(Arc::clone(&handle.0)));
			return Some(Arc::new(IsolationLevelsHandle(levels)));
		}
		if id == TypeId::of::<StaleReadsHandle>() {
			let handle = ext.downcast::<StaleReadsHandle>().ok()?;
			let stale: Arc<dyn StaleReads> = Arc::new(ViewlessStaleReads(Arc::clone(&handle.0)));
			return Some(Arc::new(StaleReadsHandle(stale)));
		}
		Some(ext)
	}
}

struct ViewlessIsolation(Arc<dyn IsolationLevels>);

impl IsolationLevels for ViewlessIsolation {
	fn supported(&self) -> &'static [Isolation] {
		self.0.supported()
	}

	fn open(
		&self,
		write: TransactionType,
		isolation: Isolation,
	) -> BoxFut<'_, Result<(Box<dyn Transactable>, bool)>> {
		Box::pin(async move {
			let (tx, local) = self.0.open(write, isolation).await?;
			Ok((Box::new(ViewlessTransaction(tx)) as Box<dyn Transactable>, local))
		})
	}
}

struct ViewlessStaleReads(Arc<dyn StaleReads>);

impl StaleReads for ViewlessStaleReads {
	fn min_staleness(&self) -> Duration {
		self.0.min_staleness()
	}

	fn max_staleness(&self) -> Duration {
		self.0.max_staleness()
	}

	fn auth_staleness(&self) -> Duration {
		self.0.auth_staleness()
	}

	fn open(&self, request: ReadSnapshotRequest) -> BoxFut<'_, Result<ReadOutcome>> {
		Box::pin(async move {
			Ok(match self.0.open(request).await? {
				ReadOutcome::Opened {
					transaction,
					local,
					info,
				} => ReadOutcome::Opened {
					transaction: Box::new(ViewlessTransaction(transaction)),
					local,
					info,
				},
				other @ ReadOutcome::BoundUnmet {
					..
				} => other,
			})
		})
	}
}
