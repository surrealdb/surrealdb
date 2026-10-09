//! A temporary store of two tiers: a local store with a capacity, and an
//! overflow store past it.
//!
//! On an edge host the local tier is a Durable Object's storage, whose size
//! is capped and shared with the object's own data, and the overflow is
//! object storage. A run is written to the local tier while the tier has
//! room for its next part, and its remaining parts to the overflow once it
//! has not, so a run may lie across both: it is read back segment by segment,
//! in the order written. A part is written whole to one tier.
//!
//! A run written [`Sharing::Private`] is `Local` even where some of its
//! segments lie in the overflow, because the map of its segments exists only
//! in this store. A run written [`Sharing::Shared`] bypasses the local tier:
//! it is written straight to the overflow, which must itself share runs, and
//! its name is the overflow's, so any handle on the overflow's backend can
//! open it.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::store::{
	RunId, RunRead, RunRef, RunWrite, SessionName, Sharing, TempStore, Tier, cannot_share,
	not_readable,
};
use crate::exec::BoxFut;

/// The names of the tiers' sessions in this process.
static NEXT_TIERED_SESSION: AtomicU64 = AtomicU64::new(1);

/// The segments of one run: each a run in one tier, `true` for the local.
type Segments = Vec<(bool, RunRef)>;

/// A local tier holding up to `capacity` bytes, then an overflow tier.
pub struct TieredTempStore {
	inner: Arc<Tiers>,
	next: AtomicU64,
}

struct Tiers {
	session: SessionName,
	local: Arc<dyn TempStore>,
	overflow: Arc<dyn TempStore>,
	capacity: u64,
	/// The bytes the local tier holds for this store's runs.
	used: AtomicU64,
	runs: Mutex<HashMap<RunId, (Segments, u64)>>,
}

impl std::fmt::Debug for TieredTempStore {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("TieredTempStore")
			.field("capacity", &self.inner.capacity)
			.field("used", &self.local_bytes())
			.finish()
	}
}

impl TieredTempStore {
	/// A store writing to `local` until it holds `capacity` bytes, then to
	/// `overflow`. Parts are the larger of the two tiers' part sizes.
	pub fn new(local: Arc<dyn TempStore>, capacity: u64, overflow: Arc<dyn TempStore>) -> Self {
		Self {
			inner: Arc::new(Tiers {
				session: SessionName::new(format!(
					"tiered-{}",
					NEXT_TIERED_SESSION.fetch_add(1, Ordering::Relaxed)
				)),
				local,
				overflow,
				capacity,
				used: AtomicU64::new(0),
				runs: Mutex::new(HashMap::new()),
			}),
			next: AtomicU64::new(1),
		}
	}

	/// The bytes the local tier holds for this store's runs.
	pub fn local_bytes(&self) -> u64 {
		self.inner.used.load(Ordering::Acquire)
	}

	/// Whether run `run` lies, wholly or in part, in the overflow tier.
	pub fn overflowed(&self, run: &RunRef) -> bool {
		if run.session != self.inner.session {
			return self.inner.overflow.shares() && run.tier == Tier::Shared;
		}
		let runs = self.inner.runs.lock().unwrap_or_else(|e| e.into_inner());
		runs.get(&run.id).is_some_and(|(segments, _)| segments.iter().any(|(local, _)| !local))
	}
}

impl Tiers {
	/// Claim `bytes` of the local tier, answering whether it had room.
	fn claim(&self, bytes: u64) -> bool {
		self.used
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
				(used + bytes <= self.capacity).then_some(used + bytes)
			})
			.is_ok()
	}
}

impl TempStore for TieredTempStore {
	fn part_size(&self) -> usize {
		self.inner.local.part_size().max(self.inner.overflow.part_size())
	}

	fn quota(&self) -> Option<u64> {
		match (self.inner.local.quota(), self.inner.overflow.quota()) {
			(Some(a), Some(b)) => Some(a.saturating_add(b)),
			_ => None,
		}
	}

	fn session(&self) -> &SessionName {
		&self.inner.session
	}

	fn shares(&self) -> bool {
		self.inner.overflow.shares()
	}

	fn write_run(&self, sharing: Sharing) -> BoxFut<'_, anyhow::Result<Box<dyn RunWrite>>> {
		let id = self.next.fetch_add(1, Ordering::Relaxed);
		let tiers = Arc::clone(&self.inner);
		Box::pin(async move {
			if sharing == Sharing::Shared {
				if !tiers.overflow.shares() {
					return Err(cannot_share());
				}
				return tiers.overflow.write_run(Sharing::Shared).await;
			}
			Ok(Box::new(TieredWrite {
				id,
				tiers,
				current: None,
				segments: Vec::new(),
				local_bytes: 0,
				bytes: 0,
				finished: false,
			}) as Box<dyn RunWrite>)
		})
	}

	fn read_run(&self, run: &RunRef) -> BoxFut<'_, anyhow::Result<Box<dyn RunRead>>> {
		let tiers = Arc::clone(&self.inner);
		let run = run.clone();
		Box::pin(async move {
			if run.session != tiers.session {
				// Only a shared run is named by a session other than this
				// store's; another session's private segments stay private.
				if run.tier != Tier::Shared {
					return Err(not_readable());
				}
				return tiers.overflow.read_run(&run).await;
			}
			let segments = tiers
				.runs
				.lock()
				.unwrap_or_else(|e| e.into_inner())
				.get(&run.id)
				.map(|(segments, _)| segments.clone())
				.ok_or_else(|| anyhow::anyhow!("A spilled run is missing from its store"))?;
			Ok(Box::new(TieredRead {
				tiers,
				segments: segments.into_iter(),
				current: None,
			}) as Box<dyn RunRead>)
		})
	}

	fn delete_run(&self, run: &RunRef) {
		if run.session != self.inner.session {
			if run.tier == Tier::Shared {
				self.inner.overflow.delete_run(run);
			}
			return;
		}
		let removed = self.inner.runs.lock().unwrap_or_else(|e| e.into_inner()).remove(&run.id);
		if let Some((segments, local_bytes)) = removed {
			for (local, segment) in &segments {
				match local {
					true => self.inner.local.delete_run(segment),
					false => self.inner.overflow.delete_run(segment),
				}
			}
			self.inner.used.fetch_sub(local_bytes, Ordering::AcqRel);
		}
	}
}

struct TieredWrite {
	id: RunId,
	tiers: Arc<Tiers>,
	/// The segment being written, and whether it is the local tier's.
	current: Option<(bool, Box<dyn RunWrite>)>,
	segments: Segments,
	local_bytes: u64,
	/// The bytes written across every segment.
	bytes: u64,
	finished: bool,
}

/// A run dropped before it finishes releases its segments and the local
/// capacity they hold; the open segment's writer releases itself.
impl Drop for TieredWrite {
	fn drop(&mut self) {
		if self.finished {
			return;
		}
		for (local, segment) in &self.segments {
			match local {
				true => self.tiers.local.delete_run(segment),
				false => self.tiers.overflow.delete_run(segment),
			}
		}
		self.tiers.used.fetch_sub(self.local_bytes, Ordering::AcqRel);
	}
}

impl TieredWrite {
	async fn close_current(&mut self) -> anyhow::Result<()> {
		if let Some((local, writer)) = self.current.take() {
			let segment = writer.finish().await?;
			self.segments.push((local, segment));
		}
		Ok(())
	}
}

impl RunWrite for TieredWrite {
	fn write_part(&mut self, part: Vec<u8>) -> BoxFut<'_, anyhow::Result<()>> {
		Box::pin(async move {
			let len = part.len() as u64;
			self.bytes += len;
			let local = self.tiers.claim(len);
			if local {
				self.local_bytes += len;
			}
			if self.current.as_ref().is_some_and(|(at, _)| *at != local) {
				self.close_current().await?;
			}
			if self.current.is_none() {
				let tier = if local {
					&self.tiers.local
				} else {
					&self.tiers.overflow
				};
				self.current = Some((local, tier.write_run(Sharing::Private).await?));
			}
			let (_, writer) = self.current.as_mut().expect("a segment is open");
			writer.write_part(part).await
		})
	}

	fn finish(self: Box<Self>) -> BoxFut<'static, anyhow::Result<RunRef>> {
		Box::pin(async move {
			let mut this = *self;
			this.close_current().await?;
			let segments = std::mem::take(&mut this.segments);
			this.tiers
				.runs
				.lock()
				.unwrap_or_else(|e| e.into_inner())
				.insert(this.id, (segments, this.local_bytes));
			this.finished = true;
			Ok(RunRef {
				session: this.tiers.session.clone(),
				id: this.id,
				tier: Tier::Local,
				bytes: this.bytes,
			})
		})
	}
}

struct TieredRead {
	tiers: Arc<Tiers>,
	segments: std::vec::IntoIter<(bool, RunRef)>,
	current: Option<Box<dyn RunRead>>,
}

impl RunRead for TieredRead {
	fn next_part(&mut self) -> BoxFut<'_, anyhow::Result<Option<Vec<u8>>>> {
		Box::pin(async move {
			loop {
				if let Some(reader) = &mut self.current
					&& let Some(part) = reader.next_part().await?
				{
					return Ok(Some(part));
				}
				let Some((local, segment)) = self.segments.next() else {
					return Ok(None);
				};
				let tier = if local {
					&self.tiers.local
				} else {
					&self.tiers.overflow
				};
				self.current = Some(tier.read_run(&segment).await?);
			}
		})
	}
}

#[cfg(test)]
mod tests {
	use super::super::store::MemTempStore;
	use super::*;

	fn tiers(
		local_part: usize,
		capacity: u64,
	) -> (Arc<MemTempStore>, Arc<MemTempStore>, MemTempStore, TieredTempStore) {
		let local = Arc::new(MemTempStore::new(local_part, None));
		let (over_a, over_b) = MemTempStore::shared(4, None);
		let (over_a, over_b) = (Arc::new(over_a), over_b);
		let store = TieredTempStore::new(
			Arc::clone(&local) as Arc<dyn TempStore>,
			capacity,
			Arc::clone(&over_a) as Arc<dyn TempStore>,
		);
		(local, over_a, over_b, store)
	}

	/// Parts past the local tier's capacity are written to the overflow, and
	/// the run reads back whole, in order, across both. The run stays `Local`.
	#[tokio::test]
	async fn a_run_past_the_local_capacity_overflows_and_reads_back_whole() {
		let local = Arc::new(MemTempStore::new(4, None));
		let overflow = Arc::new(MemTempStore::new(4, None));
		let store = TieredTempStore::new(
			Arc::clone(&local) as Arc<dyn TempStore>,
			8,
			Arc::clone(&overflow) as Arc<dyn TempStore>,
		);
		let mut run = store.write_run(Sharing::Private).await.expect("a run");
		for part in [b"aaaa", b"bbbb", b"cccc", b"dddd"] {
			run.write_part(part.to_vec()).await.expect("a part");
		}
		let id = run.finish().await.expect("finished");
		assert!(store.overflowed(&id));
		assert_eq!(id.tier, Tier::Local);
		assert_eq!(id.session, *store.session());
		assert_eq!(local.bytes(), 8);
		assert_eq!(overflow.bytes(), 8);
		let mut read = store.read_run(&id).await.expect("a reader");
		let mut got = Vec::new();
		while let Some(part) = read.next_part().await.expect("a part") {
			got.extend(part);
		}
		assert_eq!(got, b"aaaabbbbccccdddd");
		store.delete_run(&id);
		assert_eq!(store.local_bytes(), 0);
		assert_eq!(local.runs() + overflow.runs(), 0);
	}

	/// A shared run is written to the overflow alone, under the overflow's
	/// name, and any handle on the overflow's backend reads it.
	#[tokio::test]
	async fn a_shared_run_bypasses_the_local_tier() {
		let (local, over_a, over_b, store) = tiers(4, 1 << 20);
		assert!(store.shares());
		let mut run = store.write_run(Sharing::Shared).await.expect("a run");
		run.write_part(b"aaaa".to_vec()).await.expect("a part");
		run.write_part(b"bbbb".to_vec()).await.expect("a part");
		let id = run.finish().await.expect("finished");
		assert_eq!(id.tier, Tier::Shared);
		assert_eq!(id.session, *over_a.session());
		assert_eq!(local.bytes(), 0);
		let mut read = over_b.read_run(&id).await.expect("a reader");
		let mut got = Vec::new();
		while let Some(part) = read.next_part().await.expect("a part") {
			got.extend(part);
		}
		assert_eq!(got, b"aaaabbbb");
		drop(read);
		store.delete_run(&id);
		assert_eq!(over_b.runs(), 0);
	}

	/// A run dropped before it finishes releases its segments and the local
	/// capacity they held.
	#[tokio::test]
	async fn a_dropped_run_releases_its_segments() {
		let local = Arc::new(MemTempStore::new(4, None));
		let overflow = Arc::new(MemTempStore::new(4, None));
		let store = TieredTempStore::new(
			Arc::clone(&local) as Arc<dyn TempStore>,
			8,
			Arc::clone(&overflow) as Arc<dyn TempStore>,
		);
		let mut run = store.write_run(Sharing::Private).await.expect("a run");
		for part in [b"aaaa", b"bbbb", b"cccc"] {
			run.write_part(part.to_vec()).await.expect("a part");
		}
		assert_eq!(store.local_bytes(), 8);
		drop(run);
		assert_eq!(store.local_bytes(), 0);
		assert_eq!(local.runs() + overflow.runs(), 0);
	}

	/// A private segment of the tiered store is not readable or deletable by
	/// naming it in the overflow's session.
	#[tokio::test]
	async fn a_private_ref_of_the_overflow_session_is_refused() {
		let (_, over_a, _, store) = tiers(4, 0);
		let mut run = store.write_run(Sharing::Private).await.expect("a run");
		run.write_part(b"aaaa".to_vec()).await.expect("a part");
		let id = run.finish().await.expect("finished");
		let segment = RunRef {
			session: over_a.session().clone(),
			id: 1,
			tier: Tier::Local,
			bytes: 0,
		};
		assert!(store.read_run(&segment).await.is_err());
		store.delete_run(&segment);
		assert!(store.read_run(&id).await.is_ok());
	}

	/// A tiered store whose overflow cannot share cannot share.
	#[tokio::test]
	async fn a_tiered_store_over_a_local_overflow_refuses_to_share() {
		let store = TieredTempStore::new(
			Arc::new(MemTempStore::new(4, None)),
			8,
			Arc::new(MemTempStore::new(4, None)),
		);
		assert!(!store.shares());
		assert!(store.write_run(Sharing::Shared).await.is_err());
	}
}
