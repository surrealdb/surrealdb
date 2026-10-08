//! A temporary store of two tiers: a local store with a capacity, and an
//! overflow store past it.
//!
//! On an edge host the local tier is a Durable Object's storage, whose size
//! is capped and shared with the object's own data, and the overflow is
//! object storage. A run is written to the local tier while the tier has
//! room for its next part, and its remaining parts to the overflow once it
//! has not, so a run may lie across both: it is read back segment by segment,
//! in the order written. A part is written whole to one tier.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::store::{RunId, RunRead, RunWrite, TempStore};
use crate::exec::BoxFut;

/// The segments of one run: each a run in one tier, `true` for the local.
type Segments = Vec<(bool, RunId)>;

/// A local tier holding up to `capacity` bytes, then an overflow tier.
pub struct TieredTempStore {
	inner: Arc<Tiers>,
	next: AtomicU64,
}

struct Tiers {
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
	pub fn overflowed(&self, run: RunId) -> bool {
		let runs = self.inner.runs.lock().unwrap_or_else(|e| e.into_inner());
		runs.get(&run).is_some_and(|(segments, _)| segments.iter().any(|(local, _)| !local))
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

	fn write_run(&self) -> BoxFut<'_, anyhow::Result<Box<dyn RunWrite>>> {
		let id = self.next.fetch_add(1, Ordering::Relaxed);
		let tiers = Arc::clone(&self.inner);
		Box::pin(async move {
			Ok(Box::new(TieredWrite {
				id,
				tiers,
				current: None,
				segments: Vec::new(),
				local_bytes: 0,
			}) as Box<dyn RunWrite>)
		})
	}

	fn read_run(&self, run: RunId) -> BoxFut<'_, anyhow::Result<Box<dyn RunRead>>> {
		let tiers = Arc::clone(&self.inner);
		Box::pin(async move {
			let segments = tiers
				.runs
				.lock()
				.unwrap_or_else(|e| e.into_inner())
				.get(&run)
				.map(|(segments, _)| segments.clone())
				.ok_or_else(|| anyhow::anyhow!("A spilled run is missing from its store"))?;
			Ok(Box::new(TieredRead {
				tiers,
				segments: segments.into_iter(),
				current: None,
			}) as Box<dyn RunRead>)
		})
	}

	fn delete_run(&self, run: RunId) {
		let removed = self.inner.runs.lock().unwrap_or_else(|e| e.into_inner()).remove(&run);
		if let Some((segments, local_bytes)) = removed {
			for (local, id) in segments {
				match local {
					true => self.inner.local.delete_run(id),
					false => self.inner.overflow.delete_run(id),
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
}

impl TieredWrite {
	async fn close_current(&mut self) -> anyhow::Result<()> {
		if let Some((local, writer)) = self.current.take() {
			let id = writer.finish().await?;
			self.segments.push((local, id));
		}
		Ok(())
	}
}

impl RunWrite for TieredWrite {
	fn write_part(&mut self, part: Vec<u8>) -> BoxFut<'_, anyhow::Result<()>> {
		Box::pin(async move {
			let len = part.len() as u64;
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
				self.current = Some((local, tier.write_run().await?));
			}
			let (_, writer) = self.current.as_mut().expect("a segment is open");
			writer.write_part(part).await
		})
	}

	fn finish(self: Box<Self>) -> BoxFut<'static, anyhow::Result<RunId>> {
		Box::pin(async move {
			let mut this = *self;
			this.close_current().await?;
			let segments = std::mem::take(&mut this.segments);
			this.tiers
				.runs
				.lock()
				.unwrap_or_else(|e| e.into_inner())
				.insert(this.id, (segments, this.local_bytes));
			Ok(this.id)
		})
	}
}

struct TieredRead {
	tiers: Arc<Tiers>,
	segments: std::vec::IntoIter<(bool, RunId)>,
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
				let Some((local, id)) = self.segments.next() else {
					return Ok(None);
				};
				let tier = if local {
					&self.tiers.local
				} else {
					&self.tiers.overflow
				};
				self.current = Some(tier.read_run(id).await?);
			}
		})
	}
}

#[cfg(test)]
mod tests {
	use super::super::store::MemTempStore;
	use super::*;

	/// Parts past the local tier's capacity are written to the overflow, and
	/// the run reads back whole, in order, across both.
	#[tokio::test]
	async fn a_run_past_the_local_capacity_overflows_and_reads_back_whole() {
		let local = Arc::new(MemTempStore::new(4, None));
		let overflow = Arc::new(MemTempStore::new(4, None));
		let store = TieredTempStore::new(
			Arc::clone(&local) as Arc<dyn TempStore>,
			8,
			Arc::clone(&overflow) as Arc<dyn TempStore>,
		);
		let mut run = store.write_run().await.expect("a run");
		for part in [b"aaaa", b"bbbb", b"cccc", b"dddd"] {
			run.write_part(part.to_vec()).await.expect("a part");
		}
		let id = run.finish().await.expect("finished");
		assert!(store.overflowed(id));
		assert_eq!(local.bytes(), 8);
		assert_eq!(overflow.bytes(), 8);
		let mut read = store.read_run(id).await.expect("a reader");
		let mut got = Vec::new();
		while let Some(part) = read.next_part().await.expect("a part") {
			got.extend(part);
		}
		assert_eq!(got, b"aaaabbbbccccdddd");
		store.delete_run(id);
		assert_eq!(store.local_bytes(), 0);
		assert_eq!(local.runs() + overflow.runs(), 0);
	}
}
