//! What a program holds across instructions, charged to its query's lease.
//!
//! A value a program binds, copies into a call frame, iterates or caches
//! cannot be spilled, so it is pinned memory of the query: it counts toward the
//! query's ceiling and the governor's accounting while the program holds it.
//! A program keeps values in a fixed set of places on its [`State`], and each is
//! a [`Site`]: charging at the sites a value is kept covers every value
//! whatever built it, where listing the builders could never be complete.
//!
//! A [`Ledger`] holds one pinned account and the bytes each site is charged,
//! and settles the account to their total whenever a site changes. It is made
//! on the first charge of a program that keeps a value with a heap, so a
//! program that keeps only scalars allocates nothing. A total that fits in one
//! reservation chunk is counted exactly toward the query's ceiling and
//! reserves nothing from the lease ([`Reservation::settle`]), so the many
//! frames of small calls neither open the lease nor round each total up to a
//! chunk. It releases everything when its state ends.

use std::sync::Arc;

use super::interp::State;
use crate::exec::memory::size::kept_bytes;
use crate::exec::memory::{QueryMemory, Reservation, Verdict};
use crate::exec::{ExecutionContext, FlowResult};

/// A place a program keeps a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Site {
	/// The parameter slot a `LET` or `FOR` name is bound to.
	Slot(usize),
	/// The copies of the values bound to [`Slot`](Self::Slot) that the frames
	/// hold, which last until the block that bound them ends. A frame is a child
	/// of the one before it, so a name bound again leaves its earlier copy held:
	/// every binding is charged, and the charges add.
	Frame(usize),
	/// The source array of the `FOR` loop on an iterator slot, which the
	/// iterator holds until the loop ends.
	Iter(usize),
	/// The value a nested statement run once has kept.
	Once(usize),
}

/// The bytes a program keeps at each of its sites, settled against one pinned
/// account of the program's query.
pub(super) struct Ledger {
	account: Reservation,
	slots: Vec<usize>,
	frames: Vec<usize>,
	iters: Vec<usize>,
	once: Vec<usize>,
	total: usize,
	/// What [`Self::frames`] charged where each open block began, by the depth
	/// of the saved frames then, for those that began charging some.
	saved: Vec<(usize, Vec<usize>)>,
}

impl Ledger {
	/// A ledger holding nothing, drawing on `memory`.
	fn new(memory: &Arc<QueryMemory>) -> Self {
		Self {
			account: memory.pinned_reservation(),
			slots: Vec::new(),
			frames: Vec::new(),
			iters: Vec::new(),
			once: Vec::new(),
			total: 0,
			saved: Vec::new(),
		}
	}

	/// A ledger holding `slots`, the bytes another state has charged to its
	/// slots, whose values this state's slots are copies of; and whether the
	/// lease covers them.
	fn copying(memory: &Arc<QueryMemory>, slots: Vec<usize>) -> (Self, Verdict) {
		let mut ledger = Self::new(memory);
		ledger.total = slots.iter().sum();
		ledger.slots = slots;
		let verdict = ledger.account.settle(ledger.total);
		(ledger, verdict)
	}

	/// The bytes each slot is charged.
	pub(super) fn slot_bytes(&self) -> &[usize] {
		&self.slots
	}

	/// The bytes charged at `site`, which the ledger holds afresh.
	fn cell(&mut self, site: Site) -> &mut usize {
		let (cells, at) = match site {
			Site::Slot(at) => (&mut self.slots, at),
			Site::Frame(at) => (&mut self.frames, at),
			Site::Iter(at) => (&mut self.iters, at),
			Site::Once(at) => (&mut self.once, at),
		};
		if cells.len() <= at {
			cells.resize(at + 1, 0);
		}
		&mut cells[at]
	}

	/// Charge `bytes` at `site` in place of what it was charged, and answer
	/// whether the lease covers the total.
	fn set(&mut self, site: Site, bytes: usize) -> Verdict {
		self.put(site, bytes);
		self.account.settle(self.total)
	}

	/// Note `bytes` charged at `site` in place of what it was, leaving the
	/// account to be settled to [`Self::total`] by the caller.
	fn put(&mut self, site: Site, bytes: usize) {
		let cell = self.cell(site);
		let before = std::mem::replace(cell, bytes);
		self.total = self.total - before + bytes;
	}

	/// Charge `bytes` more at `site`, in addition to what it was charged, and
	/// answer whether the lease covers the total.
	fn add(&mut self, site: Site, bytes: usize) -> Verdict {
		*self.cell(site) += bytes;
		self.total += bytes;
		self.account.settle(self.total)
	}

	/// Note the frame charges as the block at saved-frame `depth` begins.
	fn save_frames(&mut self, depth: usize) {
		self.saved.retain(|(at, _)| *at < depth);
		if self.frames.iter().any(|bytes| *bytes != 0) {
			self.saved.push((depth, self.frames.clone()));
		}
	}

	/// Charge the frames as they were when the block at saved-frame `depth`
	/// began, as it ends: the copies it bound are gone and those bound before
	/// it remain. A block that noted nothing began with no frame charged.
	fn restore_frames(&mut self, depth: usize) -> Verdict {
		let noted = self.saved.last().is_some_and(|(at, _)| *at == depth);
		let before = if noted {
			self.saved.pop().map(|(_, frames)| frames).unwrap_or_default()
		} else {
			Vec::new()
		};
		let mut changed = false;
		for at in 0..self.frames.len().max(before.len()) {
			let bytes = before.get(at).copied().unwrap_or(0);
			let cell = self.cell(Site::Frame(at));
			if *cell != bytes {
				let was = std::mem::replace(cell, bytes);
				self.total = self.total - was + bytes;
				changed = true;
			}
		}
		if changed {
			self.account.settle(self.total)
		} else {
			Verdict::Hold
		}
	}
}

impl State {
	/// Charge `bytes` at `site` as what the program now keeps there, in place
	/// of what it kept. Waits, or fails, as [`Reservation::insist`] does where
	/// the lease does not cover the total. A state that has charged nothing and
	/// keeps nothing here allocates nothing.
	pub(super) async fn keep(
		&mut self,
		site: Site,
		bytes: usize,
		ctx: &ExecutionContext,
	) -> FlowResult<()> {
		let Some(ledger) = self.ledger(bytes != 0, ctx) else {
			return Ok(());
		};
		if ledger.set(site, bytes) == Verdict::Spill {
			ledger.account.insist(ctx).await?;
		}
		Ok(())
	}

	/// Charge `bytes` more at `site`, in addition to what the program kept
	/// there, as [`keep`](Self::keep) does otherwise.
	pub(super) async fn keep_more(
		&mut self,
		site: Site,
		bytes: usize,
		ctx: &ExecutionContext,
	) -> FlowResult<()> {
		let Some(ledger) = self.ledger(bytes != 0, ctx) else {
			return Ok(());
		};
		if ledger.add(site, bytes) == Verdict::Spill {
			ledger.account.insist(ctx).await?;
		}
		Ok(())
	}

	/// [`keep`](Self::keep) for the slots a call enters with: each is charged
	/// what its value weighs.
	pub(super) async fn keep_slots(&mut self, ctx: &ExecutionContext) -> FlowResult<()> {
		if !self.charge_slots(ctx) {
			return Ok(());
		}
		if let Some(ledger) = self.values.as_mut() {
			ledger.account.insist(ctx).await?;
		}
		Ok(())
	}

	/// [`keep_slots`](Self::keep_slots) for a caller that cannot suspend: the
	/// charge is granted without waiting ([`Reservation::insist_now`]).
	pub(super) fn keep_slots_now(&mut self, ctx: &ExecutionContext) -> FlowResult<()> {
		if !self.charge_slots(ctx) {
			return Ok(());
		}
		match self.values.as_mut() {
			Some(ledger) => ledger.account.insist_now(),
			None => Ok(()),
		}
	}

	/// Charge each of the state's slots what its value weighs, and answer
	/// whether the lease does not cover the total.
	fn charge_slots(&mut self, ctx: &ExecutionContext) -> bool {
		let Some(memory) = ctx.ctx().memory() else {
			return false;
		};
		let mut touched = false;
		for (at, value) in self.slots.iter().enumerate() {
			let bytes = kept_bytes(value);
			if bytes == 0 && self.values.is_none() {
				continue;
			}
			let ledger = self.values.get_or_insert_with(|| {
				let mut ledger = Ledger::new(memory);
				ledger.slots.reserve_exact(self.slots.len());
				ledger
			});
			ledger.put(Site::Slot(at), bytes);
			touched = true;
		}
		match self.values.as_mut() {
			Some(ledger) if touched => ledger.account.settle(ledger.total) == Verdict::Spill,
			_ => false,
		}
	}

	/// Note, as a block begins, what the frames hold, for
	/// [`restore_frames`](Self::restore_frames). `depth` is the number of frames
	/// saved, the block's included.
	pub(super) fn save_frames(&mut self, depth: usize) {
		if let Some(ledger) = self.values.as_mut() {
			ledger.save_frames(depth);
		}
	}

	/// Stop charging the frame copies bound since the block at saved-frame
	/// `depth` began, as it ends. Waits, or fails, as [`Self::keep`] does where
	/// the lease does not cover the total.
	pub(super) async fn restore_frames(
		&mut self,
		depth: usize,
		ctx: &ExecutionContext,
	) -> FlowResult<()> {
		if let Some(ledger) = self.values.as_mut()
			&& ledger.restore_frames(depth) == Verdict::Spill
		{
			ledger.account.insist(ctx).await?;
		}
		Ok(())
	}

	/// What the state's slots are charged, for a copy of the slots that is
	/// charged the same ([`Self::keep_copied`]). The frames' copies are not
	/// part of it: a copy of the slots shares the frames.
	pub(super) fn slot_charges(&self) -> Vec<usize> {
		self.values.as_ref().map(|ledger| ledger.slot_bytes().to_vec()).unwrap_or_default()
	}

	/// Charge the state's slots as the copies they are of slots charged `bytes`:
	/// the memory is a copy's, so the sizes are not measured again.
	pub(super) async fn keep_copied(
		&mut self,
		bytes: Vec<usize>,
		ctx: &ExecutionContext,
	) -> FlowResult<()> {
		if bytes.iter().all(|bytes| *bytes == 0) {
			return Ok(());
		}
		let Some(memory) = ctx.ctx().memory() else {
			return Ok(());
		};
		let (mut ledger, verdict) = Ledger::copying(memory, bytes);
		if verdict == Verdict::Spill {
			ledger.account.insist(ctx).await?;
		}
		self.values = Some(ledger);
		Ok(())
	}

	/// Stop charging `site`: what was kept there is gone.
	pub(super) fn release(&mut self, site: Site) {
		if let Some(ledger) = self.values.as_mut() {
			ledger.set(site, 0);
		}
	}

	/// Let go of the value nested statement `at` kept, and its charge.
	pub(super) fn reset_once(&mut self, at: usize) {
		self.once[at] = None;
		self.release(Site::Once(at));
	}

	/// The ledger the state charges to, made if `needed` and the context has
	/// the query's memory, and taking over the account of a value handed on to
	/// it ([`Self::handed`]). `None` where there is nothing to charge to or for.
	fn ledger(&mut self, needed: bool, ctx: &ExecutionContext) -> Option<&mut Ledger> {
		if self.values.is_none() && !needed && self.handed.is_none() {
			return None;
		}
		let memory = ctx.ctx().memory()?;
		let ledger = self.values.get_or_insert_with(|| Ledger::new(memory));
		if let Some(handed) = self.handed.take() {
			ledger.account.absorb(handed);
		}
		Some(ledger)
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::*;
	use crate::exec::memory::{FairGovernor, MemoryHost, MemorySettings};

	/// A query's memory over a node with room to spare, in chunks of 64 bytes.
	fn memory() -> Arc<QueryMemory> {
		let host = MemoryHost {
			governor: Arc::new(FairGovernor::new(1 << 20)),
			store: None,
			settings: MemorySettings {
				floor: 100,
				fan_in: 2,
				chunk: 64,
				wait: Duration::ZERO,
				max: usize::MAX,
			},
		};
		host.query()
	}

	/// Every binding that a frame holds for one slot is charged, and a block's
	/// ending gives back those it made while the bindings from before it stay
	/// charged.
	#[test]
	fn frame_bindings_of_a_slot_add_and_end_with_their_block() {
		let memory = memory();
		let mut ledger = Ledger::new(&memory);
		ledger.add(Site::Frame(0), 100);
		ledger.save_frames(1);
		ledger.add(Site::Frame(0), 100);
		ledger.add(Site::Frame(0), 100);
		assert_eq!(ledger.total, 300);
		assert_eq!(memory.pinned(), 300);
		ledger.restore_frames(1);
		assert_eq!(ledger.total, 100);
		assert_eq!(memory.pinned(), 100);
		// A slot is charged what its value weighs now, not what it weighed.
		ledger.set(Site::Slot(0), 500);
		ledger.set(Site::Slot(0), 40);
		assert_eq!(ledger.total, 140);
		assert_eq!(memory.pinned(), 140);
	}
}
