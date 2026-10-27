//! The write barrier: a wait until every write transaction that opened, on any node writing the
//! store, before a moment has ended.
//!
//! An index build reads the table through transactions of its own, and a write maintains an index,
//! or queues for its build, only as the snapshot it opened with shows the index. A build therefore
//! reads only once every write transaction whose snapshot predates the commit it builds against —
//! the definition of a new index, or the generation a rebuild installs — has ended: each such write
//! has then committed, and the build's own snapshot holds it, or has not committed at all. A write
//! that opens after that commit sees the index building and queues for it.
//!
//! # One process
//!
//! A datastore counts its write transactions by the epoch they opened in
//! ([`surrealdb_datastore::triggers::LiveWrites`]), each counted in before its snapshot is taken
//! and out once its commit or cancel has returned. A wait reads the epoch and polls until every
//! transaction of that epoch and earlier has ended. On a store the datastore writes alone
//! ([`Transaction::sole_writer`]) that is the whole barrier.
//!
//! # Several nodes
//!
//! On a store other nodes write too, each node answers for its own transactions, through two keys:
//!
//! - `!wb` ([`WriteBarrierKey`]), the newest barrier asked for. A build raises it once its commit
//!   has landed. Only builds write it, never a record write.
//! - `!wa{nd}` ([`WriteBarrierAckKey`]), the newest barrier node `nd` has answered. Only node `nd`
//!   writes it, so no node's writes contend with another's.
//!
//! A node answers a barrier ([`Datastore::answer_write_barrier`]) by reading `!wb`, waiting for its
//! own write transactions older than that read to end, and recording `!wa`. The barrier was raised
//! after the build's commit, and the node read it after that, so every transaction of the node's
//! whose snapshot predates the commit is among those it waited for. A node records its answer to
//! the current barrier when it registers, before it opens a transaction of its own, so a node that
//! joins later is never waited for.
//!
//! On a store that fences catalog changes ([`Transaction::fences_catalog_changes`]) a build
//! that reads against a commit stamping its table's catalog version asks no other node: every
//! write of a table's records reads that table's version as a locked read first, so a write
//! whose snapshot predates the stamp either committed before it, and the build, which reads
//! after it, finds the write, or fails its commit. A build of a generation the statement that
//! started it staged, and that reads once that statement's transaction has closed, reads against
//! such a commit: `DEFINE INDEX` and `REBUILD INDEX` stamp the table. It asks no other node only
//! while every other live node's current heartbeat carries the mark of a release with the fence
//! (`!wf{nd}`, [`CatalogFenceHeartbeatKey`]), which a node records beside its barrier mark when
//! its own writes are fenced: a node last refreshed by a release without the fence reads catalog
//! versions plainly, and its writes conflict with no stamp. Every other build, a build that reads
//! inside the transaction that defines its index above all, still raises the barrier.
//!
//! The build waits for every node other than its own whose heartbeat is within
//! [`exclude_after`] to answer, archived or not, and for its own node's transactions
//! directly. A release with the
//! barrier marks each heartbeat it records (`!wh{nd}`, [`WriteBarrierHeartbeatKey`]), so a node
//! whose current heartbeat carries no mark was last refreshed by a release without it. A node whose
//! heartbeat is older is left out: a datastore that answers barriers refuses its own write commits
//! once its heartbeat has lapsed by most of that window
//! ([`surrealdb_datastore::triggers::WriteFence`]), so it cannot commit a write the build missed.
//! The nodes' clocks must agree within the margin between the two, as for every lease between
//! nodes, and a commit admitted before the fence lapsed must land within what is left of it. A live
//! node that has never answered a barrier, or whose heartbeat carries no mark, runs a
//! release without it, and the build then proceeds without waiting, as that release's builds do.

use std::time::Duration;

use anyhow::{Result, ensure};
use surrealdb_datastore::triggers::{DefinitionHold, FencedTable};
use surrealdb_kvs::key::Resumable;
use uuid::Uuid;

use crate::catalog::providers::NodeProvider;
use crate::key::KVKeyDecode;
use crate::key::schema::{
	CatalogFenceHeartbeatKey, CatalogFenceHeartbeatPrefix, WriteBarrierAckKey,
	WriteBarrierAckPrefix, WriteBarrierHeartbeatKey, WriteBarrierHeartbeatPrefix, WriteBarrierKey,
	WriteBarrierTargetKey, WriteBarrierTargetSeqPrefix,
};
use crate::kvs::clock::SystemClock;
use crate::kvs::sequences::Sequences;
use crate::kvs::{
	Datastore, Direction, Transaction, TransactionFactory, TransactionType,
	is_retryable_transaction_conflict,
};

/// How long a datastore that answers write barriers may go without recording a heartbeat before
/// it refuses its own write commits, dated from before the heartbeat read the clock it records.
/// Within [`exclude_after`] it leaves a margin within which the nodes' clocks must
/// agree and a commit admitted before the fence lapsed must land. A heartbeat that takes longer
/// than this to commit lapses the fence: a node whose storage cannot keep its liveness refuses its
/// writes, which is what lets a build leave it out.
pub(crate) const WRITE_FENCE_STALE_AFTER: Duration = Duration::from_secs(30);

/// The longest node membership refresh interval a datastore writing a store other nodes write
/// may run with: its write fence ([`WRITE_FENCE_STALE_AFTER`]) must see at least two heartbeats
/// within its window, or the node would refuse its own writes between healthy heartbeats.
pub(crate) const MAX_SHARED_STORE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// The margin within which the nodes' clocks must agree, as node leases already assume.
pub(crate) const WRITE_BARRIER_CLOCK_MARGIN: Duration = Duration::from_secs(10);

/// How old a node's heartbeat must be before a build stops waiting for its answer, archived or
/// not, on a store whose write commits are bounded by `commit_bound`
/// ([`Transaction::write_commit_bound`]). A node refuses to admit a write commit once its
/// heartbeat is [`WRITE_FENCE_STALE_AFTER`] old; a commit it admitted returns within
/// `commit_bound`, or is abandoned and has landed, or never will, within
/// [`ABANDONED_COMMIT_SETTLE`](surrealdb_datastore::triggers::ABANDONED_COMMIT_SETTLE) more. So
/// this is their sum and [`WRITE_BARRIER_CLOCK_MARGIN`], and no write a node left out of a build's
/// wait admitted can land after it is left out. A node that shuts down drains its writes and
/// answers every barrier before it archives itself ([`Datastore::retire_write_barrier_answer`]),
/// so a clean exit is not waited for.
pub(crate) fn exclude_after(commit_bound: Duration) -> Duration {
	WRITE_FENCE_STALE_AFTER
		+ commit_bound
		+ surrealdb_datastore::triggers::ABANDONED_COMMIT_SETTLE
		+ WRITE_BARRIER_CLOCK_MARGIN
}

/// How long a wait waits for this datastore's write transactions older than it before it fences
/// them: a transaction still open then is refused at commit, so a client holding a write
/// transaction open cannot stall every index build. A commit under way when the wait gives up
/// is waited for.
pub(crate) const WRITE_BARRIER_WAIT: Duration = Duration::from_secs(60);

/// The longest a heartbeat may run on a datastore that answers write barriers, the read of the
/// feature level that follows it included. The fence lapses [`WRITE_FENCE_STALE_AFTER`] after
/// the start of the last heartbeat that committed, and the next one starts at most the larger of
/// the refresh interval and this bound after that start, then commits within this bound again: so
/// two heartbeats in a row, each within its bound, fit in the fence's life, with a margin for
/// scheduling. One that cannot commit in time leaves the node refusing its writes.
pub(crate) const HEARTBEAT_ATTEMPT_BOUND: Duration = Duration::from_secs(14);

/// Two heartbeats in a row, each within [`HEARTBEAT_ATTEMPT_BOUND`] and started at most
/// [`MAX_SHARED_STORE_HEARTBEAT_INTERVAL`] apart, rearm the write fence before it lapses.
const _: () = {
	let interval = MAX_SHARED_STORE_HEARTBEAT_INTERVAL.as_secs();
	let bound = HEARTBEAT_ATTEMPT_BOUND.as_secs();
	let spacing = if interval > bound {
		interval
	} else {
		bound
	};
	assert!(spacing + bound < WRITE_FENCE_STALE_AFTER.as_secs());
};

/// How many of the newest write barriers keep the table they were raised for
/// ([`WriteBarrierTargetKey`]). A node answering barriers further back than this cannot tell which
/// tables they are on, and fences every table if its wait runs its bound.
const KEPT_TARGETS: u64 = 1024;

/// How often a node checks for a barrier to answer.
pub(crate) const WRITE_BARRIER_POLL: Duration = Duration::from_millis(250);

/// How often a wait polls the write transactions it waits for.
const DRAIN_POLL: Duration = Duration::from_millis(10);

/// A wait, begun by [`WriteBarrier::start`], until every write transaction that opened before it
/// began has ended on every live node.
pub(crate) struct WriteBarrier {
	/// The epoch of this datastore's write transactions the wait waits through.
	since: u64,
	/// When the wait began, which bounds how long it waits for this datastore's transactions
	/// ([`WRITE_BARRIER_WAIT`]).
	began: web_time::Instant,
	/// The barrier every other live node must answer, or `None` on a store this datastore writes
	/// alone, or once a live node without the barrier was found.
	asked: Option<u64>,
	/// This node, which the wait answers for directly rather than through its answer.
	node: Uuid,
	/// The table whose index the build builds: the only one whose writes a wait that has run
	/// its bound refuses.
	target: FencedTable,
	/// Counts the wait, on `target`, as running for as long as it lives, for a transaction set
	/// aside while it runs ([`surrealdb_datastore::triggers::LiveWrites::begin_wait`]).
	_running: surrealdb_datastore::factory::WriteWait,
}

impl WriteBarrier {
	/// Begin a wait for the write transactions that opened before now. On a store other nodes
	/// write too, this raises the barrier every live node must answer, unless the build reads
	/// against a commit that stamped its table's catalog version (`stamped`) on a store that
	/// fences catalog changes ([`Transaction::fences_catalog_changes`]): the wait is then for
	/// this datastore's own transactions alone (see the module documentation).
	///
	/// A build that reads before its definition commits, inside the transaction that defines the
	/// index, passes that transaction's `hold`, which begins with the wait: the writes of the table
	/// that open after it cannot see the index, and are held back until the transaction ends
	/// ([`surrealdb_datastore::triggers::DefinitionHold`]).
	pub(crate) async fn start(
		tf: &TransactionFactory,
		sequences: &Sequences,
		node: Uuid,
		target: FencedTable,
		hold: Option<&DefinitionHold>,
		stamped: bool,
	) -> Result<Self> {
		// Counted before the epoch is read, so a transaction set aside after that read sees it.
		let running = tf.begin_write_wait(Some(std::slice::from_ref(&target)));
		let since = tf.live_writes().advance_holding(hold);
		let tx = tf.transaction(TransactionType::Read, sequences.clone()).await?;
		let local_only = if tx.sole_writer() {
			true
		} else if stamped && tx.fences_catalog_changes() {
			catch!(tx, every_live_node_fences(&tx, node).await)
		} else {
			false
		};
		tx.cancel().await?;
		let asked = if local_only {
			None
		} else {
			Some(raise(tf, sequences, &target).await?)
		};
		Ok(Self {
			since,
			began: web_time::Instant::now(),
			asked,
			node,
			target,
			_running: running,
		})
	}

	/// Whether the wait is over: every write transaction of this datastore that opened before it
	/// began has ended, and every other live node has answered its barrier. A caller polls this,
	/// sleeping [`Self::poll`] between calls.
	pub(crate) async fn done(
		&mut self,
		tf: &TransactionFactory,
		sequences: &Sequences,
	) -> Result<bool> {
		if !local_writes_settled(
			tf.live_writes(),
			self.since,
			self.began,
			self.node,
			Some(std::slice::from_ref(&self.target)),
		) {
			return Ok(false);
		}
		let Some(asked) = self.asked else {
			return Ok(true);
		};
		let tx = tf.transaction(TransactionType::Read, sequences.clone()).await?;
		let answered = catch!(tx, answers(&tx, self.node, asked).await);
		tx.cancel().await?;
		match answered {
			Answers::All => Ok(true),
			Answers::Pending => Ok(false),
			Answers::Unsupported(nd) => {
				warn!(
					target: "surrealdb::core::kvs::write_barrier",
					node = %nd,
					"a live node runs a release without the write barrier, so an index build reads \
					 without waiting for the write transactions older than its commit"
				);
				self.asked = None;
				Ok(true)
			}
		}
	}

	/// How long a caller sleeps between polls of [`Self::done`].
	pub(crate) fn poll(&self) -> Duration {
		match self.asked {
			Some(_) => WRITE_BARRIER_POLL,
			None => DRAIN_POLL,
		}
	}
}

/// Whether every write transaction of this datastore counted into `since` or an earlier epoch has
/// ended, or, once the wait that began at `began` has run [`WRITE_BARRIER_WAIT`], has been fenced
/// and has no commit under way.
fn local_writes_settled(
	live: &surrealdb_datastore::triggers::LiveWrites,
	since: u64,
	began: web_time::Instant,
	node: Uuid,
	targets: Option<&[FencedTable]>,
) -> bool {
	// An earlier wait on the same tables that ran its bound has fenced the epochs it covered, so
	// their transactions hold this one up no longer either.
	if live.ended_since_for(since, targets) {
		return true;
	}
	if began.elapsed() < wait_bound(node) {
		return false;
	}
	// Only the writes of the tables the wait is on are refused; one that cannot tell which
	// tables those are refuses them all.
	match targets {
		Some(targets) => live.fence_tables_through(since, targets),
		None => live.fence_through(since),
	}
	live.commits_settled(since)
}

/// [`WRITE_BARRIER_WAIT`], or a test's shorter bound for `node`.
fn wait_bound(#[allow(unused_variables)] node: Uuid) -> Duration {
	#[cfg(test)]
	if let Some(bound) = test_wait::bound(node) {
		return bound;
	}
	WRITE_BARRIER_WAIT
}

/// How the other live nodes stand with a barrier.
enum Answers {
	/// Every one has answered it.
	All,
	/// One has not answered it yet.
	Pending,
	/// One has never answered a barrier, so it runs a release without them.
	Unsupported(Uuid),
}

/// Whether every live node other than `node` last recorded its heartbeat as a release whose writes
/// the store fences against a later change to their table's catalog (`!wf`,
/// [`CatalogFenceHeartbeatKey`]). A node whose heartbeat is older than
/// [`exclude_after`] is left out, as a build's wait leaves it out ([`answers`]).
async fn every_live_node_fences(tx: &Transaction, node: Uuid) -> Result<bool> {
	let now = SystemClock::new().now();
	let excluded_before = now - exclude_after(tx.write_commit_bound());
	let mut marks = std::collections::HashMap::new();
	for (key, value) in tx.scan(CatalogFenceHeartbeatPrefix {}.range()?, u32::MAX, 0, None).await? {
		marks.insert(CatalogFenceHeartbeatKey::decode_key(&key)?.nd, value);
	}
	for nd in tx.all_nodes().await?.iter() {
		if nd.id == node || nd.heartbeat < excluded_before {
			continue;
		}
		// A node last refreshed by a release without the fence reads catalog versions plainly.
		if marks.get(&nd.id) != Some(&nd.heartbeat.value) {
			return Ok(false);
		}
	}
	Ok(true)
}

async fn answers(tx: &Transaction, node: Uuid, asked: u64) -> Result<Answers> {
	let now = SystemClock::new().now();
	let excluded_before = now - exclude_after(tx.write_commit_bound());
	let mut acks = std::collections::HashMap::new();
	for (key, value) in tx.scan(WriteBarrierAckPrefix {}.range()?, u32::MAX, 0, None).await? {
		acks.insert(WriteBarrierAckKey::decode_key(&key)?.nd, value);
	}
	let mut beats = std::collections::HashMap::new();
	for (key, value) in tx.scan(WriteBarrierHeartbeatPrefix {}.range()?, u32::MAX, 0, None).await? {
		beats.insert(WriteBarrierHeartbeatKey::decode_key(&key)?.nd, value);
	}
	let mut pending = false;
	for nd in tx.all_nodes().await?.iter() {
		if nd.id == node || nd.heartbeat < excluded_before {
			continue;
		}
		// A node last refreshed by a release without the barrier answers none.
		if beats.get(&nd.id) != Some(&nd.heartbeat.value) {
			return Ok(Answers::Unsupported(nd.id));
		}
		match acks.get(&nd.id) {
			None => return Ok(Answers::Unsupported(nd.id)),
			Some(ack) if *ack < asked => pending = true,
			Some(_) => {}
		}
	}
	Ok(if pending {
		Answers::Pending
	} else {
		Answers::All
	})
}

/// Raise `!wb` to a barrier of this build's own, past every barrier asked before this call, and
/// answer it.
///
/// Every raise takes a barrier no other build shares, so this node's answer to a barrier only
/// its own builds raised is known to pass no transaction a build elsewhere waits for
/// ([`surrealdb_datastore::triggers::LiveWrites::answers_only_own`]).
async fn raise(
	tf: &TransactionFactory,
	sequences: &Sequences,
	target: &FencedTable,
) -> Result<u64> {
	// Held until the barrier is recorded as this datastore's own, so no answer reads it first.
	let _raising = tf.live_writes().lock_raises().await;
	loop {
		let tx = tf.transaction(TransactionType::Write, sequences.clone()).await?;
		let current = catch!(tx, tx.get_key(&WriteBarrierKey {}, None).await);
		let next = current.unwrap_or(0).saturating_add(1);
		catch!(tx, tx.put_compare_key(&WriteBarrierKey {}, &next, current.as_ref()).await);
		// The barrier names the table it is raised for, beside a bounded run of the newest ones.
		let (ns, db, tb) = target;
		catch!(
			tx,
			tx.set_key(
				&WriteBarrierTargetKey {
					seq: next,
					ns: *ns,
					db: *db,
					tb: std::borrow::Cow::Borrowed(tb),
				},
				&()
			)
			.await
		);
		if let Some(pruned) = next.checked_sub(KEPT_TARGETS) {
			catch!(
				tx,
				tx.delr(
					WriteBarrierTargetSeqPrefix {
						seq: pruned,
					}
					.range()?
				)
				.await
			);
		}
		match tx.commit().await {
			Ok(()) => {
				tf.live_writes().note_own_barrier(next);
				return Ok(next);
			}
			Err(err) if is_retryable_transaction_conflict(&err) => continue,
			Err(err) => return Err(err),
		}
	}
}

impl Datastore {
	/// Refuse a node membership refresh `interval` the write fence cannot run with: above
	/// [`MAX_SHARED_STORE_HEARTBEAT_INTERVAL`], on a store other nodes write. A store this
	/// datastore writes alone takes no fence, and a read-only datastore arms none, so any interval
	/// serves them.
	pub(crate) async fn check_heartbeat_interval(&self, interval: Duration) -> Result<()> {
		// A read-only datastore commits no writes, and arms no fence.
		if interval <= MAX_SHARED_STORE_HEARTBEAT_INTERVAL || self.is_read_only() {
			return Ok(());
		}
		let tx = self.transaction(TransactionType::Read).await?;
		let sole_writer = tx.sole_writer();
		tx.cancel().await?;
		ensure!(
			sole_writer,
			"the node membership refresh interval is {interval:?}, but on a store other nodes \
			 write it can be at most {MAX_SHARED_STORE_HEARTBEAT_INTERVAL:?}: a node refuses its own \
			 writes once {WRITE_FENCE_STALE_AFTER:?} pass without a heartbeat"
		);
		Ok(())
	}

	/// Answer write barriers from now on: arm the write fence, which refuses this datastore's
	/// write commits once its heartbeat lapses, and record with each heartbeat, and as this node
	/// registers, that it answers them ([`Self::advertises_write_barrier`]). Called as the task
	/// that answers them starts. A datastore that never calls it records nothing, and every build
	/// takes it for a node that predates the barrier, reading without waiting for it.
	pub(crate) fn begin_answering_write_barriers(&self) {
		self.transaction_factory()
			.write_fence()
			.heartbeat(web_time::Instant::now(), WRITE_FENCE_STALE_AFTER);
		self.answers_write_barriers.store(true, std::sync::atomic::Ordering::SeqCst);
	}

	/// Record, before this registered node serves writes, that it answers write barriers: start
	/// answering them ([`Self::begin_answering_write_barriers`]), answer the current one as the
	/// responder does — once this node's write transactions older than it have ended — and record
	/// a heartbeat marked as one from a node that answers them. Until both are recorded, a build
	/// elsewhere takes this node for one that predates the barrier, and would read past a write it
	/// holds open. A read-only datastore, or a build without `maintenance-tasks`, whose node
	/// answers no barrier, records nothing more.
	pub(crate) async fn publish_write_barrier_support(&self) -> Result<()> {
		if self.is_read_only() || !cfg!(feature = "maintenance-tasks") {
			return Ok(());
		}
		self.begin_answering_write_barriers();
		self.answer_write_barrier(&self.shutdown).await?;
		self.update_node().await
	}

	/// Whether this node records that it answers write barriers: one that answers them
	/// ([`Self::begin_answering_write_barriers`]), or a read-only one, which commits no writes
	/// and so answers every barrier there will be.
	pub(crate) fn advertises_write_barrier(&self) -> bool {
		self.is_read_only() || self.answers_write_barriers.load(std::sync::atomic::Ordering::SeqCst)
	}

	/// Record, in `txn`, this node's answer to the current write barrier, as it registers: the
	/// node has opened no transaction of its own yet, so it answers every barrier asked so far. A
	/// read-only datastore commits no writes, so it answers every barrier there will be.
	pub(crate) async fn register_write_barrier_answer(&self, txn: &Transaction) -> Result<()> {
		let answer = if self.is_read_only() {
			u64::MAX
		} else {
			txn.get_key(&WriteBarrierKey {}, None).await?.unwrap_or(0)
		};
		txn.set_key(
			&WriteBarrierAckKey {
				nd: self.id(),
			},
			&answer,
		)
		.await
	}

	/// Answer every write barrier there will be, as a node shutting down does once its writes
	/// have drained and its write fence refuses any more: no build waits for it from then on.
	pub(crate) async fn retire_write_barrier_answer(&self) -> Result<()> {
		let tx = self.transaction(TransactionType::Write).await?;
		// The fence already refuses this node's writes; this one records that none are left.
		tx.exempt_from_write_fence();
		catch!(
			tx,
			tx.set_key(
				&WriteBarrierAckKey {
					nd: self.id(),
				},
				&u64::MAX
			)
			.await
		);
		tx.commit().await
	}

	/// Answer the newest write barrier, if this node has not, or record an answer again where this
	/// node's is missing: wait until every write transaction this datastore opened before reading
	/// the barrier has ended, then record the answer. Answers whether it recorded one.
	pub(crate) async fn answer_write_barrier(
		&self,
		canceller: &tokio_util::sync::CancellationToken,
	) -> Result<bool> {
		let live = self.transaction_factory().live_writes();
		// Read, and judged, while no raise through this datastore is between its commit and its
		// record as this datastore's own.
		let raises = live.lock_raises().await;
		let tx = self.transaction(TransactionType::Read).await?;
		let asked = catch!(tx, tx.get_key(&WriteBarrierKey {}, None).await);
		let answered = catch!(
			tx,
			tx.get_key(
				&WriteBarrierAckKey {
					nd: self.id(),
				},
				None
			)
			.await
		);
		tx.cancel().await?;
		// A node whose answer is gone — removed with its row while it kept running — answers
		// again, or every build would take it for a node without the barrier.
		let asked = match (asked, answered) {
			(Some(asked), Some(answered)) if answered >= asked => return Ok(false),
			(None, Some(_)) => return Ok(false),
			(asked, _) => asked.unwrap_or(0),
		};
		// An answer to barriers only this node's builds raised passes no transaction a build
		// elsewhere waits for: each of those builds waits for this node's transactions directly.
		// Any other answer is a wait that may pass a transaction set aside for its own build, and
		// is counted as running from before it reads the epoch.
		let only_own = answered.is_some_and(|answered| live.answers_only_own(answered, asked));
		let own = match answered {
			Some(answered) => live.own_barriers_covered(answered, asked),
			None => std::collections::BTreeSet::new(),
		};
		drop(raises);
		// The tables the barriers this answer covers were raised for, and those of the ones other
		// nodes' builds raised; every table where one of them is no longer kept, or the answer
		// covers more than are kept.
		let (targets, others) = match answered {
			Some(answered) if asked.saturating_sub(answered) <= KEPT_TARGETS => {
				// The targets of `(answered, asked]`, one key per barrier, in order.
				let covered = WriteBarrierTargetSeqPrefix {
					seq: answered.saturating_add(1),
				}
				.range()?
				.resume_after(
					WriteBarrierTargetSeqPrefix {
						seq: asked,
					}
					.range()?
					.end()
					.as_slice(),
					Direction::Backward,
				);
				let tx = self.transaction(TransactionType::Read).await?;
				let keys = catch!(
					tx,
					tx.keys(covered, u32::try_from(KEPT_TARGETS).unwrap_or(u32::MAX), 0, None)
						.await
				);
				tx.cancel().await?;
				let mut seen = std::collections::BTreeSet::new();
				let mut tables: Vec<FencedTable> = Vec::new();
				let mut others: Vec<FencedTable> = Vec::new();
				for key in keys {
					let key = WriteBarrierTargetKey::decode_key(&key)?;
					if key.seq > answered && key.seq <= asked {
						seen.insert(key.seq);
						let table = (key.ns, key.db, key.tb.into_owned());
						if !own.contains(&key.seq) && !others.contains(&table) {
							others.push(table.clone());
						}
						if !tables.contains(&table) {
							tables.push(table);
						}
					}
				}
				(seen.len() as u64 == asked.saturating_sub(answered)).then_some((tables, others))
			}
			_ => None,
		}
		.unzip();
		// On the tables of the barriers other nodes' builds raised, the builds it lets read: this
		// node's own builds wait for its transactions themselves, through waits of their own on
		// their tables ([`WriteBarrier::start`]). On every table where those tables are unknown.
		let _running =
			(!only_own).then(|| self.transaction_factory().begin_write_wait(others.as_deref()));
		let since = live.advance();
		let began = web_time::Instant::now();
		while !local_writes_settled(live, since, began, self.id(), targets.as_deref()) {
			if canceller.is_cancelled() {
				return Ok(false);
			}
			common::time::sleep(DRAIN_POLL).await;
		}
		let tx = self.transaction(TransactionType::Write).await?;
		catch!(
			tx,
			tx.set_key(
				&WriteBarrierAckKey {
					nd: self.id(),
				},
				&asked
			)
			.await
		);
		tx.commit().await?;
		live.forget_own_barriers_through(asked);
		Ok(true)
	}
}

/// A shorter [`WRITE_BARRIER_WAIT`] per node, for the tests of what a wait that gives up does.
#[cfg(test)]
pub(crate) mod test_wait {
	use std::collections::HashMap;
	use std::sync::{Mutex, OnceLock};
	use std::time::Duration;

	use uuid::Uuid;

	fn bounds() -> &'static Mutex<HashMap<Uuid, Duration>> {
		static BOUNDS: OnceLock<Mutex<HashMap<Uuid, Duration>>> = OnceLock::new();
		BOUNDS.get_or_init(Default::default)
	}

	/// Bounds the waits of `node` by `bound` until dropped.
	pub(crate) struct Bounded(Uuid);

	impl Drop for Bounded {
		fn drop(&mut self) {
			bounds().lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(&self.0);
		}
	}

	pub(crate) fn bound_waits(node: Uuid, bound: Duration) -> Bounded {
		bounds().lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(node, bound);
		Bounded(node)
	}

	pub(super) fn bound(node: Uuid) -> Option<Duration> {
		bounds().lock().unwrap_or_else(std::sync::PoisonError::into_inner).get(&node).copied()
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::*;

	#[test]
	fn the_exclusion_window_covers_the_fence_the_commit_bound_the_settle_and_the_clock_margin() {
		let default = surrealdb_datastore::triggers::WRITE_COMMIT_BOUND;
		let longer = default + Duration::from_secs(30);
		assert_eq!(exclude_after(longer) - exclude_after(default), Duration::from_secs(30));
		assert_eq!(
			exclude_after(default),
			WRITE_FENCE_STALE_AFTER
				+ default + surrealdb_datastore::triggers::ABANDONED_COMMIT_SETTLE
				+ WRITE_BARRIER_CLOCK_MARGIN
		);
	}
}
