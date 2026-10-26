use std::time::Duration;

pub const NORMAL_BATCH_SIZE: u32 = 500;
pub const INDEXING_BATCH_SIZE: u32 = 250;
/// Most records one initial-scan batch of an index build reads, indexes and
/// commits in one transaction.
///
/// Every batch commits its index entries together with the build's
/// continuation checkpoint, so the per-batch cost (ownership fence, checkpoint
/// write, commit and its sync) is paid once per this many records. The byte
/// budget [`INDEXING_BATCH_MAX_BYTES`] bounds a batch of large documents
/// below this count.
pub const INDEXING_SCAN_BATCH_SIZE: u32 = 8192;
/// Soft cap on the raw record bytes an index build holds in memory per
/// initial-scan batch.
///
/// Batches are sized by record count, so without a byte budget a batch of
/// [`INDEXING_SCAN_BATCH_SIZE`] large documents can hold hundreds of megabytes at
/// once. The builder samples the record sizes it observes and shrinks the
/// next batch's count so it stays around this budget; small records keep
/// using full-size batches.
pub const INDEXING_BATCH_MAX_BYTES: usize = 8 * 1024 * 1024;
/// Record count of the first initial-scan batch, before any record sizes
/// have been observed. Deliberately small so a table of large documents
/// cannot spike memory before the byte budget kicks in; for small records it
/// only costs one extra scan round-trip.
pub const INDEXING_PROBE_BATCH_SIZE: u32 = 16;
pub const COUNT_BATCH_SIZE: u32 = 50_000;
/// Maximum number of index-compaction queue (`/!ic`) entries drained per
/// cycle iteration.
///
/// Every write transaction that mutates records under a compaction-eligible
/// index enqueues one entry per such index at commit, however many records
/// it writes, so the queue length is proportional to the number of committed
/// transactions touching such indexes since it last drained, and can reach
/// hundreds of thousands of entries under many small transactions when
/// draining is delayed. This bound caps both the queue scan and
/// the write transaction that deletes the drained entries, keeping the
/// per-transaction key cardinality small enough for distributed backends,
/// which reserve every written key on every replica at prepare time.
///
/// Bounds only the queue drain: a single index's compaction work is batched
/// separately by [`COUNT_BATCH_SIZE`], so one per-index compaction
/// transaction may carry several times more writes than one queue-drain
/// batch.
pub const INDEX_COMPACTION_QUEUE_BATCH_SIZE: u32 = 10_000;

/// Queue batches one index-compaction slice reads before it reports its
/// progress and yields.
///
/// A slice is the unit a driver runs between checking for cancellation and
/// other work: the datastore's own task runs slices back to back until the
/// queue is empty, and a host that drives maintenance itself runs one per
/// call. Sized so that a slice of full batches covers tens of thousands of
/// queue entries — several seconds of sustained indexed writes — while its
/// own scan work stays well under a second.
pub const INDEX_COMPACTION_SLICE_BATCHES: usize = 8;

/// Queue batches one graph-fold slice reads before it yields; see
/// [`INDEX_COMPACTION_SLICE_BATCHES`].
pub const GRAPH_FOLD_SLICE_BATCHES: usize = 8;

/// Conflict retries one graph-fold queue-cleanup commit makes — the write
/// that deletes a batch's drained queue entries — before the slice leaves the
/// entries queued and stops at that batch, reporting them as its backlog. A
/// later slice reaches them again, re-runs their scopes, which have nothing
/// pending but what was written since, and deletes them. Bounded so that
/// contention on the queue holds a slice for a known number of attempts
/// rather than until a commit wins.
pub const QUEUE_CLEANUP_CONFLICT_RETRIES: usize = 5;

/// Fold passes one graph-fold slice runs on one scope for one queue batch
/// that names it: each pass examines up to the configured fold batch of the
/// scope's adjacency keys, packable or not, in a transaction of its own, and
/// the next pass continues after the last key examined.
///
/// A queue batch bounds the entries a slice reads, not the folding they call
/// for: a scope's adjacency keys are whatever has accrued since it was last
/// folded, and a slice that ran every pass a scope needed would run for as
/// long as its backlog. A scope whose keys outlast its passes keeps its queue
/// entries, which the rotation reaches again once the later scopes have had
/// their passes, and the slice ends at that batch, reporting the entries it
/// kept as its backlog.
pub const GRAPH_FOLD_SLICE_PASSES_PER_SCOPE: usize = 8;

/// Fetches of up to [`NORMAL_BATCH_SIZE`] queued async events that find an
/// entry which one event-processing slice runs before it yields; see
/// [`INDEX_COMPACTION_SLICE_BATCHES`]. Every batch runs user-defined SurrealQL
/// of unbounded duration, so the slice is kept short.
pub const EVENT_PROCESSING_SLICE_BATCHES: usize = 4;

/// Pages of up to [`NORMAL_BATCH_SIZE`] durable RPC sessions one purge slice
/// scans before it yields; see [`INDEX_COMPACTION_SLICE_BATCHES`]. Every
/// expired session on a page is deleted in a commit of its own, so a slice of
/// pages full of expired sessions is thousands of commits, and the slice is
/// kept short.
pub const RPC_SESSION_GC_SLICE_BATCHES: usize = 4;

/// Pages of up to [`NORMAL_BATCH_SIZE`] captured live-query events one
/// router pass spends before it yields; see
/// [`INDEX_COMPACTION_SLICE_BATCHES`]. Every event is replayed through the
/// subscriber pipeline — matching, permissions, projection and FETCH, of
/// user-defined cost — once per subscription of its table, so the slice is
/// kept short. An event costs the subscriptions it is replayed to, evaluated
/// or passed over, at least one, and a pass that reaches an event whose
/// subscriptions outnumber what its budget has left replays it to that many
/// and stops inside the event, so a bulk write, or a table with many
/// subscriptions, is delivered over as many passes as its replays fill, and
/// a table's subscriptions are read a page at a time as the replay reaches
/// them. A database with nothing to deliver
/// costs one of the slice, as does a namespace the lap enters, so a lap over
/// a catalog of them spans passes as a backlog does, and a pass that stops
/// with events or databases still to deliver keeps its lap for the next pass.
pub const LIVE_QUERY_ROUTER_SLICE_BATCHES: usize = 4;

/// Pages of up to [`NORMAL_BATCH_SIZE`] node rows one node cleanup pass scans
/// before it yields, and as many pages of keys, archived nodes' live queries
/// and their rows, it deletes, each page in a commit of its own; see
/// [`INDEX_COMPACTION_SLICE_BATCHES`].
pub const NODE_CLEANUP_SLICE_BATCHES: usize = 4;

/// Pages of up to [`NORMAL_BATCH_SIZE`] node rows one node expiry pass scans
/// before it yields, the expired nodes on each page archived in a commit of
/// its own; see [`INDEX_COMPACTION_SLICE_BATCHES`].
pub const NODE_EXPIRE_SLICE_BATCHES: usize = 4;

/// Steps of the catalog one changefeed-collection pass takes before it
/// yields: a namespace entered, a database reached, or a page of a
/// database's tables read.
///
/// Discovering a database's retention reads its table list, and collecting
/// it scans its backlog whether or not anything is there, so this bounds a
/// pass's catalog reads and scans however many namespaces, databases and
/// tables the datastore holds: every list is read a page per step. A sweep
/// takes a page of steps per pass, keeping its place in the catalog between
/// passes, and the next sweep begins once it has visited every database.
pub const CHANGEFEED_GC_SLICE_STEPS: usize = 64;

/// How long a processor's claim on a queued async event keeps every other
/// processor off it.
///
/// A worker claims an entry, in a transaction of its own, immediately before
/// it evaluates the body. Two processors can fetch one entry — an adopter and an
/// owner archived while still running, or two drains of one node's queue —
/// and while the entry's own writes are transactional and discarded by
/// whichever settles it second, an effect the body has outside its
/// transaction, an HTTP request say, is not. A claim this node wrote is
/// honoured for as long as a worker of this process is running the entry,
/// which the process knows, so two drains of one node's queue never evaluate
/// a body twice however long it runs. Another node's worker cannot be asked,
/// so its claim is honoured for this window from when it was written, on the
/// clocks the cluster's nodes already keep close enough to expire one
/// another's heartbeats by, and an older one is taken over: its holder is
/// presumed to have died with it. It is twice the window a node is presumed
/// live without a heartbeat, so that an owner archived for missing its
/// heartbeats keeps the entries it holds until an adopter can take it for
/// gone rather than paused. Across nodes, then, exclusion is guaranteed for a
/// body that finishes within the window; one on an owner that has stopped
/// heartbeating, and outlives the window as well, can be run again by its
/// adopter.
pub const EVENT_CLAIM_TTL: Duration = Duration::from_secs(60);

/// Nodes with state one event-adoption pass steps over before it yields.
///
/// Finding the queues to adopt reads one key per node with any state and the
/// row of each node with a queue, so this bounds a pass's reads however many
/// live nodes the cluster has; the next pass resumes after the last node
/// stepped over, so a large cluster is walked a page per pass.
pub const EVENT_ADOPTION_PAGE_SIZE: usize = 256;

/// Steps of the catalog one stalled-build recovery pass takes: an index
/// reached, whose build state is checked, or a level entered — a namespace,
/// database or table whose list the walk begins.
///
/// The check reads one build-state key per index, and every catalog list is
/// read a page per step, so this bounds a pass's reads however many indexes,
/// tables, databases and namespaces the catalog holds; the next pass
/// continues the walk where the last stopped, and a walk that reaches the
/// end of the catalog is dropped for the next pass to begin afresh.
pub const INDEX_RESUME_PAGE_SIZE: usize = 256;

/// Maximum number of data keys deleted per committed background-reclaim page.
///
/// Bounds the reclaim transaction's write batch, and so the memory one reclaim
/// holds, independently of the reclaimed prefix's cardinality: a prefix of any
/// size is destroyed in pages of at most this many keys, each committed before
/// the next is scanned. The scan is keys-only — a delete needs no value — so a
/// page's own footprint is its key bytes and the write batch dominates.
pub const RECLAIM_BATCH_SIZE: u32 = 1_000;

/// Maximum number of data keys one background-reclaim pass deletes before it
/// yields.
///
/// A single reclaim queue entry can name an arbitrarily large prefix, and the
/// pass runs on a schedule shared with the expired-session purge. Exhausting
/// this budget ends the pass with the resume cursor committed, so the next tick
/// continues from the same key rather than restarting the prefix.
pub const RECLAIM_PASS_KEY_BUDGET: u64 = 100_000;

/// Maximum number of reclaim queue batches, of [`RECLAIM_BATCH_SIZE`] entries
/// each, one background-reclaim pass walks before it yields.
///
/// The walk reads every entry it passes and stamps or ages it, which is
/// bounded by the number of pending removals rather than by
/// [`RECLAIM_PASS_KEY_BUDGET`], so a queue that has grown long would otherwise
/// be read and stamped whole in one pass, in a slice the sweep's other jobs
/// and shutdown wait on. A pass that spends this keeps its position for the
/// next pass, and the scan continues from there.
pub const RECLAIM_PASS_WALK_BATCHES: usize = 4;

/// How many reclaim queue entries one pass divides its key budget between.
///
/// A scan walks the whole queue whatever the key budget, in passes of at most
/// [`RECLAIM_PASS_WALK_BATCHES`] batches: stamping a first sighting and ageing
/// an entry against the grace costs at most one write per entry, so that work
/// scales with the catalog rather than with user data and is not what
/// [`RECLAIM_PASS_KEY_BUDGET`] exists to bound. The key budget is then spent on
/// the longest-waiting eligible entries a pass walked, each granted its share
/// of what is left.
///
/// This bounds the divisor, not how many entries a pass may serve: an entry whose
/// reclaim spends no keys does not consume the budget, so a pass continues past
/// as many of those as it finds. What the cap buys is a useful share — dividing
/// the budget between every candidate would drive each share towards a single key
/// as the queue grows, so no entry would delete a meaningful amount.
///
/// `RECLAIM_PASS_KEY_BUDGET / RECLAIM_BATCH_SIZE`: as many entries as can each be
/// granted one whole page out of a single pass's budget.
pub const RECLAIM_PASS_ENTRY_QUOTA: usize =
	(RECLAIM_PASS_KEY_BUDGET / RECLAIM_BATCH_SIZE as u64) as usize;

/// Maximum number of changefeed entries deleted per committed
/// changefeed-collection page.
///
/// Bounds the collector's transaction, and so the memory one pass holds,
/// independently of how large the retention backlog has grown: a backlog of any
/// size is collected in pages of at most this many keys, each committed before
/// the next is scanned.
pub const CHANGEFEED_GC_BATCH_SIZE: u32 = 1_000;

/// Maximum number of changefeed entries one collection pass deletes before it
/// yields.
///
/// The backlog is sized by write throughput rather than by the catalog, so a
/// single database can hold arbitrarily many stale entries. Exhausting this
/// budget ends the pass with every page committed, so the next tick resumes at
/// the head of what survives. The budget is shared across the databases a pass
/// visits, so one large backlog cannot starve the databases behind it.
///
/// This bounds how long a pass runs, not how much memory it holds — that is
/// [`CHANGEFEED_GC_BATCH_SIZE`], one page at a time, whatever the budget. Set
/// well above the reclaim budget because the two face different problems: a
/// reclaim drains a backlog that `REMOVE` statements fill occasionally, and need
/// only make progress each pass, whereas this backlog refills continuously with
/// every changefeed write. A budget the write rate outruns would trade an
/// unbounded transaction for a backlog that grows on disk without limit, so the
/// budget is sized to sustain the collection rate rather than merely to advance.
pub const CHANGEFEED_GC_PASS_KEY_BUDGET: u64 = 1_000_000;

/// The estimated bytes per key.
pub const ESTIMATED_BYTES_PER_KEY: u32 = 128;
/// The estimated bytes per key-value entry.
pub const ESTIMATED_BYTES_PER_KV: u32 = 512;
