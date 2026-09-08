pub const NORMAL_BATCH_SIZE: u32 = 500;
pub const INDEXING_BATCH_SIZE: u32 = 250;
/// Soft cap on the raw record bytes an index build holds in memory per
/// initial-scan batch.
///
/// Batches are sized by record count, so without a byte budget a batch of
/// [`INDEXING_BATCH_SIZE`] large documents can hold hundreds of megabytes at
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
/// Every mutation of a record carrying a compaction-eligible index enqueues
/// one entry, so the queue length is proportional to all indexed write
/// activity since it last drained and can reach hundreds of thousands of
/// entries when draining is delayed. This bound caps both the queue scan and
/// the write transaction that deletes the drained entries, keeping the
/// per-transaction key cardinality small enough for distributed backends,
/// which reserve every written key on every replica at prepare time.
///
/// Bounds only the queue drain: a single index's compaction work is batched
/// separately by [`COUNT_BATCH_SIZE`], so one per-index compaction
/// transaction may carry several times more writes than one queue-drain
/// batch.
pub const INDEX_COMPACTION_QUEUE_BATCH_SIZE: u32 = 10_000;

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

/// How many reclaim queue entries one pass divides its key budget between.
///
/// A pass walks the whole queue whatever its budget: stamping a first sighting
/// and ageing an entry against the grace costs at most one write per entry, so
/// that work scales with the catalog rather than with user data and is not what
/// [`RECLAIM_PASS_KEY_BUDGET`] exists to bound. The key budget is then spent on
/// the longest-waiting eligible entries, each granted its share of what is left.
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
