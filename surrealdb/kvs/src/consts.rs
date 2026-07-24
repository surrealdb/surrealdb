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

/// The estimated bytes per key.
pub const ESTIMATED_BYTES_PER_KEY: u32 = 128;
/// The estimated bytes per key-value entry.
pub const ESTIMATED_BYTES_PER_KV: u32 = 512;
