use std::str::FromStr;
use std::time::Duration;

use crate::cnf::Config;
use crate::kvs::config::{SyncMode, parse_duration};
use crate::sys::TOTAL_SYSTEM_MEMORY;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const GIB: u64 = 1024 * MIB;

/// The default compaction readahead size is 256 KiB.
/// This is should ideally be aligned with max_sectors_kb,
/// because the kernel splits each 2MB read into multiple smaller
/// IO requests, adding CPU overhead.
/// 256kb is based on the assumption that that for most workloads,
/// this will be the default sector size.
fn default_compaction_readahead_size() -> usize {
	(256 * KIB) as usize
}

/// Size the LRU block cache from available memory:
/// `max(total_memory / 2 - 1 GiB, 16 MiB)`.
fn default_block_cache_size() -> usize {
	let mem = *TOTAL_SYSTEM_MEMORY;
	mem.saturating_div(2).saturating_sub(GIB).max(16 * MIB) as usize
}

/// Scale each write buffer with system memory.
/// < 1 GiB: 32 MiB, < 16 GiB: 64 MiB, otherwise 128 MiB.
fn default_write_buffer_size() -> usize {
	let mem = *TOTAL_SYSTEM_MEMORY;
	if mem < GIB {
		(32 * MIB) as usize
	} else if mem < 16 * GIB {
		(64 * MIB) as usize
	} else {
		(128 * MIB) as usize
	}
}

/// Scale the maximum number of write buffers with system memory.
/// < 4 GiB: 2, < 16 GiB: 4, < 64 GiB: 8, otherwise 32.
fn default_max_write_buffer_number() -> usize {
	let mem = *TOTAL_SYSTEM_MEMORY;
	if mem < 4 * GIB {
		2
	} else if mem < 16 * GIB {
		4
	} else if mem < 64 * GIB {
		8
	} else {
		32
	}
}

/// Scale the maximum number of subcompactions with the CPU count
/// available to the process. `available_parallelism` respects cgroup
/// CPU quotas on Linux, so a large host running inside a small
/// cgroup is clipped to its quota. The result is clamped into the
/// range `[1, 4]` so at least one subcompaction is permitted and so
/// no more subcompactions are spawned than are useful for a single
/// LSM level.
fn default_max_concurrent_subcompactions() -> u32 {
	let cpu_count = std::thread::available_parallelism().map(|x| x.get()).unwrap_or(1);
	(cpu_count as u32).clamp(1, 4)
}

/// Cap the maximum number of background jobs (flush and compaction
/// workers) against the process memory budget. Each background job
/// carries a thread stack (~8 MiB on Linux) plus compaction
/// readahead and scratch buffers, so memory tends to be the
/// effective ceiling before CPU does on constrained deployments.
/// Use `cpu_count * 2` as the upper bound (the previous default),
/// capped at roughly one job per 128 MiB of system memory and
/// never below 2.
fn default_jobs_count() -> usize {
	let cpu_count = std::thread::available_parallelism().map(|x| x.get()).unwrap_or(1);
	let memory_limited_jobs = (*TOTAL_SYSTEM_MEMORY / (128 * MIB)).max(2) as usize;
	(cpu_count * 2).min(memory_limited_jobs)
}

/// Scale the target file size for compaction with system memory.
/// Smaller SSTs on small systems keep per-compaction work, readahead
/// buffers and disk headroom in proportion. Note that with
/// `target_file_size_multiplier` (default 2) and
/// `file_compaction_trigger` (default 4), L0 can accumulate up to
/// `4 * base` bytes before compaction triggers and each deeper level
/// doubles in size, so the base also caps the on-disk pyramid.
/// < 1 GiB: 8 MiB, < 4 GiB: 16 MiB, < 16 GiB: 32 MiB, otherwise 64 MiB.
fn default_target_file_size_base() -> u64 {
	let mem = *TOTAL_SYSTEM_MEMORY;
	if mem < GIB {
		8 * MIB
	} else if mem < 4 * GIB {
		16 * MIB
	} else if mem < 16 * GIB {
		32 * MIB
	} else {
		64 * MIB
	}
}

/// Scale the grouped-commit batch cap with system memory. Each
/// queued transaction holds its write set in memory until the
/// coordinator flushes, so an unbounded cap can transiently inflate
/// RSS under bursty write load. Larger systems retain the previous
/// throughput-oriented cap of 4096.
/// < 1 GiB: 256, < 4 GiB: 1024, otherwise 4096.
fn default_grouped_commit_max_batch_size() -> usize {
	let mem = *TOTAL_SYSTEM_MEMORY;
	if mem < GIB {
		256
	} else if mem < 4 * GIB {
		1024
	} else {
		4096
	}
}

/// Scale the number of RocksDB info log files to keep on disk. Each
/// rolled log file is bounded but retained; reducing the retention
/// count saves disk on small deployments.
/// < 1 GiB: 2, < 4 GiB: 5, otherwise 10.
fn default_keep_log_file_num() -> usize {
	let mem = *TOTAL_SYSTEM_MEMORY;
	if mem < GIB {
		2
	} else if mem < 4 * GIB {
		5
	} else {
		10
	}
}

/// Cap the per-iterator auto-readahead size at 256 KiB. The implicit
/// prefetcher doubles its read window after each sequential block read,
/// starting at `initial_auto_readahead_size`; this constant bounds the
/// upper limit of that doubling.
///
/// The optimum is hardware-dependent and tracks the kernel block-layer
/// limit on a single device IO request, exposed at
/// `/sys/block/<device>/queue/max_sectors_kb`. When RocksDB's readahead
/// exceeds `max_sectors_kb`, the kernel splits each prefetch into
/// multiple device IOs, adding per-IO overhead. The empirical optimum
/// is therefore approximately the first multiple of `max_sectors_kb`.
///
/// Benchmarked on a cold-cache seekrandom workload (200M-key DB,
/// seek_nexts=10000) on an NVMe with `max_sectors_kb=128`: 128 KiB was
/// the per-machine optimum (578 ops/s, 640 MB/s) and 4 MiB was ~60%
/// slower. Production-class NVMe drives typically ship with
/// `max_sectors_kb=512`, where the same logic implies an optimum closer
/// to 512 KiB. 256 KiB is chosen here as RocksDB's own default and a
/// safe middle ground for both 128 KiB and 512 KiB hardware. Operators
/// on either end of that range can override via the
/// `SURREAL_ROCKSDB_MAX_AUTO_READAHEAD_SIZE` env var.
fn default_max_auto_readahead_size() -> usize {
	(256 * KIB) as usize
}

/// Scale the maximum number of files RocksDB can keep open
/// simultaneously with system memory. Each open SST pins table
/// reader state (index, filter, prefix metadata) in memory, so on
/// small deployments a tight cap keeps table-reader memory
/// proportionate to the available budget. Larger systems retain the
/// previous default of 1026 FDs.
/// < 1 GiB: 256, < 4 GiB: 512, otherwise 1026.
fn default_max_open_files() -> usize {
	let mem = *TOTAL_SYSTEM_MEMORY;
	if mem < GIB {
		256
	} else if mem < 4 * GIB {
		512
	} else {
		1026
	}
}

/// Always enable blob file separation. The `min_blob_size` threshold
/// (default 4 KiB) is the safety mechanism that keeps small values in
/// the LSM where they belong; only values that exceed the threshold
/// pay the cost of blob storage. Per-blob-file in-memory metadata is
/// ~100 bytes; blob files share the `max_open_files` budget with
/// SSTs (no separate FD pool); blob cache shares the block cache (no
/// separate memory allocation).
fn default_enable_blob_files() -> bool {
	true
}

/// Scale the target blob file size with system memory. Blob files
/// store large values (>= `min_blob_size`, default 4 KiB) separately
/// from SSTs, so a single blob file can otherwise pin hundreds of MiB
/// of disk on small deployments.
/// < 1 GiB: 16 MiB, < 4 GiB: 64 MiB, < 16 GiB: 128 MiB, otherwise 256 MiB.
fn default_blob_file_size() -> u64 {
	let mem = *TOTAL_SYSTEM_MEMORY;
	if mem < GIB {
		16 * MIB
	} else if mem < 4 * GIB {
		64 * MIB
	} else if mem < 16 * GIB {
		128 * MIB
	} else {
		256 * MIB
	}
}

/// Scale the WAL size limit (in MiB) with system memory. This governs
/// how many MiB of *archived* (obsolete) WAL files RocksDB keeps
/// around before deleting the oldest ones; it does not force flushes
/// or affect the active WAL. Bounding it prevents archived WAL files
/// from consuming otherwise scarce disk on small deployments, while
/// larger systems retain the RocksDB default of unlimited retention.
/// Returns the limit in MiB (as required by `set_wal_size_limit_mb`);
/// 0 means unlimited.
/// < 1 GiB: 32 MiB, < 16 GiB: 128 MiB, otherwise 0 (unlimited).
fn default_wal_size_limit() -> u64 {
	let mem = *TOTAL_SYSTEM_MEMORY;
	if mem < GIB {
		32
	} else if mem < 16 * GIB {
		128
	} else {
		0
	}
}

// --------------------------------------------------
// Basic options
// --------------------------------------------------

#[derive(Default, Clone, Debug)]
pub enum BlobCompression {
	#[default]
	Snappy,
	Lz4,
	Zstd,
	None,
}

impl FromStr for BlobCompression {
	type Err = ();

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if s.eq_ignore_ascii_case("none") {
			Ok(Self::None)
		} else if s.eq_ignore_ascii_case("lz4") {
			Ok(Self::Lz4)
		} else if s.eq_ignore_ascii_case("snappy") {
			Ok(Self::Snappy)
		} else if s.eq_ignore_ascii_case("zstd") {
			Ok(Self::Zstd)
		} else {
			Err(())
		}
	}
}

/// Default file count at L0 that triggers a level-0 → level-1 compaction.
///
/// The RocksDB default is 4. We tighten it to 2 so that L0 stays small and
/// the `MergingIterator` heap exercised by scans has fewer participants —
/// each L0 file becomes its own iterator child until it is folded into L1.
fn default_file_compaction_trigger() -> usize {
	2
}

/// Default level-0 file count that triggers RocksDB's write slowdown.
///
/// The RocksDB default is 20. Lowering to 8 means writes self-throttle
/// earlier, keeping L0 from growing during write bursts and bounding the
/// `MergingIterator` heap size for concurrent scans. Trades a small slice
/// of pure-write throughput for steadier read latency.
fn default_level0_slowdown_writes_trigger() -> i32 {
	8
}

/// Default level-0 file count that triggers RocksDB's write stall.
///
/// The RocksDB default is 36. Lowering to 12 is the corresponding hard cap
/// to the slowdown trigger above: even under pathological write bursts, L0
/// cannot accumulate beyond this many files. Should remain comfortably
/// above `level0_slowdown_writes_trigger`.
fn default_level0_stop_writes_trigger() -> i32 {
	12
}

/// Default periodic compaction interval (seconds).
///
/// RocksDB's own default is `UINT64_MAX` (never), which means cold ranges
/// with no overlap-driven compaction keep their superseded versions and
/// stale data forever. One hour bounds that overhang at a modest constant
/// background-I/O cost. Set to `0` to disable.
fn default_periodic_compaction_seconds() -> u64 {
	3600
}

/// Default timeout (seconds) for the post-flush `wait_for_compact` step
/// during shutdown.
///
/// Once memtables are flushed at shutdown, RocksDB may have queued an
/// auto-compaction (the new L0 file having crossed
/// `file_compaction_trigger`). Draining that work means the next startup
/// does not have to recover or re-perform it. Thirty seconds matches a
/// typical container-orchestrator SIGTERM grace period so the default
/// never causes a SIGKILL mid-wait; operators issuing maintenance
/// shutdowns can raise it via `rocksdb_shutdown_wait_for_compact_seconds`.
/// Set to `0` to wait indefinitely.
fn default_shutdown_wait_for_compact_seconds() -> u64 {
	30
}

/// Default tokio worker count used to size the inline-blocking permit
/// cap when the embedder hasn't injected a value.
///
/// Mirrors the server crate's `cnf::RUNTIME_WORKER_THREADS` lazy at
/// `surrealdb/server/src/cnf/mod.rs` (`max(4, num_cpus::get())`) so the
/// cap converges on the same value the server uses to size its tokio
/// runtime in the default-deployment case (no `SURREAL_RUNTIME_WORKER_THREADS`).
/// Keep the two definitions in lockstep — if one moves, move the other.
fn default_runtime_worker_threads() -> usize {
	std::cmp::max(4, num_cpus::get())
}

/// Configuration for the RocksDB storage engine, parsed from query parameters.
#[derive(Debug, Clone)]
pub struct RocksDbConfig {
	/// Whether MVCC versioning is enabled.
	pub versioned: bool,
	/// Version retention period in nanoseconds (0 = unlimited).
	pub retention: Duration,
	/// Disk sync mode.
	pub sync_mode: SyncMode,
	/// The number of threads to start for flushing and compaction (default: number
	/// of CPUs)
	pub thread_count: usize,
	/// The maximum number of background jobs (flush and compaction
	/// workers) that RocksDB may run concurrently. Defaults to
	/// `cpu_count * 2`, capped at roughly one job per 128 MiB of
	/// system memory and never below 2, so that a large host running
	/// inside a small cgroup does not spawn more background threads
	/// than the memory budget comfortably supports.
	pub jobs_count: usize,
	/// The maximum number of open files which can be opened by RocksDB
	/// (default: dynamic from 256 to 1026 depending on system memory).
	/// Each open SST pins table reader state (index, filter, prefix
	/// metadata) in memory, so the cap is kept proportional to the
	/// available memory budget.
	pub max_open_files: usize,
	/// The size of each uncompressed data block in bytes (default: 64 KiB)
	pub block_size: usize,
	/// The limit (in MiB) on archived write-ahead-log retention; once
	/// obsolete WAL files exceed this budget RocksDB deletes the oldest
	/// first. Does not affect the active WAL or force flushes. Set to
	/// 0 for unlimited retention (default: dynamic from 32 MiB to
	/// unlimited depending on system memory).
	pub wal_size_limit: u64,
	/// The target file size for compaction in bytes
	/// (default: dynamic from 8 MiB to 64 MiB depending on system memory)
	pub target_file_size_base: u64,
	/// The target file size multiplier for each compaction level (default: 2)
	pub target_file_size_multiplier: usize,
	/// The number of files needed to trigger level 0 compaction (default: 2).
	/// Tighter than the RocksDB default of 4 to keep L0 small for scans.
	pub file_compaction_trigger: usize,
	/// The level-0 file count that triggers RocksDB's write slowdown
	/// (default: 8, RocksDB default: 20). Lowering self-throttles writes
	/// earlier, bounding the `MergingIterator` heap exercised by scans.
	pub level0_slowdown_writes_trigger: i32,
	/// The level-0 file count that triggers RocksDB's write stall
	/// (default: 12, RocksDB default: 36). Acts as the hard cap when the
	/// slowdown trigger cannot keep up; should remain above
	/// `level0_slowdown_writes_trigger`.
	pub level0_stop_writes_trigger: i32,
	/// The interval (seconds) at which RocksDB rewrites any SST not touched
	/// by overlap-driven compaction. Bounds the version-overhang and
	/// tombstone accumulation on cold ranges. Set to 0 to disable
	/// (default: 3600, RocksDB default: never).
	pub periodic_compaction_seconds: u64,
	/// The readahead buffer size used during compaction
	/// (default: dynamic from 4 MiB to 16 MiB)
	pub compaction_readahead_size: usize,
	/// The maximum number of threads which will perform subcompactions
	/// (default: `min(cpu_count, 4)`). Clamped against the process's
	/// available parallelism so a large host running inside a small
	/// cgroup does not oversubscribe the CPU budget with subcompaction
	/// workers.
	pub max_concurrent_subcompactions: u32,
	/// Use separate queues for WAL writes and memtable writes (default: true)
	pub enable_pipelined_writes: bool,
	/// The maximum number of information log files to keep on disk
	/// (default: dynamic from 2 to 10 depending on system memory).
	/// Lower retention saves disk on small deployments; larger systems
	/// retain the previous default for easier post-mortem debugging.
	pub keep_log_file_num: usize,
	/// The information log level of the RocksDB library (default: "warn")
	pub storage_log_level: String,
	/// Use to specify the database compaction style (default: "level").
	/// Accepted values: "level" | "universal". When set to "universal" the
	/// `universal_*` fields below configure the universal compaction
	/// algorithm; otherwise they are ignored.
	pub compaction_style: String,
	/// Universal compaction: size-ratio percentage controlling how much
	/// slack the next file's size has when picking files to merge
	/// (default: 1, RocksDB default: 1). Only applies when
	/// `compaction_style = "universal"`.
	pub universal_size_ratio: i32,
	/// Universal compaction: minimum number of files in a single merge
	/// (default: 2, RocksDB default: 2). Only applies when
	/// `compaction_style = "universal"`.
	pub universal_min_merge_width: u32,
	/// Universal compaction: maximum number of files in a single merge
	/// (default: u32::MAX, RocksDB sentinel for "unlimited"). Only applies
	/// when `compaction_style = "universal"`.
	pub universal_max_merge_width: u32,
	/// Universal compaction: maximum acceptable storage amplification, as
	/// a percentage of the live data size (default: 200, RocksDB default:
	/// 200). Only applies when `compaction_style = "universal"`.
	pub universal_max_size_amplification_percent: u32,
	/// Universal compaction: percentage of output files to compress
	/// (default: -1, RocksDB sentinel for "compress all output"). Only
	/// applies when `compaction_style = "universal"`.
	pub universal_compression_size_percent: i32,
	/// Universal compaction: stop-style governing when a merge is judged
	/// large enough to halt at (default: "total"; accepted values:
	/// "similar_size" | "total"). Only applies when
	/// `compaction_style = "universal"`.
	pub universal_stop_style: String,
	/// The size of the window used to track deletions (default: 500).
	/// Tighter than the previous 1000 to react to delete-driven compaction
	/// triggers more eagerly, reducing tombstone overhang seen by scans.
	pub deletion_factory_window_size: usize,
	/// The number of deletions to track in the window (default: 25).
	/// Halved from 50 to keep the trigger ratio aligned with the smaller
	/// window above.
	pub deletion_factory_delete_count: usize,
	/// The ratio of deletions to track in the window (default: 0.5)
	pub deletion_factory_ratio: f64,
	/// Whether to enable separate key and value file storage
	/// (default: true on systems with >= 1 GiB of memory, false below).
	/// The blob GC loop, extra file descriptors and additional
	/// compaction readahead are not worth the complexity on very small
	/// deployments.
	pub enable_blob_files: bool,
	/// The minimum size of a value for it to be stored in blob files (default: 4
	/// KiB)
	pub min_blob_size: u64,
	/// The target blob file size
	/// (default: dynamic from 16 MiB to 256 MiB depending on system memory)
	pub blob_file_size: u64,
	/// Compression type used for blob files (default: "snappy")
	/// Supported values: "none", "snappy", "lz4", "zstd"
	pub blob_compression_type: BlobCompression,
	/// Whether to enable blob garbage collection (default: true)
	pub enable_blob_gc: bool,
	/// Fractional age cutoff for blob GC eligibility between 0 and 1 (default: 0.5)
	pub blob_gc_age_cutoff: f64,
	/// Discardable ratio threshold to force GC between 0 and 1 (default: 0.5)
	pub blob_gc_force_threshold: f64,
	/// Readahead size for blob compaction/GC (default: 0)
	pub blob_compaction_readahead_size: u64,
	/// The size of the least-recently-used block cache
	/// (default: dynamic depending on system memory)
	pub block_cache_size: usize,
	/// The amount of data each write buffer can build up in memory
	/// (default: dynamic from 32 MiB to 128 MiB)
	pub write_buffer_size: usize,
	/// The maximum number of write buffers which can be used
	/// (default: dynamic from 2 to 32)
	pub max_write_buffer_number: usize,
	/// The minimum number of write buffers to merge before writing to disk
	/// (default: 2)
	pub min_write_buffer_number_to_merge: usize,
	/// The maximum allowed space usage for SST files in bytes (default: 0, meaning unlimited).
	/// When this limit is reached, the datastore enters read-and-deletion-only mode, where only
	/// read and delete operations are allowed. This allows gradual space recovery through data
	/// deletion. Set to 0 to disable space monitoring.
	pub sst_max_allowed_space_usage: u64,
	/// The maximum wait time in nanoseconds before forcing a grouped commit (default: 5ms).
	/// This timeout ensures that transactions don't wait indefinitely under low concurrency and
	/// balances commit latency against write throughput.
	pub grouped_commit_timeout: u64,
	/// Threshold for deciding whether to wait for more transactions (default: 12)
	/// If the current batch size is greater or equal to this threshold (and below
	/// ROCKSDB_GROUPED_COMMIT_MAX_BATCH_SIZE), then the coordinator will wait up to
	/// ROCKSDB_GROUPED_COMMIT_TIMEOUT to collect more transactions. Smaller batches are flushed
	/// immediately to preserve low latency.
	pub grouped_commit_wait_threshold: usize,
	/// The maximum number of transactions in a single grouped commit batch
	/// (default: dynamic from 256 to 4096 depending on system memory).
	/// Each queued transaction holds its write set in memory until the
	/// coordinator flushes, so smaller systems use smaller batches to
	/// cap transient RSS under bursty write load. Larger batches improve
	/// throughput but increase memory usage and commit latency.
	pub grouped_commit_max_batch_size: usize,

	/// The initial readahead size used by RocksDB's implicit (auto) iterator
	/// prefetcher. Once an iterator performs `file_reads_for_auto_readahead`
	/// sequential file reads, RocksDB starts prefetching at this size and then
	/// doubles it on each subsequent read up to `max_auto_readahead_size`
	/// (default: 8 KiB)
	pub initial_auto_readahead_size: usize,
	/// The upper bound on RocksDB's implicit (auto) iterator readahead size.
	/// Raising this cap (from the RocksDB default of 256 KiB) can significantly
	/// improve throughput on long range scans at the cost of additional I/O
	/// and memory per iterator. Because each active iterator can grow its
	/// readahead up to this cap, many concurrent scans multiply the memory
	/// footprint, so the default scales down on small systems
	/// (default: dynamic from 512 KiB to 4 MiB depending on system memory).
	pub max_auto_readahead_size: usize,
	/// The number of sequential file reads an iterator must perform on a
	/// single SST before RocksDB begins its implicit (auto) readahead. Set to
	/// 0 to start prefetching from the very first read (default: 2)
	pub file_reads_for_auto_readahead: u64,

	/// Whether to enable the custom SurrealDB prefix extractor on the
	/// RocksDB column family. When enabled, SST bloom filters are built on
	/// table+category prefixes (records, indexes, graph edges, refs,
	/// per-table metadata) which allows scans to skip entire SSTs that do
	/// not contain the scanning table. See `prefix_extractor.rs` for the
	/// extractor semantics (default: true)
	pub prefix_extractor_enabled: bool,
	/// When the prefix extractor is enabled, controls whether whole keys
	/// are also added to the SST bloom filter (in addition to the prefix).
	/// * true (default): bloom filter carries both whole keys and prefixes. Point lookups benefit
	///   from the whole-key filter, at the cost of a larger bloom filter footprint.
	/// * false: bloom filter carries only prefixes. Smaller bloom filter, but point lookups fall
	///   back to prefix filter + block index search. Useful when the workload is scan-heavy.
	pub whole_key_filtering: bool,
	/// When the prefix extractor is enabled, configures the per-memtable
	/// prefix bloom filter size as a ratio of the write buffer size (capped
	/// at 0.25 by RocksDB). Set to 0 to disable the memtable prefix bloom
	/// (default: 0.1).
	pub memtable_prefix_bloom_ratio: f64,

	/// Whether to verify per-block CRC32C checksums when iterating during
	/// scans and counts. Verification runs on first read of a block (cold
	/// path); cached blocks are not re-checksummed. Disabling trades
	/// integrity for cold-scan throughput; only safe on trusted storage.
	/// Applies to both `scan_read_options` and `count_read_options`
	/// (default: true).
	pub scan_verify_checksums: bool,

	/// Whether the graceful-shutdown path runs a full-keyspace compaction
	/// (down to the bottommost level, mirroring `ALTER SYSTEM COMPACT`)
	/// after flushing memtables. Off by default because the operation is
	/// O(database size) and can take minutes-to-hours on large datasets,
	/// which conflicts with typical container-orchestrator SIGTERM grace
	/// periods. Useful for maintenance shutdowns and benchmark setups
	/// where the next startup is expected to be scan-heavy
	/// (default: false).
	pub compact_on_shutdown: bool,

	/// Timeout (seconds) for the post-flush `wait_for_compact` step during
	/// graceful shutdown. After memtables flush to L0 the engine may have
	/// auto-scheduled an L0→L1 compaction; waiting for it to drain means
	/// the next startup does not have to recover or re-perform it. Set to
	/// `0` to wait indefinitely
	/// (default: 30).
	pub shutdown_wait_for_compact_seconds: u64,

	/// Tokio runtime worker thread count used to size the inline-blocking
	/// `InlineGuard` permit cap.
	///
	/// Read from the shared `runtime_worker_threads` `ConfigMap` key,
	/// which the server populates via
	/// `Datastore::builder().with_runtime_worker_threads(...)` from its
	/// `cnf::RUNTIME_WORKER_THREADS` static — itself sourced from
	/// `SURREAL_RUNTIME_WORKER_THREADS` with a `max(4, num_cpus::get())`
	/// fallback. `ConfigMap::from_env()` also lowercases the env var
	/// into the same key, so an explicit env override still wins.
	///
	/// Embedded callers building a custom tokio runtime can match it
	/// by populating the same key on the `ConfigMap` they pass to
	/// `Datastore::builder().with_config(...)` or by chaining
	/// `with_runtime_worker_threads(...)`. When neither is provided
	/// the field falls back to [`default_runtime_worker_threads`] so
	/// the inline fast path stays enabled by default.
	pub runtime_worker_threads: usize,

	/// Tokio workers kept available for async work no matter how many
	/// transactions are stuck in a synchronous storage call. Subtracted
	/// from `runtime_worker_threads` to size the inline-blocking permit
	/// cap. The cap is `workers - reserve`, saturating at 0. Larger
	/// values reserve more headroom for async progress under cold-cache
	/// load but cap inline throughput sooner on sustained CRUD
	/// workloads
	/// (default: `2`).
	pub runtime_reserve: usize,
}

impl Default for RocksDbConfig {
	fn default() -> Self {
		let cpu_count = std::thread::available_parallelism().map(|x| x.get()).unwrap_or(1);
		Self {
			versioned: false,
			retention: Duration::ZERO,
			sync_mode: SyncMode::Every,
			thread_count: cpu_count,
			jobs_count: default_jobs_count(),
			max_open_files: default_max_open_files(),
			block_size: 64 * 1024,
			wal_size_limit: default_wal_size_limit(),
			target_file_size_base: default_target_file_size_base(),
			target_file_size_multiplier: 2,
			file_compaction_trigger: default_file_compaction_trigger(),
			level0_slowdown_writes_trigger: default_level0_slowdown_writes_trigger(),
			level0_stop_writes_trigger: default_level0_stop_writes_trigger(),
			periodic_compaction_seconds: default_periodic_compaction_seconds(),
			compaction_readahead_size: default_compaction_readahead_size(),
			max_concurrent_subcompactions: default_max_concurrent_subcompactions(),
			enable_pipelined_writes: true,
			keep_log_file_num: default_keep_log_file_num(),
			storage_log_level: "warn".to_owned(),
			compaction_style: "level".to_owned(),
			universal_size_ratio: 1,
			universal_min_merge_width: 2,
			universal_max_merge_width: u32::MAX,
			universal_max_size_amplification_percent: 200,
			universal_compression_size_percent: -1,
			universal_stop_style: "total".to_owned(),
			deletion_factory_window_size: 500,
			deletion_factory_delete_count: 25,
			deletion_factory_ratio: 0.5,
			enable_blob_files: default_enable_blob_files(),
			min_blob_size: 4 * 1024,
			blob_file_size: default_blob_file_size(),
			blob_compression_type: Default::default(),
			enable_blob_gc: true,
			blob_gc_age_cutoff: 0.5,
			blob_gc_force_threshold: 0.5,
			blob_compaction_readahead_size: 0,
			block_cache_size: default_block_cache_size(),
			write_buffer_size: default_write_buffer_size(),
			max_write_buffer_number: default_max_write_buffer_number(),
			min_write_buffer_number_to_merge: 2,
			sst_max_allowed_space_usage: 0,
			grouped_commit_timeout: Duration::from_millis(5).as_nanos() as u64,
			grouped_commit_wait_threshold: 12,
			grouped_commit_max_batch_size: default_grouped_commit_max_batch_size(),
			initial_auto_readahead_size: 8 * 1024,
			max_auto_readahead_size: default_max_auto_readahead_size(),
			file_reads_for_auto_readahead: 2,
			prefix_extractor_enabled: true,
			whole_key_filtering: true,
			memtable_prefix_bloom_ratio: 0.1,
			scan_verify_checksums: true,
			compact_on_shutdown: false,
			shutdown_wait_for_compact_seconds: default_shutdown_wait_for_compact_seconds(),
			runtime_worker_threads: default_runtime_worker_threads(),
			runtime_reserve: 2,
		}
	}
}

impl Config for RocksDbConfig {
	fn parse(&mut self, map: &crate::cnf::ConfigMap) {
		map.parse_key_bool("datastore_versioned", &mut self.versioned)
			.parse_key_with("datastore_retention", &mut self.retention, |x| parse_duration(x).ok())
			.parse_key("rocksdb_thread_count", &mut self.thread_count)
			.parse_key("rocksdb_jobs_count", &mut self.jobs_count)
			.parse_key("rocksdb_max_open_files", &mut self.max_open_files)
			.parse_key("rocksdb_block_size", &mut self.block_size)
			.parse_key("rocksdb_wal_size_limit", &mut self.wal_size_limit)
			.parse_key("rocksdb_target_file_size_base", &mut self.target_file_size_base)
			.parse_key("rocksdb_target_file_size_multiplier", &mut self.target_file_size_multiplier)
			.parse_key("rocksdb_file_compaction_trigger", &mut self.file_compaction_trigger)
			.parse_key(
				"rocksdb_level0_slowdown_writes_trigger",
				&mut self.level0_slowdown_writes_trigger,
			)
			.parse_key("rocksdb_level0_stop_writes_trigger", &mut self.level0_stop_writes_trigger)
			.parse_key("rocksdb_periodic_compaction_seconds", &mut self.periodic_compaction_seconds)
			.parse_key("rocksdb_compaction_readahead_size", &mut self.compaction_readahead_size)
			.parse_key(
				"rocksdb_max_concurrent_subcompactions",
				&mut self.max_concurrent_subcompactions,
			)
			.parse_key_bool("rocksdb_enable_pipelined_writes", &mut self.enable_pipelined_writes)
			.parse_key("rocksdb_keep_log_file_num", &mut self.keep_log_file_num)
			.parse_key("rocksdb_storage_log_level", &mut self.storage_log_level)
			.parse_key("rocksdb_compaction_style", &mut self.compaction_style)
			.parse_key("rocksdb_universal_size_ratio", &mut self.universal_size_ratio)
			.parse_key("rocksdb_universal_min_merge_width", &mut self.universal_min_merge_width)
			.parse_key("rocksdb_universal_max_merge_width", &mut self.universal_max_merge_width)
			.parse_key(
				"rocksdb_universal_max_size_amplification_percent",
				&mut self.universal_max_size_amplification_percent,
			)
			.parse_key(
				"rocksdb_universal_compression_size_percent",
				&mut self.universal_compression_size_percent,
			)
			.parse_key("rocksdb_universal_stop_style", &mut self.universal_stop_style)
			.parse_key(
				"rocksdb_deletion_factory_window_size",
				&mut self.deletion_factory_window_size,
			)
			.parse_key(
				"rocksdb_deletion_factory_delete_count",
				&mut self.deletion_factory_delete_count,
			)
			.parse_key("rocksdb_deletion_factory_ratio", &mut self.deletion_factory_ratio)
			.parse_key_bool("rocksdb_enable_blob_files", &mut self.enable_blob_files)
			.parse_key("rocksdb_min_blob_size", &mut self.min_blob_size)
			.parse_key("rocksdb_blob_file_size", &mut self.blob_file_size)
			.parse_key("rocksdb_blob_compression_type", &mut self.blob_compression_type)
			.parse_key_bool("rocksdb_enable_blob_gc", &mut self.enable_blob_gc)
			.parse_key("rocksdb_blob_gc_age_cutoff", &mut self.blob_gc_age_cutoff)
			.parse_key("rocksdb_blob_gc_force_threshold", &mut self.blob_gc_force_threshold)
			.parse_key(
				"rocksdb_blob_compaction_readahead_size",
				&mut self.blob_compaction_readahead_size,
			)
			.parse_key("rocksdb_block_cache_size", &mut self.block_cache_size)
			.parse_key("rocksdb_write_buffer_size", &mut self.write_buffer_size)
			.parse_key("rocksdb_max_write_buffer_number", &mut self.max_write_buffer_number)
			.parse_key(
				"rocksdb_min_write_buffer_number_to_merge",
				&mut self.min_write_buffer_number_to_merge,
			)
			.parse_key("rocksdb_sst_max_allowed_space_usage", &mut self.sst_max_allowed_space_usage)
			.parse_key("rocksdb_grouped_commit_timeout", &mut self.grouped_commit_timeout)
			.parse_key(
				"rocksdb_grouped_commit_wait_threshold",
				&mut self.grouped_commit_wait_threshold,
			)
			.parse_key(
				"rocksdb_grouped_commit_max_batch_size",
				&mut self.grouped_commit_max_batch_size,
			)
			.parse_key("rocksdb_initial_auto_readahead_size", &mut self.initial_auto_readahead_size)
			.parse_key("rocksdb_max_auto_readahead_size", &mut self.max_auto_readahead_size)
			.parse_key(
				"rocksdb_file_reads_for_auto_readahead",
				&mut self.file_reads_for_auto_readahead,
			)
			.parse_key_bool("rocksdb_prefix_extractor_enabled", &mut self.prefix_extractor_enabled)
			.parse_key_bool("rocksdb_whole_key_filtering", &mut self.whole_key_filtering)
			.parse_key("rocksdb_memtable_prefix_bloom_ratio", &mut self.memtable_prefix_bloom_ratio)
			.parse_key_bool("rocksdb_scan_verify_checksums", &mut self.scan_verify_checksums)
			.parse_key_bool("rocksdb_compact_on_shutdown", &mut self.compact_on_shutdown)
			.parse_key(
				"rocksdb_shutdown_wait_for_compact_seconds",
				&mut self.shutdown_wait_for_compact_seconds,
			)
			.parse_key("rocksdb_runtime_reserve", &mut self.runtime_reserve)
			// Shared key with the server crate's tokio runtime sizing.
			// The server injects its resolved `cnf::RUNTIME_WORKER_THREADS`
			// via `Datastore::builder().with_runtime_worker_threads(...)`,
			// and `ConfigMap::from_env()` also lowercases
			// `SURREAL_RUNTIME_WORKER_THREADS` into the same key, so an
			// explicit env override still wins. When neither is present
			// the field retains its `default_runtime_worker_threads()`
			// default and the inline cap is computed from that.
			.parse_key("runtime_worker_threads", &mut self.runtime_worker_threads);

		if map.has_key("datastore_sync") {
			map.parse_key("datastore_sync", &mut self.sync_mode);
		} else {
			map.parse_key("datastore_sync_data", &mut self.sync_mode);
		}
	}
}

#[cfg(test)]
mod test {
	use std::time::Duration;

	use crate::cnf::ConfigMap;
	use crate::kvs::config::SyncMode;
	use crate::kvs::rocksdb::RocksDbConfig;

	#[test]
	fn test_rocksdb_config_defaults() {
		let map = ConfigMap::empty();
		let config = map.load::<RocksDbConfig>();
		assert!(!config.versioned);
		assert_eq!(config.retention, Duration::ZERO);
		assert_eq!(config.sync_mode, SyncMode::Every);
	}

	#[test]
	fn test_rocksdb_config_compaction_defaults() {
		let config = ConfigMap::empty().load::<RocksDbConfig>();
		// Compaction-trigger defaults tightened to favour scans.
		assert_eq!(config.file_compaction_trigger, 2);
		assert_eq!(config.level0_slowdown_writes_trigger, 8);
		assert_eq!(config.level0_stop_writes_trigger, 12);
		// Periodic compaction enabled by default (one hour).
		assert_eq!(config.periodic_compaction_seconds, 3600);
		// Deletion-collector factory window halved.
		assert_eq!(config.deletion_factory_window_size, 500);
		assert_eq!(config.deletion_factory_delete_count, 25);
		assert!((config.deletion_factory_ratio - 0.5).abs() < f64::EPSILON);
		// Universal-compaction knobs default to RocksDB's own defaults.
		assert_eq!(config.compaction_style, "level");
		assert_eq!(config.universal_size_ratio, 1);
		assert_eq!(config.universal_min_merge_width, 2);
		assert_eq!(config.universal_max_merge_width, u32::MAX);
		assert_eq!(config.universal_max_size_amplification_percent, 200);
		assert_eq!(config.universal_compression_size_percent, -1);
		assert_eq!(config.universal_stop_style, "total");
		// Shutdown defaults: drain in-flight compactions for up to 60s but
		// never run a full-keyspace compaction unless asked.
		assert!(!config.compact_on_shutdown);
		assert_eq!(config.shutdown_wait_for_compact_seconds, 30);
	}

	#[test]
	fn test_rocksdb_config_compaction_overrides() {
		let map = ConfigMap::empty()
			.with_key_value("rocksdb_file_compaction_trigger", "5")
			.with_key_value("rocksdb_level0_slowdown_writes_trigger", "16")
			.with_key_value("rocksdb_level0_stop_writes_trigger", "24")
			.with_key_value("rocksdb_periodic_compaction_seconds", "0");
		let config = map.load::<RocksDbConfig>();
		assert_eq!(config.file_compaction_trigger, 5);
		assert_eq!(config.level0_slowdown_writes_trigger, 16);
		assert_eq!(config.level0_stop_writes_trigger, 24);
		assert_eq!(config.periodic_compaction_seconds, 0);
	}

	#[test]
	fn test_rocksdb_config_shutdown_overrides() {
		let map = ConfigMap::empty()
			.with_key_value("rocksdb_compact_on_shutdown", "true")
			.with_key_value("rocksdb_shutdown_wait_for_compact_seconds", "5");
		let config = map.load::<RocksDbConfig>();
		assert!(config.compact_on_shutdown);
		assert_eq!(config.shutdown_wait_for_compact_seconds, 5);
	}

	#[test]
	fn test_rocksdb_config_universal_overrides() {
		let map = ConfigMap::empty()
			.with_key_value("rocksdb_compaction_style", "universal")
			.with_key_value("rocksdb_universal_size_ratio", "5")
			.with_key_value("rocksdb_universal_min_merge_width", "3")
			.with_key_value("rocksdb_universal_max_merge_width", "16")
			.with_key_value("rocksdb_universal_max_size_amplification_percent", "150")
			.with_key_value("rocksdb_universal_compression_size_percent", "75")
			.with_key_value("rocksdb_universal_stop_style", "similar_size");
		let config = map.load::<RocksDbConfig>();
		assert_eq!(config.compaction_style, "universal");
		assert_eq!(config.universal_size_ratio, 5);
		assert_eq!(config.universal_min_merge_width, 3);
		assert_eq!(config.universal_max_merge_width, 16);
		assert_eq!(config.universal_max_size_amplification_percent, 150);
		assert_eq!(config.universal_compression_size_percent, 75);
		assert_eq!(config.universal_stop_style, "similar_size");
	}

	#[test]
	fn test_rocksdb_config_sync_every() {
		let map =
			ConfigMap::from_config_string("sync=every").map_keys(|x| format!("datastore_{x}"));
		let config = map.load::<RocksDbConfig>();
		assert_eq!(config.sync_mode, SyncMode::Every);
	}

	#[test]
	fn test_rocksdb_config_sync_never() {
		let map =
			ConfigMap::from_config_string("sync=never").map_keys(|x| format!("datastore_{x}"));
		let config = map.load::<RocksDbConfig>();
		assert_eq!(config.sync_mode, SyncMode::Never);
	}

	#[test]
	fn test_rocksdb_config_sync_periodic() {
		let map =
			ConfigMap::from_config_string("sync=200ms").map_keys(|x| format!("datastore_{x}"));
		let config = map.load::<RocksDbConfig>();
		assert_eq!(config.sync_mode, SyncMode::Interval(Duration::from_millis(200)));
	}

	#[test]
	fn test_rocksdb_config_sync_periodic_seconds() {
		let map = ConfigMap::from_config_string("sync=5s").map_keys(|x| format!("datastore_{x}"));
		let config = map.load::<RocksDbConfig>();
		assert_eq!(config.sync_mode, SyncMode::Interval(Duration::from_secs(5)));
	}

	#[test]
	fn test_rocksdb_config_full_params() {
		let map = ConfigMap::from_config_string("versioned=true&retention=30d&sync=every")
			.map_keys(|x| format!("datastore_{x}"));
		let config = map.load::<RocksDbConfig>();
		assert!(config.versioned);
		assert_eq!(config.retention, Duration::from_secs(30 * 24 * 60 * 60));
		assert_eq!(config.sync_mode, SyncMode::Every);
	}

	#[test]
	fn test_rocksdb_config_runtime_worker_threads_default() {
		// With no env override and no explicit injection, the field must
		// land on a non-zero default that keeps the inline-blocking fast
		// path enabled. The default mirrors the server's tokio runtime
		// sizing (`max(4, num_cpus::get())`), so it is always >= 4.
		let config = ConfigMap::empty().load::<RocksDbConfig>();
		assert!(
			config.runtime_worker_threads >= 4,
			"expected default runtime_worker_threads >= 4, got {}",
			config.runtime_worker_threads,
		);
	}

	#[test]
	fn test_rocksdb_config_runtime_worker_threads_override() {
		// An explicit value injected via the shared `runtime_worker_threads`
		// ConfigMap key (the same key `ConfigMap::from_env()` populates
		// from `SURREAL_RUNTIME_WORKER_THREADS`, and the key the builder's
		// `with_runtime_worker_threads` writes into) must win over the
		// default.
		let config = ConfigMap::empty()
			.with_key_value("runtime_worker_threads", "1")
			.load::<RocksDbConfig>();
		assert_eq!(config.runtime_worker_threads, 1);
	}
}
