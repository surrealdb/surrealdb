use std::sync::Arc;

use rocksdb::{BlockBasedIndexType, BlockBasedOptions, Cache, Options, WriteBufferManager};

use super::TARGET;
use crate::kvs::Result;
use crate::kvs::rocksdb::RocksDbConfig;
use crate::mem::{MemoryReporter, cleanup_memory_reporters, register_memory_reporter};

pub(super) struct MemoryManager {
	/// The write buffer manager
	write_buffer_manager: WriteBufferManager,
	/// The RocksDB block cache
	cache: Cache,
}

impl MemoryReporter for MemoryManager {
	fn memory_allocated(&self) -> usize {
		self.write_buffer_manager.get_usage() + self.cache.get_usage()
	}
}

impl MemoryManager {
	/// Pre-configure the disk space manager
	pub(super) fn configure(opts: &mut Options, config: &RocksDbConfig) -> Result<Self> {
		// Get the configuration options
		let block_cache_size = config.block_cache_size;
		let write_buffer_size = config.write_buffer_size;
		// Get the total write buffer size
		let total_write_buffer_size =
			config.max_write_buffer_number.saturating_mul(write_buffer_size);
		// Combine the cache and the write buffers to get the memory limit
		let total_memory_limit = total_write_buffer_size + block_cache_size;
		info!(target: TARGET, "Memory manager: total memory limit: {total_memory_limit}");
		// Set the block cache size in bytes
		info!(target: TARGET, "Memory manager: block cache size: {block_cache_size}B");
		// Configure the in-memory cache options
		let cache = Cache::new_lru_cache(config.block_cache_size);
		// Create a new write buffer manager with the cache
		let write_buffer_manager = WriteBufferManager::new_write_buffer_manager_with_cache(
			total_memory_limit,
			true,
			cache.clone(),
		);
		// Set the write buffer manager in the options
		opts.set_write_buffer_manager(&write_buffer_manager);
		// Set the row cache in the options
		opts.set_row_cache(&cache);
		// Build the manager and apply its per-CF settings to `opts`
		let manager = Self {
			write_buffer_manager,
			cache,
		};
		manager.apply_to_cf_options(opts, config);
		// Continue
		Ok(manager)
	}

	/// Apply the column-family-level memory settings to `target`. Called
	/// once on the main `opts` during [`Self::configure`] so the implicit
	/// default CF is configured correctly, and also called on an
	/// explicit [`rocksdb::ColumnFamilyDescriptor`]'s options when
	/// versioning is enabled (see `Datastore::new`) so the explicit
	/// default CF receives the same memtable and block-cache settings
	/// rather than RocksDB's fresh defaults.
	pub(super) fn apply_to_cf_options(&self, target: &mut Options, config: &RocksDbConfig) {
		let write_buffer_size = config.write_buffer_size;
		// Get the minimum number of write buffers to merge
		let requested_write_buffers_to_merge = config.min_write_buffer_number_to_merge.max(1);
		// Get the maximum number of write buffers
		let max_write_buffer_number = config.max_write_buffer_number.min(i32::MAX as usize) as i32;
		// Clamp the merge target to the maximum number of write buffers.
		// RocksDB cannot merge more memtables than it is allowed to keep
		// in memory, so if `min_write_buffer_number_to_merge` exceeds
		// `max_write_buffer_number` (for example when an operator lowers
		// the max to 1 on a constrained instance) writers would stall
		// indefinitely waiting for a merge that can never happen. Clamp
		// to a valid value and warn so the misconfiguration is visible.
		let write_buffers_to_merge =
			requested_write_buffers_to_merge.min(config.max_write_buffer_number.max(1));
		// Check if the number of write buffers exceeds the maximum number allowed
		if write_buffers_to_merge != requested_write_buffers_to_merge {
			warn!(target: TARGET,
				"Memory manager: min_write_buffer_number_to_merge ({requested_write_buffers_to_merge}) exceeds \
				max_write_buffer_number ({}); clamping to {write_buffers_to_merge} to avoid \
				stalling writers",
				config.max_write_buffer_number,
			);
		}
		// Get the adjusted minimum number of write buffers to merge
		let min_write_buffers_to_merge = write_buffers_to_merge.min(i32::MAX as usize) as i32;
		// Set the amount of data to build up in memory
		info!(target: TARGET, "Memory manager: write buffer size: {write_buffer_size}B");
		target.set_write_buffer_size(write_buffer_size);
		// Set the maximum number of write buffers
		info!(target: TARGET, "Memory manager: maximum write buffers: {max_write_buffer_number}");
		target.set_max_write_buffer_number(max_write_buffer_number);
		// Set minimum number of write buffers to merge
		info!(target: TARGET, "Memory manager: minimum write buffers to merge: {min_write_buffers_to_merge}");
		target.set_min_write_buffer_number_to_merge(min_write_buffers_to_merge);
		// Configure the block based file options
		let mut block = BlockBasedOptions::default();
		block.set_pin_l0_filter_and_index_blocks_in_cache(true);
		block.set_pin_top_level_index_and_filter(true);
		block.set_bloom_filter(10.0, false);
		// Configure the target block size
		info!(target: TARGET, "Target block size: {}", config.block_size);
		block.set_block_size(config.block_size);
		// Configure the block cache
		info!(target: TARGET, "Block cache size: {}", config.block_cache_size);
		block.set_block_cache(&self.cache);
		// Configure the index type and partition filters
		info!(target: TARGET, "Configuring two-level index search");
		block.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
		// Configure the partition filters for SST files
		info!(target: TARGET, "Use partitioned filters for each SST file");
		block.set_partition_filters(true);
		// Configure the metadata block size
		info!(target: TARGET, "Block size for partitioned metadata: 4096 B");
		block.set_metadata_block_size(4096);
		// Set the initial size for implicit iterator auto-readahead
		info!(target: TARGET, "Initial auto-readahead size: {}", config.initial_auto_readahead_size);
		block.set_initial_auto_readahead_size(config.initial_auto_readahead_size);
		// Set the maximum size for implicit iterator auto-readahead
		info!(target: TARGET, "Maximum auto-readahead size: {}", config.max_auto_readahead_size);
		block.set_max_auto_readahead_size(config.max_auto_readahead_size);
		// Set the number of sequential file reads before triggering auto-readahead
		info!(target: TARGET, "Number of file reads for auto-readahead: {}", config.file_reads_for_auto_readahead);
		block.set_num_file_reads_for_auto_readahead(config.file_reads_for_auto_readahead);
		// When the prefix extractor is enabled the SST bloom filter is
		// keyed on table+category prefixes. `whole_key_filtering=true`
		// additionally adds whole keys (better for point lookups, larger
		// filter); `whole_key_filtering=false` keeps the filter tight and
		// focuses all bits on the prefix (better for scan-heavy workloads).
		// The setting only matters when a prefix extractor is configured —
		// without one, `whole_key_filtering` is effectively always true.
		let whole_key_filtering = if config.prefix_extractor_enabled {
			config.whole_key_filtering
		} else {
			true
		};
		info!(target: TARGET, "Memory manager: whole key filtering: {whole_key_filtering}");
		block.set_whole_key_filtering(whole_key_filtering);
		// Configure the database with the cache
		target.set_block_based_table_factory(&block);
		target.set_blob_cache(&self.cache);
	}

	// Register the memory manager with the global allocator tracker
	#[allow(clippy::clone_on_ref_ptr)] // Arc::clone would not coerce to `Arc<dyn MemoryReporter>`
	pub(super) fn register_with_allocator_tracker(self: &Arc<Self>) {
		// Downgrade the memory manager to a memory reporter
		let reporter: Arc<dyn MemoryReporter> = self.clone();
		// Register with the global allocator tracker
		register_memory_reporter("rocksdb", Arc::downgrade(&reporter));
	}

	/// Shutdown the memory manager
	pub fn shutdown(&self) -> Result<()> {
		// Clean up the memory manager
		cleanup_memory_reporters();
		// All good
		Ok(())
	}
}
