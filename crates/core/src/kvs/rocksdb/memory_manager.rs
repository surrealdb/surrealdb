use rocksdb::{BlockBasedOptions, Cache, Options, WriteBufferManager};

use super::{TARGET, cnf};
use crate::kvs::Result;

pub(super) struct MemoryManager {
	/// The write buffer manager
	write_buffer_manager: WriteBufferManager,
	/// The RocksDB block cache
	cache: Cache,
}

impl MemoryManager {
	/// Pre-configure the disk space manager
	pub(super) fn configure(opts: &mut Options) -> Result<Self> {
		//
		let block_cache_size = *cnf::ROCKSDB_BLOCK_CACHE_SIZE;
		let write_buffer_size = *cnf::ROCKSDB_WRITE_BUFFER_SIZE;
		let max_write_buffer_number = *cnf::ROCKSDB_MAX_WRITE_BUFFER_NUMBER;
		let min_write_buffers_to_merge = *cnf::ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE;
		let total_write_buffer_size = write_buffer_size * max_write_buffer_number as usize;
		let total_memory_limit = total_write_buffer_size + block_cache_size;
		// Set the block cache size in bytes
		info!(target: TARGET, "Memory manager: block cache size: {block_cache_size}B");
		// Set the amount of data to build up in memory
		info!(target: TARGET, "Memory manager: write buffer size: {write_buffer_size}B");
		opts.set_write_buffer_size(write_buffer_size);
		// Set the maximum number of write buffers
		info!(target: TARGET, "Memory manager: maximum write buffers: {max_write_buffer_number}");
		opts.set_max_write_buffer_number(max_write_buffer_number);
		// Set minimum number of write buffers to merge
		info!(target: TARGET, "Memory manager: minimum write buffers to merge: {min_write_buffers_to_merge}");
		opts.set_min_write_buffer_number_to_merge(min_write_buffers_to_merge);
		// Combine the cache and the write buffers to get the memory limit
		info!(target: TARGET, "Memory manager: total memory limit: {total_memory_limit}");
		// Configure the in-memory cache options
		let cache = Cache::new_lru_cache(*cnf::ROCKSDB_BLOCK_CACHE_SIZE);
		// Configure the block based file options
		let mut block = BlockBasedOptions::default();
		block.set_pin_l0_filter_and_index_blocks_in_cache(true);
		block.set_pin_top_level_index_and_filter(true);
		block.set_bloom_filter(10.0, false);
		block.set_block_size(*cnf::ROCKSDB_BLOCK_SIZE);
		block.set_block_cache(&cache);
		// Configure the database with the cache
		opts.set_block_based_table_factory(&block);
		opts.set_blob_cache(&cache);
		opts.set_row_cache(&cache);
		// Create a new write buffer manager with the cache
		let write_buffer_manager = WriteBufferManager::new_write_buffer_manager_with_cache(
			total_memory_limit,
			true,
			cache.clone(),
		);
		// Set the write buffer manager in the options
		opts.set_write_buffer_manager(&write_buffer_manager);
		// Continue
		Ok(Self {
			write_buffer_manager,
			cache,
		})
	}
}
