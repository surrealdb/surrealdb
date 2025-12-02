use std::collections::HashMap;
use std::sync::Weak;

use parking_lot::RwLock;

static MEMORY_REPORTERS: RwLock<Vec<MemoryReporterEntry>> = RwLock::new(Vec::new());

pub(crate) fn memory_reporters_allocated_total() -> usize {
	// Acquire the read lock
	let reporters = MEMORY_REPORTERS.read();
	// Get the total memory allocated
	reporters
		.iter()
		.filter_map(|r| r.reporter.upgrade())
		.map(|reporter| reporter.memory_allocated())
		.sum()
}

pub(crate) fn memory_reporters_allocated_by_name() -> HashMap<String, usize> {
	// Acquire the read lock
	let reporters = MEMORY_REPORTERS.read();
	// Create a new HashMap to store the memory allocated by name
	let mut output = HashMap::new();
	// Iterate over the reporters
	for r in reporters.iter() {
		output.insert(
			r.name.clone(),
			r.reporter.upgrade().map(|v| v.memory_allocated()).unwrap_or(0),
		);
	}
	// Return the HashMap
	output
}

pub struct MemoryReporterEntry {
	/// The name of the reporter
	pub name: String,
	/// The weak reference to the reporter
	pub reporter: Weak<dyn MemoryReporter>,
}

/// Trait for objects that can report their memory usage to the global allocator tracker
pub trait MemoryReporter: Send + Sync {
	/// Returns the amount of memory currently allocated by this object
	fn memory_allocated(&self) -> usize;
}

pub fn register_memory_reporter(name: &str, reporter: Weak<dyn MemoryReporter>) {
	// Convert the name to a string
	let name = name.to_string();
	// Acquire the write lock
	let mut reporters = MEMORY_REPORTERS.write();
	// Clean up dead weak references while we're here
	reporters.retain(|r| r.reporter.strong_count() > 0);
	// Add the reporter to the list
	reporters.push(MemoryReporterEntry {
		name,
		reporter,
	});
}

pub fn cleanup_memory_reporters() {
	// Acquire the write lock
	let mut reporters = MEMORY_REPORTERS.write();
	// Clean up dead weak references while we're here
	reporters.retain(|r| r.reporter.strong_count() > 0);
}
