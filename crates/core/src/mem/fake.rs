#![cfg(not(feature = "allocator"))]

/// This structure implements a wrapper around the
/// system allocator, or around a user-specified
/// allocator. It tracks the current memory which
/// is allocated, and the total memory allocated
/// across the duration of the programme. This
/// memory use can then be checked at runtime.
#[derive(Debug)]
pub struct FakeAlloc;

impl FakeAlloc {
	#[inline]
	pub const fn new() -> Self {
		Self {}
	}
}

impl FakeAlloc {
	/// Returns the number of bytes that are allocated to the process
	pub fn current_usage(&self) -> usize {
		0
	}
	/// Returns the total number of bytes allocated since startup
	pub fn total_usage(&self) -> usize {
		0
	}
	/// Returns the amount of memory (in KiB) that is currently allocated
	pub fn current_usage_as_kb(&self) -> f32 {
		0.0
	}
	/// Returns the amount of memory (in MiB) that is currently allocated
	pub fn current_usage_as_mb(&self) -> f32 {
		0.0
	}
	/// Returns the amount of memory (in GiB) that is currently allocated
	pub fn current_usage_as_gb(&self) -> f32 {
		0.0
	}
	/// Returns the total amount of memory (in KiB) allocated since startup
	pub fn total_usage_as_kb(&self) -> f32 {
		0.0
	}
	/// Returns the total amount of memory (in MiB) allocated since startup
	pub fn total_usage_as_mb(&self) -> f32 {
		0.0
	}
	/// Returns the total amount of memory (in GiB) allocated since startup
	pub fn total_usage_as_gb(&self) -> f32 {
		0.0
	}
	/// Checks whether the allocator is above the memory limit threshold
	pub async fn is_beyond_threshold(&self) -> bool {
		false
	}
}
