#![cfg(not(feature = "allocator"))]

/// This structure implements a wrapper around the
/// system allocator, or around a user-specified
/// allocator. It tracks the current memory which
/// is allocated, and the total memory allocated
/// across the duration of the programme. This
/// memory use can then be checked at runtime.
#[derive(Debug)]
pub struct FakeAlloc;

impl Default for FakeAlloc {
	fn default() -> Self {
		Self::new()
	}
}

impl FakeAlloc {
	#[inline]
	pub const fn new() -> Self {
		Self {}
	}
}

impl FakeAlloc {
	/// Returns the number of bytes that are allocated to the process
	pub fn current_usage(&self) -> (usize, usize) {
		(0, 0)
	}
	/// Checks whether the allocator is above the memory limit threshold
	pub fn is_beyond_threshold(&self) -> bool {
		false
	}
}
