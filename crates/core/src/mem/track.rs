#![cfg(feature = "allocator")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::LazyLock;

/// This atomic counter monitors the amount of memory
/// (in bytes) that is currently allocated for this
/// process at this time.
static CURRENT: AtomicUsize = AtomicUsize::new(0);

/// An environment variable for configuring the memory
/// threshold which can be used across the programme
/// to check if the amount of available memory is low.
/// The value can be specified as bytes (b, or without
/// any suffix), kibibytes (k, kb, or kib), mebibytes
/// (m, mb, or mib), or gibibytes (g, gb, or gib).
static MEMORY_THRESHOLD: LazyLock<usize> = std::sync::LazyLock::new(|| {
	std::env::var("SURREAL_MEMORY_THRESHOLD")
		.map(|input| {
			// Trim the input of any spaces
			let input = input.trim();
			// Check if this contains a suffix
			let split = input.find(|c: char| !c.is_ascii_digit());
			// Split the value into number and suffix
			let parts = match split {
				Some(index) => input.split_at(index),
				None => (input, ""),
			};
			// Parse the number as a positive number
			let number = parts.0.parse::<usize>().unwrap_or_default();
			// Parse the supplied suffix as a multiplier
			let suffix = match parts.1.trim().to_lowercase().as_str() {
				"" | "b" => 1,
				"k" | "kb" | "kib" => 1024,
				"m" | "mb" | "mib" => 1024 * 1024,
				"g" | "gb" | "gib" => 1024 * 1024 * 1024,
				_ => 1,
			};
			// Multiply the input by the suffix
			let bytes = number.checked_mul(suffix).unwrap_or_default();
			// Log the parsed memory threshold
			debug!("Memory threshold guide: {input} ({bytes} bytes)");
			// Return the total byte threshold
			bytes
		})
		.unwrap_or(0)
});

/// This structure implements a wrapper around the
/// system allocator, or around a user-specified
/// allocator. It tracks the current memory which
/// is allocated, allowing the memory use to be
/// checked at runtime.
#[derive(Debug)]
pub struct TrackAlloc<Alloc = System> {
	alloc: Alloc,
}

impl<A> TrackAlloc<A> {
	#[inline]
	pub const fn new(alloc: A) -> Self {
		Self {
			alloc,
		}
	}
}

impl<A> TrackAlloc<A> {
	/// Returns the number of bytes that are allocated to the process
	pub fn current_usage(&self) -> usize {
		CURRENT.load(Ordering::Relaxed)
	}
	/// Returns the amount of memory (in KiB) that is currently allocated
	pub fn current_usage_as_kb(&self) -> f32 {
		Self::kb(self.current_usage())
	}
	/// Returns the amount of memory (in MiB) that is currently allocated
	pub fn current_usage_as_mb(&self) -> f32 {
		Self::mb(self.current_usage())
	}
	/// Returns the amount of memory (in GiB) that is currently allocated
	pub fn current_usage_as_gb(&self) -> f32 {
		Self::gb(self.current_usage())
	}
	/// Checks whether the allocator is above the memory limit threshold
	pub async fn is_beyond_threshold(&self) -> bool {
		match *MEMORY_THRESHOLD {
			0 => false,
			v => self.current_usage() > v,
		}
	}
	/// Performs the bytes to kibibytes conversion
	fn kb(x: usize) -> f32 {
		x as f32 / 1024.0
	}
	/// Performs the bytes to mebibytes conversion
	fn mb(x: usize) -> f32 {
		x as f32 / (1024.0 * 1024.0)
	}
	/// Performs the bytes to gibibytes conversion
	fn gb(x: usize) -> f32 {
		x as f32 / (1024.0 * 1024.0 * 1024.0)
	}
}

unsafe impl<A: GlobalAlloc> GlobalAlloc for TrackAlloc<A> {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		let ret = self.alloc.alloc(layout);
		if !ret.is_null() {
			CURRENT.fetch_add(layout.size(), Ordering::Relaxed);
		}
		ret
	}

	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		let ret = self.alloc.alloc_zeroed(layout);
		if !ret.is_null() {
			CURRENT.fetch_add(layout.size(), Ordering::Relaxed);
		}
		ret
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
		self.alloc.dealloc(ptr, layout);
	}

	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		let ret = self.alloc.realloc(ptr, layout, new_size);
		if !ret.is_null() {
			CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
			CURRENT.fetch_add(new_size, Ordering::Relaxed);
		}
		ret
	}
}
