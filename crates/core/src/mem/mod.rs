mod fake;
mod track;

pub use track::{MemoryReporter, cleanup_memory_reporters, register_memory_reporter};

#[cfg(not(feature = "allocator"))]
pub static ALLOC: fake::FakeAlloc = fake::FakeAlloc::new();

#[cfg(feature = "allocator")]
#[cfg(not(any(
	target_os = "android",
	target_os = "freebsd",
	target_os = "ios",
	target_os = "linux",
	target_os = "macos",
	target_os = "netbsd",
	target_os = "openbsd"
)))]
#[global_allocator]
pub static ALLOC: track::TrackAlloc<std::alloc::System> =
	track::TrackAlloc::new(std::alloc::System);

#[cfg(feature = "allocator")]
#[cfg(any(
	target_os = "android",
	target_os = "freebsd",
	target_os = "ios",
	target_os = "linux",
	target_os = "macos",
	target_os = "netbsd",
	target_os = "openbsd"
))]
#[global_allocator]
pub static ALLOC: track::TrackAlloc<jemallocator::Jemalloc> =
	track::TrackAlloc::new(jemallocator::Jemalloc);
