mod fake;
mod notrack;
mod registry;
mod track;

pub use registry::{cleanup_memory_reporters, register_memory_reporter, MemoryReporter};

// --------------------------------------------------
// No global allocator, or allocation tracking
// --------------------------------------------------

#[cfg(not(feature = "allocator"))]
pub static ALLOC: fake::FakeAlloc = fake::FakeAlloc::new();

// --------------------------------------------------
// Global allocator, with allocation tracking
// --------------------------------------------------

#[cfg(all(
	feature = "allocator",
	feature = "allocation-tracking",
	not(any(unix, windows)),
	not(all(
		any(target_arch = "x86_64", target_arch = "x86"),
		any(target_os = "linux", target_os = "macos"),
		not(target_env = "msvc"),
	))
))]
#[global_allocator]
pub static ALLOC: track::TrackAlloc<std::alloc::System> =
	track::TrackAlloc::new(std::alloc::System);

#[cfg(all(
	feature = "allocator",
	feature = "allocation-tracking",
	any(unix, windows),
	not(all(
		any(target_arch = "x86_64", target_arch = "x86"),
		any(target_os = "linux", target_os = "macos"),
		not(target_env = "msvc"),
	)),
))]
#[global_allocator]
pub static ALLOC: track::TrackAlloc<mimalloc::MiMalloc> =
	track::TrackAlloc::new(mimalloc::MiMalloc);

#[cfg(all(
	feature = "allocator",
	feature = "allocation-tracking",
	all(
		any(target_arch = "x86_64", target_arch = "x86"),
		any(target_os = "linux", target_os = "macos"),
		not(target_env = "msvc"),
	)
))]
#[global_allocator]
pub static ALLOC: track::TrackAlloc<jemallocator::Jemalloc> =
	track::TrackAlloc::new(jemallocator::Jemalloc);

// --------------------------------------------------
// Global allocator, without allocation tracking
// --------------------------------------------------

#[cfg(all(
	feature = "allocator",
	not(feature = "allocation-tracking"),
	not(any(unix, windows)),
	not(all(
		any(target_arch = "x86_64", target_arch = "x86"),
		any(target_os = "linux", target_os = "macos"),
		not(target_env = "msvc"),
	))
))]
#[global_allocator]
pub static ALLOC: notrack::NotrackAlloc<std::alloc::System> =
	notrack::NotrackAlloc::new(std::alloc::System);

#[cfg(all(
	feature = "allocator",
	not(feature = "allocation-tracking"),
	any(unix, windows),
	not(all(
		any(target_arch = "x86_64", target_arch = "x86"),
		any(target_os = "linux", target_os = "macos"),
		not(target_env = "msvc"),
	))
))]
#[global_allocator]
pub static ALLOC: notrack::NotrackAlloc<mimalloc::MiMalloc> =
	notrack::NotrackAlloc::new(mimalloc::MiMalloc);

#[cfg(all(
	feature = "allocator",
	not(feature = "allocation-tracking"),
	all(
		any(target_arch = "x86_64", target_arch = "x86"),
		any(target_os = "linux", target_os = "macos"),
		not(target_env = "msvc"),
	)
))]
#[global_allocator]
pub static ALLOC: notrack::NotrackAlloc<jemallocator::Jemalloc> =
	notrack::NotrackAlloc::new(jemallocator::Jemalloc);
