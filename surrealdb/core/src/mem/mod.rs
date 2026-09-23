mod fake;
mod notrack;
mod registry;
mod track;

pub use registry::{
	MemoryReporter, cleanup_memory_reporters, memory_reporters_allocated_by_name,
	memory_reporters_allocated_total, register_memory_reporter,
};

/// The memory ceiling the guard enforces, in bytes, or `0` when it enforces
/// none.
///
/// This is the value [`ALLOC`]'s `is_beyond_threshold` compares tracked bytes
/// against. It is `0` both when `SURREAL_MEMORY_THRESHOLD` is unset or zero and
/// when allocation tracking is not compiled in: without tracking there is no
/// tracked total to compare, so no threshold is ever enforced, and reporting
/// the configured value would present a disabled guard as an active one.
/// Exposed so the observability layer can publish the limit alongside the usage
/// without depending on the configuration module directly.
pub fn memory_threshold() -> usize {
	if cfg!(all(feature = "allocator", feature = "allocation-tracking")) {
		*crate::cnf::MEMORY_THRESHOLD
	} else {
		0
	}
}

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
