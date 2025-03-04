mod fake;
mod track;

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
#[cfg(target_os = "android")]
#[global_allocator]
pub static ALLOC: track::TrackAlloc<jemallocator::Jemalloc> =
	track::TrackAlloc::new(jemallocator::Jemalloc);

#[cfg(feature = "allocator")]
#[cfg(target_os = "freebsd")]
#[global_allocator]
pub static ALLOC: track::TrackAlloc<jemallocator::Jemalloc> =
	track::TrackAlloc::new(jemallocator::Jemalloc);

#[cfg(feature = "allocator")]
#[cfg(target_os = "ios")]
#[global_allocator]
pub static ALLOC: track::TrackAlloc<mimalloc::MiMalloc> =
	track::TrackAlloc::new(mimalloc::MiMalloc);

#[cfg(feature = "allocator")]
#[cfg(target_os = "linux")]
#[global_allocator]
pub static ALLOC: track::TrackAlloc<mimalloc::MiMalloc> =
	track::TrackAlloc::new(mimalloc::MiMalloc);

#[cfg(feature = "allocator")]
#[cfg(target_os = "macos")]
#[global_allocator]
pub static ALLOC: track::TrackAlloc<mimalloc::MiMalloc> =
	track::TrackAlloc::new(mimalloc::MiMalloc);

#[cfg(feature = "allocator")]
#[cfg(target_os = "netbsd")]
#[global_allocator]
pub static ALLOC: track::TrackAlloc<jemallocator::Jemalloc> =
	track::TrackAlloc::new(jemallocator::Jemalloc);

#[cfg(feature = "allocator")]
#[cfg(target_os = "openbsd")]
#[global_allocator]
pub static ALLOC: track::TrackAlloc<jemallocator::Jemalloc> =
	track::TrackAlloc::new(jemallocator::Jemalloc);
