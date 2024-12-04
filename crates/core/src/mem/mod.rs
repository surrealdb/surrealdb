mod fake;
mod track;

#[cfg(not(feature = "allocator"))]
pub static ALLOC: fake::FakeAlloc = fake::FakeAlloc::new();

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
