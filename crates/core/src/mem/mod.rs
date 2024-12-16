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
pub static ALLOC: std::alloc::System = std::alloc::System;

#[cfg(feature = "allocator")]
#[cfg(target_os = "android")]
#[global_allocator]
pub static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[cfg(feature = "allocator")]
#[cfg(target_os = "freebsd")]
#[global_allocator]
pub static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[cfg(feature = "allocator")]
#[cfg(target_os = "ios")]
#[global_allocator]
pub static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "allocator")]
#[cfg(target_os = "linux")]
#[global_allocator]
pub static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "allocator")]
#[cfg(target_os = "macos")]
#[global_allocator]
pub static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "allocator")]
#[cfg(target_os = "netbsd")]
#[global_allocator]
pub static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[cfg(feature = "allocator")]
#[cfg(target_os = "openbsd")]
#[global_allocator]
pub static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;
