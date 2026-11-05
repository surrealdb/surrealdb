#![cfg(all(feature = "allocator", feature = "allocation-tracking"))]

use std::alloc::{Layout, System};

/// Reports the bytes an allocator reserves to satisfy a [`Layout`].
///
/// Allocators round a request up to a size class, so [`Layout::size`]
/// understates what a live allocation occupies. Tracking the reserved size
/// instead makes the tracked total reflect what the allocator holds, which is
/// the quantity a memory threshold has to compare against.
///
/// The answer is derived from the layout alone, never from a live allocation.
/// That matters three ways:
///
/// - it costs no pointer-to-metadata lookup, so it leaves the allocator's sized-deallocation fast
///   path intact;
/// - the same layout always yields the same value, and the deallocation path receives the layout
///   the allocation was made with, so the two counters cannot drift;
/// - the value is a function of the request, so an identical workload reports an identical figure.
pub trait ReservedSize {
	/// The bytes this allocator reserves for `layout`.
	fn reserved_size(&self, layout: Layout) -> usize;
}

/// The system allocator exposes no portable way to predict a size class, so the
/// requested size is the best available answer. Tracked totals on these targets
/// keep understating the heap by whatever the platform allocator rounds up.
impl ReservedSize for System {
	#[inline]
	fn reserved_size(&self, layout: Layout) -> usize {
		layout.size()
	}
}

/// Alignment below which the C malloc APIs already return suitably aligned
/// memory without an explicit alignment flag, for a request at least that
/// large. `_Alignof(max_align_t)` is 16 on every architecture this arm is
/// compiled for.
#[cfg(all(
	any(target_arch = "x86_64", target_arch = "x86"),
	any(target_os = "linux", target_os = "macos"),
	not(target_env = "msvc"),
))]
const ALIGNOF_MAX_ALIGN_T: usize = 16;

#[cfg(all(
	any(target_arch = "x86_64", target_arch = "x86"),
	any(target_os = "linux", target_os = "macos"),
	not(target_env = "msvc"),
))]
impl ReservedSize for jemallocator::Jemalloc {
	#[inline]
	fn reserved_size(&self, layout: Layout) -> usize {
		// Mirrors the flag the allocator itself passes for this layout: an
		// explicit alignment is only needed when the request cannot already be
		// satisfied by natural alignment.
		let flags = if layout.align() <= ALIGNOF_MAX_ALIGN_T && layout.align() <= layout.size() {
			0
		} else {
			jemalloc_sys::MALLOCX_ALIGN(layout.align())
		};
		// `nallocx` computes the size class arithmetically — no allocation, no
		// lock, and no metadata access — so it is safe to call from inside the
		// global allocator. It answers 0 for a request it could not serve,
		// which is not a size the caller can act on; fall back to the request.
		let reserved = unsafe { jemalloc_sys::nallocx(layout.size(), flags) };
		if reserved == 0 {
			layout.size()
		} else {
			reserved
		}
	}
}

/// Alignment up to which mimalloc satisfies a request from a natural block.
/// Above it, mimalloc over-allocates and returns an interior pointer, so the
/// block it reserves covers the padding as well as the payload.
#[cfg(all(
	any(unix, windows),
	not(all(
		any(target_arch = "x86_64", target_arch = "x86"),
		any(target_os = "linux", target_os = "macos"),
		not(target_env = "msvc"),
	)),
))]
const MI_MAX_ALIGN_SIZE: usize = 16;

#[cfg(all(
	any(unix, windows),
	not(all(
		any(target_arch = "x86_64", target_arch = "x86"),
		any(target_os = "linux", target_os = "macos"),
		not(target_env = "msvc"),
	)),
))]
impl ReservedSize for mimalloc::MiMalloc {
	#[inline]
	fn reserved_size(&self, layout: Layout) -> usize {
		// An over-aligned request is served by over-allocating and handing back
		// an interior pointer, so the reserved block is sized from the padded
		// request rather than the payload. This reproduces the allocator's own
		// arithmetic for that case; querying the returned pointer instead would
		// report the block minus the alignment adjustment, understating it by
		// an amount that varies per allocation.
		let size = if layout.align() > MI_MAX_ALIGN_SIZE {
			layout.size().max(MI_MAX_ALIGN_SIZE).saturating_add(layout.align().saturating_sub(1))
		} else {
			layout.size()
		};
		// `mi_good_size` is a pure size-class computation, so it neither
		// allocates nor takes a lock and is safe to call from inside the global
		// allocator.
		//
		// It is not exact at both ends of the range. Below mimalloc's smallest
		// bin it answers the request rather than the bin, understating a
		// handful of bytes per allocation; for an over-aligned request the
		// padding folded in above is a worst case, so where the natural block
		// was already aligned the answer overstates by up to `align - 1`. Both
		// errors are deterministic functions of the layout, which is the
		// property the counters depend on.
		unsafe { mimalloc_sys::mi_good_size(size) }
	}
}
