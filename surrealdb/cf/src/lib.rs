//! A SurrealDB node for a Cloudflare Durable Object: the engine with the
//! bytecode VM, over a log ordered by a sequencer.
//!
//! A [`NodeCore`] is one datastore. Its storage is a [`log::Replica`] the host
//! fills by applying the log (from R2 and from its gateway), and its commits
//! go to the host's commit function, which sends them to the sequencer. Each
//! client connection is a [`ConnectionCore`]: a session of its own over the
//! node's datastore, speaking the RPC protocol the SDKs speak over a WebSocket.
//!
//! The Rust API is the [`node`] module. The `js` feature, on by default, adds
//! the `wasm-bindgen` exports a JavaScript host drives it with (module `js`:
//! the classes `Node`, `Connection` and `Frames`, and free functions). A Rust
//! host builds with `default-features = false` and links none of them.
#![recursion_limit = "256"]

/// The global allocator: the standard one (dlmalloc), counted, so the host can see how many
/// allocations and bytes a piece of work took ([`alloc_counts`]). Counting is two relaxed adds on a
/// single thread. dlmalloc stays because its cost does not grow as an executor's long-lived heap
/// fragments, which a free-list allocator's search does.
#[global_allocator]
static ALLOCATOR: Counting = Counting(std::alloc::System);

struct Counting(std::alloc::System);

/// Frees by size: under 256 bytes, under 4 KiB, under 64 KiB, and larger. Measures what a commit's
/// drop frees (the host's `/stats` reports them).
static FREES: [std::sync::atomic::AtomicU64; 4] = [const { std::sync::atomic::AtomicU64::new(0) }; 4];

static ALLOCS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static ALLOC_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Bytes allocated and not yet freed: the heap in use (wasm memory only grows, so its size is a
/// high-water mark, not this).
static LIVE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

// SAFETY: forwards every call to the system allocator, counting on the way.
unsafe impl std::alloc::GlobalAlloc for Counting {
	unsafe fn alloc(&self, l: std::alloc::Layout) -> *mut u8 {
		use std::sync::atomic::Ordering::Relaxed;
		ALLOCS.fetch_add(1, Relaxed);
		ALLOC_BYTES.fetch_add(l.size() as u64, Relaxed);
		LIVE.fetch_add(l.size() as u64, Relaxed);
		unsafe { self.0.alloc(l) }
	}
	unsafe fn dealloc(&self, p: *mut u8, l: std::alloc::Layout) {
		let class = match l.size() {
			0..256 => 0,
			256..4096 => 1,
			4096..65536 => 2,
			_ => 3,
		};
		FREES[class].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
		LIVE.fetch_sub(l.size() as u64, std::sync::atomic::Ordering::Relaxed);
		unsafe { self.0.dealloc(p, l) }
	}
	unsafe fn realloc(&self, p: *mut u8, l: std::alloc::Layout, n: usize) -> *mut u8 {
		use std::sync::atomic::Ordering::Relaxed;
		ALLOCS.fetch_add(1, Relaxed);
		ALLOC_BYTES.fetch_add(n as u64, Relaxed);
		LIVE.fetch_add(n as u64, Relaxed);
		LIVE.fetch_sub(l.size() as u64, Relaxed);
		unsafe { self.0.realloc(p, l, n) }
	}
}

/// The heap a query may grow the node to by default: past it, a transaction's write or a bounded
/// query's next batch fails with an error, rather than the isolate running out of its 128 MB (which
/// ends the object and every client on it). The rest is the JS heap's and the engine's own. A host
/// sets its own with [`set_heap_budget`] (the Rust node's is derived in its `budget.rs`).
pub const HEAP_BUDGET: u64 = 80 << 20;

static HEAP_LIMIT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(HEAP_BUDGET);

/// Set the live heap, in bytes, past which writes and bounded queries fail.
pub fn set_heap_budget(bytes: u64) {
	HEAP_LIMIT.store(bytes, std::sync::atomic::Ordering::Relaxed);
}

/// The live heap budget in force.
pub fn heap_budget() -> u64 {
	HEAP_LIMIT.load(std::sync::atomic::Ordering::Relaxed)
}

/// The heap in use, in bytes.
pub fn heap_live() -> u64 {
	LIVE.load(std::sync::atomic::Ordering::Relaxed)
}

/// The error a query past [`HEAP_BUDGET`] fails with.
pub fn over_budget_message() -> String {
	format!(
		"This query needs more memory than an executor has ({} MB in use, {} MB allowed): split \
		 the write into smaller transactions, or page the read with LIMIT/START or query_stream",
		heap_live() >> 20,
		heap_budget() >> 20
	)
}

/// Allocations and bytes allocated since the module started: `(count, bytes)`.
pub fn alloc_counts() -> (u64, u64) {
	use std::sync::atomic::Ordering::Relaxed;
	(ALLOCS.load(Relaxed), ALLOC_BYTES.load(Relaxed))
}

/// Frees since the module started, by size: `[<256 B, <4 KiB, <64 KiB, larger]`.
pub fn free_counts() -> [u64; 4] {
	std::array::from_fn(|i| FREES[i].load(std::sync::atomic::Ordering::Relaxed))
}

pub mod ingest;
#[cfg(feature = "js")]
pub mod js;
pub mod log;
pub mod node;

pub use self::node::{ConnectionCore, FramesCore, NodeCore};
