#![allow(clippy::unwrap_used)]
//! Comprehensive benchmark for allocation tracking strategies
//!
//! This benchmark compares three different implementations:
//! 1. AtomicAllocator - Simple global atomic counter (baseline)
//! 2. PerThreadAllocator - Per-thread nodes with parking_lot::Mutex
//! 3. LockFreeAllocator - Lock-free with batched updates
//!
//! Benchmark scenarios:
//! - Single-threaded: Sequential alloc/dealloc operations
//! - Multi-threaded: Scalability with 1-256 threads
//! - Usage queries: Query latency with 1-1000 active threads
//! - High contention: 128 threads allocating simultaneously
//! - Mixed workload: Concurrent allocations + usage queries

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::{Cell, RefCell};
use std::hint::black_box;
use std::ptr::null_mut;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicPtr, AtomicUsize, Ordering};
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use parking_lot::Mutex;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

/// Trait for benchmarking different allocator tracking strategies
trait BenchAllocator: Send + Sync {
	fn alloc(&self, size: usize);
	fn dealloc(&self, size: usize);
	fn current_usage(&self) -> usize;
}

// ============================================================================
// Implementation 0: No-Op Allocator (Baseline)
// ============================================================================

struct NoOpAllocator;

impl NoOpAllocator {
	fn new() -> Self {
		Self
	}
}

impl BenchAllocator for NoOpAllocator {
	#[inline(always)]
	fn alloc(&self, _size: usize) {
		// No-op: measure cost of just the function call
	}

	#[inline(always)]
	fn dealloc(&self, _size: usize) {
		// No-op: measure cost of just the function call
	}

	fn current_usage(&self) -> usize {
		0
	}
}

// ============================================================================
// Implementation 1: Simple Atomic Counter
// ============================================================================

struct AtomicAllocator {
	total_bytes: AtomicUsize,
}

impl AtomicAllocator {
	fn new() -> Self {
		Self {
			total_bytes: AtomicUsize::new(0),
		}
	}
}

impl BenchAllocator for AtomicAllocator {
	#[inline(always)]
	fn alloc(&self, size: usize) {
		self.total_bytes.fetch_add(size, Ordering::Relaxed);
	}

	#[inline(always)]
	fn dealloc(&self, size: usize) {
		self.total_bytes.fetch_sub(size, Ordering::Relaxed);
	}

	fn current_usage(&self) -> usize {
		self.total_bytes.load(Ordering::Relaxed)
	}
}

// ============================================================================
// Implementation 2: Per-Thread Counter with Global Linked List
// ============================================================================

struct ThreadCounterNode {
	next: AtomicPtr<ThreadCounterNode>,
	counter: AtomicUsize,
}

struct PerThreadAllocator {
	node_layout: Layout,
	global_list_head: AtomicPtr<ThreadCounterNode>,
	global_list_lock: Mutex<()>,
}

impl PerThreadAllocator {
	fn new() -> Self {
		Self {
			node_layout: Layout::new::<ThreadCounterNode>(),
			global_list_head: AtomicPtr::new(null_mut()),
			global_list_lock: Mutex::new(()),
		}
	}

	fn get_thread_node(&self) -> *mut ThreadCounterNode {
		thread_local! {
			static THREAD_NODE: RefCell<*mut ThreadCounterNode> = const { RefCell::new(null_mut()) };
		}

		THREAD_NODE.with(|cell| {
			let mut node_ptr = *cell.borrow();
			if node_ptr.is_null() {
				// Allocate a new node for this thread
				let node_raw = unsafe { System.alloc(self.node_layout) } as *mut ThreadCounterNode;
				if node_raw.is_null() {
					panic!("Failed to allocate ThreadCounterNode");
				}

				// Initialize the newly allocated memory
				unsafe {
					node_raw.write(ThreadCounterNode {
						next: AtomicPtr::new(null_mut()),
						counter: AtomicUsize::new(0),
					});
				}

				// Insert this thread's node into the global list
				{
					let _guard = self.global_list_lock.lock();
					let head = self.global_list_head.load(Ordering::Relaxed);
					unsafe {
						(*node_raw).next.store(head, Ordering::Relaxed);
					}
					self.global_list_head.store(node_raw, Ordering::Relaxed);
				}

				*cell.borrow_mut() = node_raw;
				node_ptr = node_raw;
			}
			node_ptr
		})
	}
}

impl BenchAllocator for PerThreadAllocator {
	#[inline(always)]
	fn alloc(&self, size: usize) {
		let node = self.get_thread_node();
		unsafe {
			(*node).counter.fetch_add(size, Ordering::Relaxed);
		}
	}

	#[inline(always)]
	fn dealloc(&self, size: usize) {
		let node = self.get_thread_node();
		unsafe {
			(*node).counter.fetch_sub(size, Ordering::Relaxed);
		}
	}

	fn current_usage(&self) -> usize {
		let mut total = 0;

		let _guard = self.global_list_lock.lock();
		let mut current = self.global_list_head.load(Ordering::Relaxed);

		while !current.is_null() {
			unsafe {
				total += (*current).counter.load(Ordering::Relaxed);
				current = (*current).next.load(Ordering::Relaxed);
			}
		}

		total
	}
}

// ============================================================================
// Implementation 3: Lock-Free Batched Tracking (matches actual TrackAlloc)
// ============================================================================

/// Batch threshold - flush to global every 12KB of delta
const BATCH_THRESHOLD: i64 = 12 * 1024;

/// Per-thread state for batched updates
struct LockFreeThreadState {
	local_bytes: Cell<i64>,
	global_bytes: Cell<*const AtomicI64>,
}

impl LockFreeThreadState {
	const fn new() -> Self {
		Self {
			local_bytes: Cell::new(0),
			global_bytes: Cell::new(std::ptr::null()),
		}
	}

	fn flush_to_global(&self) {
		let delta = self.local_bytes.get();
		if delta != 0 {
			let global_ptr = self.global_bytes.get();
			if !global_ptr.is_null() {
				unsafe {
					(*global_ptr).fetch_add(delta, Ordering::Relaxed);
				}
			}
			self.local_bytes.set(0);
		}
	}
}

struct LockFreeAllocator {
	global_total_bytes: AtomicI64,
}

impl LockFreeAllocator {
	fn new() -> Arc<Self> {
		Arc::new(Self {
			global_total_bytes: AtomicI64::new(0),
		})
	}

	// Mirrors the actual TrackAlloc::add() implementation
	#[inline(always)]
	fn add(&self, size: usize) {
		thread_local! {
			static THREAD_STATE: LockFreeThreadState = const { LockFreeThreadState::new() };
			static RECURSION_DEPTH: Cell<u32> = const { Cell::new(0) };
		}

		const MAX_DEPTH: u32 = 3;

		let depth = RECURSION_DEPTH.with(|d| {
			let current = d.get();
			if current >= MAX_DEPTH {
				return MAX_DEPTH;
			}
			d.set(current + 1);
			current
		});

		if depth >= MAX_DEPTH {
			return;
		}

		let global_ptr = &self.global_total_bytes as *const AtomicI64;
		THREAD_STATE.with(|state| {
			// Initialize pointer on first use
			if state.global_bytes.get().is_null() {
				state.global_bytes.set(global_ptr);
			}

			let bytes = state.local_bytes.get() + size as i64;
			state.local_bytes.set(bytes);
			if bytes >= BATCH_THRESHOLD {
				state.flush_to_global();
			}
		});

		RECURSION_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
	}

	// Mirrors the actual TrackAlloc::sub() implementation
	#[inline(always)]
	fn sub(&self, size: usize) {
		thread_local! {
			static THREAD_STATE: LockFreeThreadState = const { LockFreeThreadState::new() };
			static RECURSION_DEPTH: Cell<u32> = const { Cell::new(0) };
		}

		const MAX_DEPTH: u32 = 3;

		let depth = RECURSION_DEPTH.with(|d| {
			let current = d.get();
			if current >= MAX_DEPTH {
				return MAX_DEPTH;
			}
			d.set(current + 1);
			current
		});

		if depth >= MAX_DEPTH {
			return;
		}

		let global_ptr = &self.global_total_bytes as *const AtomicI64;
		THREAD_STATE.with(|state| {
			// Initialize pointer on first use
			if state.global_bytes.get().is_null() {
				state.global_bytes.set(global_ptr);
			}

			let bytes = state.local_bytes.get() - size as i64;
			state.local_bytes.set(bytes);
			if bytes <= -BATCH_THRESHOLD {
				state.flush_to_global();
			}
		});

		RECURSION_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
	}
}

impl BenchAllocator for LockFreeAllocator {
	#[inline(always)]
	fn alloc(&self, size: usize) {
		self.add(size);
	}

	#[inline(always)]
	fn dealloc(&self, size: usize) {
		self.sub(size);
	}

	fn current_usage(&self) -> usize {
		self.global_total_bytes.load(Ordering::Relaxed).max(0) as usize
	}
}

// ============================================================================
// Benchmark 1: Single-threaded Performance
// ============================================================================

fn bench_single_threaded(c: &mut Criterion) {
	let mut group = c.benchmark_group("single_threaded");
	group.throughput(Throughput::Elements(1_000_000));

	group.bench_function("noop_alloc_dealloc", |b| {
		let allocator = NoOpAllocator::new();
		b.iter(|| {
			for i in 0..1_000_000 {
				allocator.alloc(black_box(i % 1024));
				allocator.dealloc(black_box(i % 1024));
			}
		});
	});

	group.bench_function("atomic_alloc_dealloc", |b| {
		let allocator = AtomicAllocator::new();
		b.iter(|| {
			for i in 0..1_000_000 {
				allocator.alloc(black_box(i % 1024));
				allocator.dealloc(black_box(i % 1024));
			}
		});
	});

	group.bench_function("perthread_alloc_dealloc", |b| {
		let allocator = PerThreadAllocator::new();
		b.iter(|| {
			for i in 0..1_000_000 {
				allocator.alloc(black_box(i % 1024));
				allocator.dealloc(black_box(i % 1024));
			}
		});
	});

	group.bench_function("lockfree_alloc_dealloc", |b| {
		let allocator = LockFreeAllocator::new();
		b.iter(|| {
			for i in 0..1_000_000 {
				allocator.alloc(black_box(i % 1024));
				allocator.dealloc(black_box(i % 1024));
			}
		});
	});

	group.finish();
}

// ============================================================================
// Benchmark 2: Multi-threaded Alloc/Dealloc
// ============================================================================

fn bench_multi_threaded(c: &mut Criterion) {
	let mut group = c.benchmark_group("multi_threaded");
	group.measurement_time(Duration::from_secs(10));

	for thread_count in [1, 4, 16, 64, 256] {
		let ops_per_thread = 10_000;
		let total_ops = thread_count * ops_per_thread;
		group.throughput(Throughput::Elements(total_ops as u64));

		group.bench_with_input(
			BenchmarkId::new("noop", thread_count),
			&thread_count,
			|b, &threads| {
				let allocator = Arc::new(NoOpAllocator::new());
				b.iter(|| {
					(0..threads).into_par_iter().for_each(|_| {
						for i in 0..ops_per_thread {
							allocator.alloc(black_box(i % 1024));
							allocator.dealloc(black_box(i % 1024));
						}
					});
				});
			},
		);

		group.bench_with_input(
			BenchmarkId::new("atomic", thread_count),
			&thread_count,
			|b, &threads| {
				let allocator = Arc::new(AtomicAllocator::new());
				b.iter(|| {
					(0..threads).into_par_iter().for_each(|_| {
						for i in 0..ops_per_thread {
							allocator.alloc(black_box(i % 1024));
							allocator.dealloc(black_box(i % 1024));
						}
					});
				});
			},
		);

		group.bench_with_input(
			BenchmarkId::new("perthread", thread_count),
			&thread_count,
			|b, &threads| {
				let allocator = Arc::new(PerThreadAllocator::new());
				b.iter(|| {
					(0..threads).into_par_iter().for_each(|_| {
						for i in 0..ops_per_thread {
							allocator.alloc(black_box(i % 1024));
							allocator.dealloc(black_box(i % 1024));
						}
					});
				});
			},
		);

		group.bench_with_input(
			BenchmarkId::new("lockfree", thread_count),
			&thread_count,
			|b, &threads| {
				let allocator = LockFreeAllocator::new();
				b.iter(|| {
					(0..threads).into_par_iter().for_each(|_| {
						for i in 0..ops_per_thread {
							allocator.alloc(black_box(i % 1024));
							allocator.dealloc(black_box(i % 1024));
						}
					});
				});
			},
		);
	}

	group.finish();
}

// ============================================================================
// Benchmark 3: Usage Query Performance
// ============================================================================

fn bench_usage_queries(c: &mut Criterion) {
	let mut group = c.benchmark_group("usage_queries");
	group.measurement_time(Duration::from_secs(10));

	// Pre-warm allocators with different thread counts
	for thread_count in [1, 10, 100, 1000] {
		// No-op allocator
		group.bench_with_input(
			BenchmarkId::new("noop", thread_count),
			&thread_count,
			|b, &threads| {
				let allocator = Arc::new(NoOpAllocator::new());

				// Pre-warm: create thread-local state (no-op in this case)
				(0..threads).into_par_iter().for_each(|_| {
					allocator.alloc(1024);
				});

				b.iter(|| {
					black_box(allocator.current_usage());
				});
			},
		);

		// Atomic allocator
		group.bench_with_input(
			BenchmarkId::new("atomic", thread_count),
			&thread_count,
			|b, &threads| {
				let allocator = Arc::new(AtomicAllocator::new());

				// Pre-warm: create thread-local state
				(0..threads).into_par_iter().for_each(|_| {
					allocator.alloc(1024);
				});

				b.iter(|| {
					black_box(allocator.current_usage());
				});
			},
		);

		// Per-thread allocator
		group.bench_with_input(
			BenchmarkId::new("perthread", thread_count),
			&thread_count,
			|b, &threads| {
				let allocator = Arc::new(PerThreadAllocator::new());

				// Pre-warm: create thread-local state
				(0..threads).into_par_iter().for_each(|_| {
					allocator.alloc(1024);
				});

				b.iter(|| {
					black_box(allocator.current_usage());
				});
			},
		);

		// Lock-free allocator
		group.bench_with_input(
			BenchmarkId::new("lockfree", thread_count),
			&thread_count,
			|b, &threads| {
				let allocator = LockFreeAllocator::new();

				// Pre-warm: create thread-local state
				(0..threads).into_par_iter().for_each(|_| {
					allocator.alloc(1024);
				});

				b.iter(|| {
					black_box(allocator.current_usage());
				});
			},
		);
	}

	group.finish();
}

// ============================================================================
// Benchmark 4: High Contention Scenario
// ============================================================================

fn bench_high_contention(c: &mut Criterion) {
	let mut group = c.benchmark_group("high_contention");
	group.measurement_time(Duration::from_secs(10));
	group.sample_size(50);

	let thread_count = 128;
	let ops_per_thread = 5_000;
	let total_ops = thread_count * ops_per_thread;
	group.throughput(Throughput::Elements(total_ops as u64));

	group.bench_function("noop_128_threads", |b| {
		let allocator = Arc::new(NoOpAllocator::new());
		b.iter(|| {
			(0..thread_count).into_par_iter().for_each(|_| {
				for i in 0..ops_per_thread {
					allocator.alloc(black_box(i % 1024));
					allocator.dealloc(black_box(i % 1024));
				}
			});
		});
	});

	group.bench_function("atomic_128_threads", |b| {
		let allocator = Arc::new(AtomicAllocator::new());
		b.iter(|| {
			(0..thread_count).into_par_iter().for_each(|_| {
				for i in 0..ops_per_thread {
					allocator.alloc(black_box(i % 1024));
					allocator.dealloc(black_box(i % 1024));
				}
			});
		});
	});

	group.bench_function("perthread_128_threads", |b| {
		let allocator = Arc::new(PerThreadAllocator::new());
		b.iter(|| {
			(0..thread_count).into_par_iter().for_each(|_| {
				for i in 0..ops_per_thread {
					allocator.alloc(black_box(i % 1024));
					allocator.dealloc(black_box(i % 1024));
				}
			});
		});
	});

	group.bench_function("lockfree_128_threads", |b| {
		let allocator = LockFreeAllocator::new();
		b.iter(|| {
			(0..thread_count).into_par_iter().for_each(|_| {
				for i in 0..ops_per_thread {
					allocator.alloc(black_box(i % 1024));
					allocator.dealloc(black_box(i % 1024));
				}
			});
		});
	});

	group.finish();
}

// ============================================================================
// Benchmark 5: Mixed Workload (Alloc + Query)
// ============================================================================

fn bench_mixed_workload(c: &mut Criterion) {
	let mut group = c.benchmark_group("mixed_workload");
	group.measurement_time(Duration::from_secs(10));
	group.sample_size(50);

	let alloc_threads = 50;
	let query_threads = 10;
	let ops_per_alloc_thread = 1_000;

	group.bench_function("noop_mixed", |b| {
		let allocator = Arc::new(NoOpAllocator::new());
		let running = Arc::new(AtomicBool::new(false));

		b.iter(|| {
			running.store(true, Ordering::Release);

			let alloc_allocator = allocator.clone();
			let alloc_running = running.clone();
			let alloc_handle = std::thread::spawn(move || {
				(0..alloc_threads).into_par_iter().for_each(|_| {
					for i in 0..ops_per_alloc_thread {
						if !alloc_running.load(Ordering::Acquire) {
							break;
						}
						alloc_allocator.alloc(black_box(i % 1024));
						alloc_allocator.dealloc(black_box(i % 1024));
					}
				});
			});

			let query_allocator = allocator.clone();
			let query_running = running.clone();
			let query_handle = std::thread::spawn(move || {
				(0..query_threads).into_par_iter().for_each(|_| {
					while query_running.load(Ordering::Acquire) {
						black_box(query_allocator.current_usage());
					}
				});
			});

			alloc_handle.join().unwrap();
			running.store(false, Ordering::Release);
			query_handle.join().unwrap();
		});
	});

	group.bench_function("atomic_mixed", |b| {
		let allocator = Arc::new(AtomicAllocator::new());
		let running = Arc::new(AtomicBool::new(false));

		b.iter(|| {
			running.store(true, Ordering::Release);

			let alloc_allocator = allocator.clone();
			let alloc_running = running.clone();
			let alloc_handle = std::thread::spawn(move || {
				(0..alloc_threads).into_par_iter().for_each(|_| {
					for i in 0..ops_per_alloc_thread {
						if !alloc_running.load(Ordering::Acquire) {
							break;
						}
						alloc_allocator.alloc(black_box(i % 1024));
						alloc_allocator.dealloc(black_box(i % 1024));
					}
				});
			});

			let query_allocator = allocator.clone();
			let query_running = running.clone();
			let query_handle = std::thread::spawn(move || {
				(0..query_threads).into_par_iter().for_each(|_| {
					while query_running.load(Ordering::Acquire) {
						black_box(query_allocator.current_usage());
					}
				});
			});

			alloc_handle.join().unwrap();
			running.store(false, Ordering::Release);
			query_handle.join().unwrap();
		});
	});

	group.bench_function("perthread_mixed", |b| {
		let allocator = Arc::new(PerThreadAllocator::new());
		let running = Arc::new(AtomicBool::new(false));

		b.iter(|| {
			running.store(true, Ordering::Release);

			let alloc_allocator = allocator.clone();
			let alloc_running = running.clone();
			let alloc_handle = std::thread::spawn(move || {
				(0..alloc_threads).into_par_iter().for_each(|_| {
					for i in 0..ops_per_alloc_thread {
						if !alloc_running.load(Ordering::Acquire) {
							break;
						}
						alloc_allocator.alloc(black_box(i % 1024));
						alloc_allocator.dealloc(black_box(i % 1024));
					}
				});
			});

			let query_allocator = allocator.clone();
			let query_running = running.clone();
			let query_handle = std::thread::spawn(move || {
				(0..query_threads).into_par_iter().for_each(|_| {
					while query_running.load(Ordering::Acquire) {
						black_box(query_allocator.current_usage());
					}
				});
			});

			alloc_handle.join().unwrap();
			running.store(false, Ordering::Release);
			query_handle.join().unwrap();
		});
	});

	group.bench_function("lockfree_mixed", |b| {
		let allocator = LockFreeAllocator::new();
		let running = Arc::new(AtomicBool::new(false));

		b.iter(|| {
			running.store(true, Ordering::Release);

			let alloc_allocator = allocator.clone();
			let alloc_running = running.clone();
			let alloc_handle = std::thread::spawn(move || {
				(0..alloc_threads).into_par_iter().for_each(|_| {
					for i in 0..ops_per_alloc_thread {
						if !alloc_running.load(Ordering::Acquire) {
							break;
						}
						alloc_allocator.alloc(black_box(i % 1024));
						alloc_allocator.dealloc(black_box(i % 1024));
					}
				});
			});

			let query_allocator = allocator.clone();
			let query_running = running.clone();
			let query_handle = std::thread::spawn(move || {
				(0..query_threads).into_par_iter().for_each(|_| {
					while query_running.load(Ordering::Acquire) {
						black_box(query_allocator.current_usage());
					}
				});
			});

			alloc_handle.join().unwrap();
			running.store(false, Ordering::Release);
			query_handle.join().unwrap();
		});
	});

	group.finish();
}

// ============================================================================
// Nested Allocation Benchmarks
// ============================================================================

fn bench_nested_allocations(c: &mut Criterion) {
	let mut group = c.benchmark_group("nested_allocations");

	// Benchmark 1: Single allocation (baseline)
	group.bench_function("single", |b| {
		b.iter(|| {
			let v = vec![0u8; 1024];
			black_box(v);
		});
	});

	// Benchmark 2: Nested allocation (Vec in Vec)
	group.bench_function("nested_vec", |b| {
		b.iter(|| {
			let outer = vec![vec![0u8; 256]; 4];
			black_box(outer);
		});
	});

	// Benchmark 3: Deep nesting (tests recursion limit)
	group.bench_function("deep_nesting", |b| {
		b.iter(|| {
			let v1 = vec![0u8; 100];
			let v2 = vec![v1.clone(); 2];
			let v3 = vec![v2.clone(); 2];
			black_box(v3);
		});
	});

	group.finish();
}

// ============================================================================
// Main Benchmark Configuration
// ============================================================================

criterion_group!(
	benches,
	bench_single_threaded,
	bench_multi_threaded,
	bench_usage_queries,
	bench_high_contention,
	bench_mixed_workload,
	bench_nested_allocations
);
criterion_main!(benches);
