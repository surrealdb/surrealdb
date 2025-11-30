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
	fn current_usage(&self) -> (usize, usize); // (bytes, threads)
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

	fn current_usage(&self) -> (usize, usize) {
		(0, 0)
	}
}

// ============================================================================
// Implementation 1: Simple Atomic Counter
// ============================================================================

struct AtomicAllocator {
	total_bytes: AtomicUsize,
	thread_count: AtomicUsize,
}

impl AtomicAllocator {
	fn new() -> Self {
		Self {
			total_bytes: AtomicUsize::new(0),
			thread_count: AtomicUsize::new(0),
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

	fn current_usage(&self) -> (usize, usize) {
		(self.total_bytes.load(Ordering::Relaxed), self.thread_count.load(Ordering::Relaxed))
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
	active_threads: AtomicUsize,
	global_list_head: AtomicPtr<ThreadCounterNode>,
	global_list_lock: Mutex<()>,
}

impl PerThreadAllocator {
	fn new() -> Self {
		Self {
			node_layout: Layout::new::<ThreadCounterNode>(),
			active_threads: AtomicUsize::new(0),
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

				self.active_threads.fetch_add(1, Ordering::Relaxed);

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

	fn current_usage(&self) -> (usize, usize) {
		let mut total = 0;
		let mut threads = 0;

		let _guard = self.global_list_lock.lock();
		let mut current = self.global_list_head.load(Ordering::Relaxed);

		while !current.is_null() {
			unsafe {
				total += (*current).counter.load(Ordering::Relaxed);
				current = (*current).next.load(Ordering::Relaxed);
				threads += 1;
			}
		}

		(total, threads)
	}
}

// ============================================================================
// Implementation 3: Hierarchical Lock-Free Tracking
// ============================================================================

/// Batch threshold - flush to global every 64KB of delta
const BATCH_THRESHOLD: i64 = 64 * 1024;

struct LockFreeAllocator {
	global_total_bytes: AtomicI64,
	active_thread_count: AtomicUsize,
}

/// Per-thread state for batched updates
struct LockFreeThreadState {
	local_bytes: Cell<i64>,
	global_bytes: *const AtomicI64,
	thread_count: *const AtomicUsize,
}

impl LockFreeThreadState {
	fn new(global_bytes: *const AtomicI64, thread_count: *const AtomicUsize) -> Self {
		unsafe {
			(*thread_count).fetch_add(1, Ordering::Relaxed);
		}
		Self {
			local_bytes: Cell::new(0),
			global_bytes,
			thread_count,
		}
	}

	fn flush_to_global(&self) {
		let delta = self.local_bytes.get();
		if delta != 0 {
			unsafe {
				(*self.global_bytes).fetch_add(delta, Ordering::Relaxed);
			}
			self.local_bytes.set(0);
		}
	}
}

impl Drop for LockFreeThreadState {
	fn drop(&mut self) {
		// Flush any remaining bytes when thread exits
		self.flush_to_global();
		unsafe {
			(*self.thread_count).fetch_sub(1, Ordering::Relaxed);
		}
	}
}

impl LockFreeAllocator {
	fn new() -> Arc<Self> {
		Arc::new(Self {
			global_total_bytes: AtomicI64::new(0),
			active_thread_count: AtomicUsize::new(0),
		})
	}

	fn get_thread_state(&self) -> Option<&'static LockFreeThreadState> {
		thread_local! {
			static THREAD_STATE: RefCell<Option<Box<LockFreeThreadState>>> = const { RefCell::new(None) };
			static INSIDE_ALLOCATOR: Cell<bool> = const { Cell::new(false) };
		}

		// Reentrancy guard - return None to skip tracking
		if INSIDE_ALLOCATOR.with(|flag| flag.get()) {
			return None;
		}

		INSIDE_ALLOCATOR.with(|flag| flag.set(true));

		let state_ref = THREAD_STATE.with(|cell| {
			let mut opt = cell.borrow_mut();
			if opt.is_none() {
				let global_ptr = &self.global_total_bytes as *const AtomicI64;
				let count_ptr = &self.active_thread_count as *const AtomicUsize;
				*opt = Some(Box::new(LockFreeThreadState::new(global_ptr, count_ptr)));
			}
			// SAFETY: We leak the reference here but ThreadState lives until thread exit
			unsafe { &*(opt.as_ref().unwrap().as_ref() as *const LockFreeThreadState) }
		});

		INSIDE_ALLOCATOR.with(|flag| flag.set(false));
		Some(state_ref)
	}
}

impl BenchAllocator for LockFreeAllocator {
	#[inline(always)]
	fn alloc(&self, size: usize) {
		if let Some(state) = self.get_thread_state() {
			// Batch updates locally
			state.local_bytes.set(state.local_bytes.get() + size as i64);

			// Flush to global if batch threshold reached
			if state.local_bytes.get() >= BATCH_THRESHOLD {
				state.flush_to_global();
			}
		}
	}

	#[inline(always)]
	fn dealloc(&self, size: usize) {
		if let Some(state) = self.get_thread_state() {
			state.local_bytes.set(state.local_bytes.get() - size as i64);

			// Flush if significant negative delta
			if state.local_bytes.get() <= -BATCH_THRESHOLD {
				state.flush_to_global();
			}
		}
	}

	fn current_usage(&self) -> (usize, usize) {
		// O(1) operation - just read the global atomic
		let total = self.global_total_bytes.load(Ordering::Relaxed);
		let threads = self.active_thread_count.load(Ordering::Relaxed);
		(total.max(0) as usize, threads)
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
// Main Benchmark Configuration
// ============================================================================

criterion_group!(
	benches,
	bench_single_threaded,
	bench_multi_threaded,
	bench_usage_queries,
	bench_high_contention,
	bench_mixed_workload
);
criterion_main!(benches);
