use criterion::{criterion_group, criterion_main, Criterion};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::cell::RefCell;
use std::hint::black_box;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Mutex;

trait BenchAllocator: Send + Sync {
	fn alloc(&self, size: usize);
	fn dealloc(&self, size: usize);

	fn total_usage(&self) -> usize;
}

struct AtomicAllocator(AtomicUsize);

impl BenchAllocator for AtomicAllocator {
	fn alloc(&self, size: usize) {
		self.0.fetch_add(size, Ordering::Relaxed);
	}

	fn dealloc(&self, size: usize) {
		self.0.fetch_sub(size, Ordering::Relaxed);
	}

	fn total_usage(&self) -> usize {
		self.0.load(Ordering::Relaxed)
	}
}

// A node for the linked list of thread counters
struct ThreadCounterNode {
	next: AtomicPtr<ThreadCounterNode>,
	counter: AtomicUsize,
}

// Global linked list head holding references to all thread counters
static GLOBAL_LIST_HEAD: AtomicPtr<ThreadCounterNode> = AtomicPtr::new(null_mut());

// A mutex to protect insertion into the global linked list
static GLOBAL_LIST_LOCK: Mutex<()> = Mutex::new(());

// Thread-local storage for the node pointer for this thread
thread_local! {
	static THREAD_NODE: RefCell<*mut ThreadCounterNode> = RefCell::new(null_mut());
}

// Retrieves the thread's node, creating and registering it if necessary
fn get_thread_node() -> *mut ThreadCounterNode {
	THREAD_NODE.with(|cell| {
		let mut node_ptr = *cell.borrow();
		if node_ptr.is_null() {
			// Create a new node for this thread
			let node = Box::new(ThreadCounterNode {
				next: AtomicPtr::new(null_mut()),
				counter: AtomicUsize::new(0),
			});

			let node_raw = Box::into_raw(node);

			// Insert this thread's node into the global list
			{
				let _guard = GLOBAL_LIST_LOCK.lock().unwrap();
				let head = GLOBAL_LIST_HEAD.load(Ordering::Relaxed);
				unsafe {
					(*node_raw).next.store(head, Ordering::Relaxed);
				}
				GLOBAL_LIST_HEAD.store(node_raw, Ordering::Relaxed);
			}

			*cell.borrow_mut() = node_raw;
			node_ptr = node_raw;
		}
		node_ptr
	})
}

struct PerThreadBenchAllocator;

impl BenchAllocator for PerThreadBenchAllocator {
	fn alloc(&self, size: usize) {
		let node = get_thread_node();
		unsafe {
			(*node).counter.fetch_add(size, Ordering::Relaxed);
		}
	}

	fn dealloc(&self, size: usize) {
		let node = get_thread_node();
		unsafe {
			(*node).counter.fetch_sub(size, Ordering::Relaxed);
		}
	}
	fn total_usage(&self) -> usize {
		let mut total = 0;
		let mut current = GLOBAL_LIST_HEAD.load(Ordering::Relaxed);

		while !current.is_null() {
			unsafe {
				total += (*current).counter.load(Ordering::Relaxed);
				current = (*current).next.load(Ordering::Relaxed);
			}
		}

		total
	}
}

fn bench_alloc<T: BenchAllocator>(c: &mut Criterion, count: usize, bench_name: &str, allocator: T) {
	c.bench_function(&bench_name, |b| {
		b.iter(|| {
			let r = (0..count)
				.into_par_iter()
				.map(|i| {
					allocator.alloc(i);
					allocator.dealloc(i);
				})
				.count();
			black_box(r);
		})
	});
	assert_eq!(allocator.total_usage(), 0);
}

fn bench_atomic_allocator(c: &mut Criterion) {
	bench_alloc(c, 1000000, "atomic_allocator", AtomicAllocator(AtomicUsize::new(0)));
}

fn bench_thread_local_allocator(c: &mut Criterion) {
	bench_alloc(c, 1000000, "thread_local_allocator", PerThreadBenchAllocator {});
}

criterion_group!(benches, bench_atomic_allocator, bench_thread_local_allocator);
criterion_main!(benches);
