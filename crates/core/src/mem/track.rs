#![cfg(feature = "allocator")]

use std::alloc::{GlobalAlloc, Layout, System};
#[cfg(feature = "allocation-tracking")]
use std::cell::RefCell;
#[cfg(feature = "allocation-tracking")]
use std::ptr::null_mut;
#[cfg(feature = "allocation-tracking")]
use std::sync::atomic::AtomicPtr;
#[cfg(feature = "allocation-tracking")]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "allocation-tracking")]
use std::sync::Mutex;

/// This structure implements a wrapper around the
/// system allocator, or around a user-specified
/// allocator. It tracks the current memory which
/// is allocated, allowing the memory use to be
/// checked at runtime.
#[derive(Debug)]
pub struct TrackAlloc<Alloc = System> {
	alloc: Alloc,
}

impl<A> TrackAlloc<A> {
	#[inline]
	pub const fn new(alloc: A) -> Self {
		Self {
			alloc,
		}
	}
}

impl<A> TrackAlloc<A> {
	/// Returns a tuple with the number of bytes that are allocated to the process,
	/// and the number of threads currently pooled.
	#[cfg(feature = "allocation-tracking")]
	pub fn current_usage(&self) -> (usize, usize) {
		let mut total = 0;
		let mut current = GLOBAL_LIST_HEAD.load(Ordering::Relaxed);
		let mut threads = 0;
		while !current.is_null() {
			unsafe {
				total += (*current).counter.load(Ordering::Relaxed);
				current = (*current).next.load(Ordering::Relaxed);
				threads += 1;
			}
		}
		(total, threads)
	}

	/// Returns the number of bytes that are allocated to the process
	#[cfg(not(feature = "allocation-tracking"))]
	pub fn current_usage(&self) -> (usize, usize) {
		(0, 0)
	}

	/// Checks whether the allocator is above the memory limit threshold
	#[cfg(feature = "allocation-tracking")]
	pub fn is_beyond_threshold(&self) -> bool {
		match *crate::cnf::MEMORY_THRESHOLD {
			0 => false,
			v => self.current_usage().0 > v,
		}
	}

	#[cfg(feature = "allocation-tracking")]
	fn add(size: usize) {
		let node = get_thread_node();
		unsafe {
			(*node).counter.fetch_add(size, Ordering::Relaxed);
		}
	}

	#[cfg(feature = "allocation-tracking")]
	fn sub(size: usize) {
		let node = get_thread_node();
		unsafe {
			(*node).counter.fetch_sub(size, Ordering::Relaxed);
		}
	}

	/// Checks whether the allocator is above the memory limit threshold
	#[cfg(not(feature = "allocation-tracking"))]
	pub fn is_beyond_threshold(&self) -> bool {
		false
	}
}

#[cfg(feature = "allocation-tracking")]
unsafe impl<A: GlobalAlloc> GlobalAlloc for TrackAlloc<A> {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		let ret = self.alloc.alloc(layout);
		if !ret.is_null() {
			Self::add(layout.size());
		}
		ret
	}

	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		let ret = self.alloc.alloc_zeroed(layout);
		if !ret.is_null() {
			Self::add(layout.size());
		}
		ret
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		self.alloc.dealloc(ptr, layout);
		Self::sub(layout.size());
	}

	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		let ret = self.alloc.realloc(ptr, layout, new_size);
		if !ret.is_null() {
			Self::sub(layout.size());
			Self::add(new_size);
		}
		ret
	}
}

/// A node for the linked list of thread counters
#[cfg(feature = "allocation-tracking")]
struct ThreadCounterNode {
	next: AtomicPtr<ThreadCounterNode>,
	counter: AtomicUsize,
}

/// Global linked list head holding references to all thread counters
#[cfg(feature = "allocation-tracking")]
static GLOBAL_LIST_HEAD: AtomicPtr<ThreadCounterNode> = AtomicPtr::new(null_mut());

/// A mutex to protect insertion into the global linked list
#[cfg(feature = "allocation-tracking")]
static GLOBAL_LIST_LOCK: Mutex<()> = Mutex::new(());

/// Thread-local storage for the node pointer for this thread
#[cfg(feature = "allocation-tracking")]
thread_local! {
	static THREAD_NODE: RefCell<*mut ThreadCounterNode> = RefCell::new(null_mut());
}

/// Retrieves the thread's node, creating and registering it if necessary
#[cfg(feature = "allocation-tracking")]
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

#[cfg(not(feature = "allocation-tracking"))]
unsafe impl<A: GlobalAlloc> GlobalAlloc for TrackAlloc<A> {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		self.alloc.alloc(layout)
	}

	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		self.alloc.alloc_zeroed(layout)
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		self.alloc.dealloc(ptr, layout);
	}

	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		self.alloc.realloc(ptr, layout, new_size)
	}
}
