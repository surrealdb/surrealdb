#![cfg(feature = "allocator")]

#[cfg(feature = "allocation-tracking")]
use parking_lot::Mutex;
use std::alloc::{GlobalAlloc, Layout, System};
#[cfg(feature = "allocation-tracking")]
use std::cell::RefCell;
#[cfg(feature = "allocation-tracking")]
use std::ptr::null_mut;
#[cfg(feature = "allocation-tracking")]
use std::sync::atomic::AtomicPtr;
#[cfg(feature = "allocation-tracking")]
use std::sync::atomic::{AtomicUsize, Ordering};

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

impl<A: GlobalAlloc> TrackAlloc<A> {
	/// Returns a tuple with the current total allocated bytes (summed across all threads),
	/// and the number of threads that have allocated memory.
	///
	/// - We only sum if `ENABLE_THREAD_ALLOC` is set, meaning thread-local tracking is enabled.
	/// - We traverse a global linked list of thread nodes.
	/// Each node has a counter of allocated bytes.
	#[cfg(feature = "allocation-tracking")]
	pub fn current_usage(&self) -> (usize, usize) {
		let mut total: usize = 0;
		let mut threads = 0;

		let mut current = GLOBAL_LIST_HEAD.load(Ordering::Relaxed);
		// We do not lock the list here because we assume the list head is stable after insertion.
		// Thread nodes are never removed, only appended.
		// In a more complex scenario, a lock might be needed.
		while !current.is_null() {
			unsafe {
				// `current` points to a `ThreadCounterNode` allocated by `self.alloc`.
				// We know it's valid and initialized before being inserted into the list.
				total += (*current).counter.load(Ordering::Relaxed);
				current = (*current).next.load(Ordering::Relaxed);
				threads += 1;
			}
		}
		(total, threads)
	}

	/// Returns a tuple with the current total allocated bytes (summed across all threads),
	/// and the number of threads that have allocated memory.
	#[cfg(not(feature = "allocation-tracking"))]
	pub fn current_usage(&self) -> (usize, usize) {
		(0, 0)
	}

	/// Checks if the current usage exceeds a configured threshold. No tracking if the feature is off.
	#[cfg(feature = "allocation-tracking")]
	pub fn is_beyond_threshold(&self) -> bool {
		match *crate::cnf::MEMORY_THRESHOLD {
			0 => false,
			v => self.current_usage().0 > v,
		}
	}

	#[cfg(feature = "allocation-tracking")]
	fn add(&self, size: usize) {
		// Retrieves or initializes this thread's `ThreadCounterNode` and increments its counter.
		let node = self.get_thread_node();
		unsafe {
			// Using `unsafe` because we are dereferencing a raw pointer.
			// This is safe here because:
			// 1. `node` was allocated and initialized properly.
			// 2. `node` never moves after insertion, and we don't free it.
			(*node).counter.fetch_add(size, Ordering::Relaxed);
		}
	}

	#[cfg(feature = "allocation-tracking")]
	fn sub(&self, size: usize) {
		let node = self.get_thread_node();
		unsafe {
			// Same reasoning as in `add()`: pointer is always valid and not moved.
			let val = (*node).counter.fetch_sub(size, Ordering::Relaxed);
			// If we subtracted more than we had, reset to 0 to avoid underflow in usage tracking.
			// This scenario indicates a potential double free or logic error, but resetting to 0
			// ensures we don't get nonsensical negative values.
			if size > val {
				(*node).counter.store(0, Ordering::Relaxed);
			}
		}
	}

	/// Retrieves the thread's node, creating and registering it if necessary.
	///
	/// The `ThreadCounterNode` structure holds a per-thread atomic counter of allocated bytes
	/// and a pointer to the next node in a global singly-linked list of thread counters.
	///
	/// **Why `unsafe` is used here:**
	/// - We use `unsafe` when we allocate and write to raw pointers.
	/// However, this is controlled:
	///   1. We allocate memory with `self.alloc` to avoid recursion, ensuring the allocation
	///      does not go through the tracked allocator and cause infinite recursion.
	///   2. We immediately initialize the newly allocated memory with `node_raw.write(...)`.
	///   3. Once written, we link the node into a global list.
	///      Other threads will only see a fully
	///      initialized node because the list insertion is done under a lock.
	/// - After insertion, the node remains alive for the entire program's life.
	///   We never free it, so the pointer is always valid.
	///
	/// **Thread Local Storage (TLS): **
	/// - Each thread stores a pointer to its `ThreadCounterNode` in a TLS variable (`THREAD_NODE`).
	/// - The first time this thread calls `get_thread_node()`, we allocate and insert the node.
	/// - Subsequent calls just return the cached pointer, which is guaranteed to be valid.
	#[cfg(feature = "allocation-tracking")]
	fn get_thread_node(&self) -> *mut ThreadCounterNode {
		THREAD_NODE.with(|cell| {
			let mut node_ptr = *cell.borrow();
			if node_ptr.is_null() {
				// Allocate a new node from the wrapped allocator, bypassing the global allocator to avoid recursion.
				let layout = Layout::new::<ThreadCounterNode>();
				let node_raw = unsafe { self.alloc.alloc(layout) } as *mut ThreadCounterNode;
				if node_raw.is_null() {
					panic!("Failed to allocate ThreadCounterNode");
				}

				// Safely initialize the memory.
				// This is `unsafe` because we're directly writing into a raw pointer.
				// It's safe because:
				// - The pointer came from a successful allocation of the correct size.
				// - We write a fully initialized `ThreadCounterNode` with known fields.
				unsafe {
					node_raw.write(ThreadCounterNode {
						next: AtomicPtr::new(null_mut()),
						counter: AtomicUsize::new(0),
					});
				}

				// Insert this thread's node into the global list of nodes.
				// We lock here to ensure that no other thread modifies the list concurrently,
				// guaranteeing that when the node is visible to other threads, it is fully initialized.
				{
					let guard = GLOBAL_LIST_LOCK.lock();
					let head = GLOBAL_LIST_HEAD.load(Ordering::Relaxed);
					unsafe {
						// The `node_raw` now points to a fully initialized `ThreadCounterNode`.
						// It's safe to store the head in `node_raw.next` because `head` is either
						// null or another valid `ThreadCounterNode` pointer.
						(*node_raw).next.store(head, Ordering::Relaxed);
					}
					// Atomically update the global head to point to this new node.
					GLOBAL_LIST_HEAD.store(node_raw, Ordering::Relaxed);
					drop(guard);
				}

				// Store the node pointer in thread-local storage for fast access in future calls.
				*cell.borrow_mut() = node_raw;
				node_ptr = node_raw;
			}
			node_ptr
		})
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
		// Allocate using the wrapped allocator and then record the allocated size.
		let ret = self.alloc.alloc(layout);
		if !ret.is_null() {
			self.add(layout.size());
		}
		ret
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		self.alloc.dealloc(ptr, layout);
		self.sub(layout.size());
	}

	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		let ret = self.alloc.alloc_zeroed(layout);
		if !ret.is_null() {
			self.add(layout.size());
		}
		ret
	}

	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		let ret = self.alloc.realloc(ptr, layout, new_size);
		if !ret.is_null() {
			// Update counters: remove old size, add new size.
			self.sub(layout.size());
			self.add(new_size);
		}
		ret
	}
}

/// `ThreadCounterNode` stores:
/// - `next`: pointer to the next node in a singly-linked list of per-thread counters.
/// - `counter`: the number of bytes allocated by the thread associated with this node.
///
/// Each thread gets one `ThreadCounterNode`.
/// The global list is used to sum memory usage.
#[cfg(feature = "allocation-tracking")]
struct ThreadCounterNode {
	next: AtomicPtr<ThreadCounterNode>,
	counter: AtomicUsize,
}

/// `GLOBAL_LIST_HEAD` points to the start of the linked list of `ThreadCounterNode`s.
/// Each node is appended at initialization time for each thread, never removed.
#[cfg(feature = "allocation-tracking")]
static GLOBAL_LIST_HEAD: AtomicPtr<ThreadCounterNode> = AtomicPtr::new(null_mut());

/// A lock to ensure that only one thread at a time modifies the global list of nodes.
/// We use `parking_lot::Mutex` because it's known not to allocate at runtime.
#[cfg(feature = "allocation-tracking")]
static GLOBAL_LIST_LOCK: Mutex<()> = Mutex::new(());

#[cfg(feature = "allocation-tracking")]
thread_local! {
	/// `THREAD_NODE` stores a pointer to this thread's `ThreadCounterNode`.
	/// It's initially null, and once the thread first allocates, we initialize the node and store it here.
	static THREAD_NODE: RefCell<*mut ThreadCounterNode> = RefCell::new(null_mut());
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
