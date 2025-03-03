#![cfg(feature = "allocator")]

use std::alloc::{GlobalAlloc, Layout, System};

#[cfg(feature = "allocation-tracking")]
use parking_lot::Mutex;
#[cfg(feature = "allocation-tracking")]
use std::cell::RefCell;
#[cfg(feature = "allocation-tracking")]
use std::ptr::null_mut;
#[cfg(feature = "allocation-tracking")]
use std::sync::atomic::{AtomicIsize, AtomicPtr, Ordering};

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
	#[cfg(not(feature = "allocation-tracking"))]
	pub fn current_usage(&self) -> (usize, usize) {
		(0, 0)
	}

	/// Checks whether the allocator is above the memory limit threshold
	#[cfg(not(feature = "allocation-tracking"))]
	pub fn is_beyond_threshold(&self) -> bool {
		false
	}

	/// Returns a tuple with the current total allocated bytes (summed across all threads),
	/// and the number of threads that have allocated memory.
	///
	/// We traverse a global linked list of thread nodes.
	/// Each node has a counter of allocated bytes.
	#[cfg(feature = "allocation-tracking")]
	pub fn current_usage(&self) -> (usize, usize) {
		let mut total = 0;
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
		// In rare cases, due to concurrent updates or mismatched add/sub calls,
		// the net tracked usage can temporarily go negative.
		// We clamp it to zero so we don't report a negative total.
		let total = total.max(0) as usize;
		(total, threads)
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
			(*node).counter.fetch_add(size as isize, Ordering::Relaxed);
		}
	}

	#[cfg(feature = "allocation-tracking")]
	fn sub(&self, size: usize) {
		let node = self.get_thread_node();
		unsafe {
			// Same reasoning as in `add()`: pointer is always valid and not moved.
			(*node).counter.fetch_sub(size as isize, Ordering::Relaxed);
		}
	}

	/// Retrieves the thread's node, creating and registering it if necessary.
	///
	/// The `ThreadCounterNode` structure holds a per-thread atomic counter of allocated bytes
	/// and a pointer to the next node in a global singly-linked list of thread counters.
	///
	/// **Why `unsafe` is used here:**
	/// - We use `unsafe` when we allocate and write to raw pointers.
	///   However, this is controlled:
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
			// If we already have a guard, just return the node pointer
			if let Some(ref guard) = *cell.borrow() {
				return guard.0;
			}

			// Otherwise, create and link a new node
			let layout = Layout::new::<ThreadCounterNode>();
			let node_raw = unsafe { self.alloc.alloc(layout) } as *mut ThreadCounterNode;
			if node_raw.is_null() {
				panic!("Failed to allocate ThreadCounterNode");
			}
			unsafe {
				node_raw.write(ThreadCounterNode {
					next: AtomicPtr::new(std::ptr::null_mut()),
					counter: AtomicIsize::new(0),
				});
			}

			// Lock global list, insert at head
			{
				let guard = GLOBAL_LIST_LOCK.lock();
				let head = GLOBAL_LIST_HEAD.load(Ordering::Relaxed);
				unsafe {
					(*node_raw).next.store(head, Ordering::Relaxed);
				}
				GLOBAL_LIST_HEAD.store(node_raw, Ordering::Relaxed);
				drop(guard);
			}

			// Make a guard that removes this node on drop
			let guard = ThreadNodeGuard(node_raw);
			// Store the guard in TLS (so on thread exit, it will drop)
			*cell.borrow_mut() = Some(guard);

			node_raw
		})
	}
}

#[cfg(feature = "allocation-tracking")]
fn remove_thread_node(node_to_remove: *mut ThreadCounterNode) {
	let guard = GLOBAL_LIST_LOCK.lock();

	let mut current = GLOBAL_LIST_HEAD.load(Ordering::Relaxed);
	let mut prev: *mut ThreadCounterNode = null_mut();

	while !current.is_null() {
		if current == node_to_remove {
			let next = unsafe { (*current).next.load(Ordering::Relaxed) };
			if prev.is_null() {
				// Removing the head
				GLOBAL_LIST_HEAD.store(next, Ordering::Relaxed);
			} else {
				// Fix up the previous node’s 'next' pointer
				unsafe {
					(*prev).next.store(next, Ordering::Relaxed);
				}
			}
			break;
		}
		prev = current;
		current = unsafe { (*current).next.load(Ordering::Relaxed) };
	}
	drop(guard);
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
	counter: AtomicIsize,
}

#[cfg(feature = "allocation-tracking")]
struct ThreadNodeGuard(*mut ThreadCounterNode);

#[cfg(feature = "allocation-tracking")]
impl Drop for ThreadNodeGuard {
	fn drop(&mut self) {
		remove_thread_node(self.0);
	}
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
	/// `THREAD_NODE` stores a pointer to this thread's `ThreadNodeGuard`.
	/// It's initially null, and once the thread first allocates, we initialize the node and store it here.
	static THREAD_NODE: RefCell<Option<ThreadNodeGuard>> = const { RefCell::new(None) };
}
