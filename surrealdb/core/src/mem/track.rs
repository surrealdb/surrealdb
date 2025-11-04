#![cfg(feature = "allocator")]

#[cfg(feature = "allocation-tracking")]
use std::alloc::handle_alloc_error;
use std::alloc::{GlobalAlloc, Layout, System};
#[cfg(feature = "allocation-tracking")]
use std::cell::RefCell;
#[cfg(feature = "allocation-tracking")]
use std::ptr::null_mut;
#[cfg(feature = "allocation-tracking")]
use std::sync::atomic::{AtomicIsize, AtomicPtr, Ordering};

#[cfg(feature = "allocation-tracking")]
use parking_lot::Mutex;

/// This structure implements a wrapper around the
/// system allocator, or around a user-specified
/// allocator. It tracks the current memory which
/// is allocated, allowing the memory use to be
/// checked at runtime.
///
/// # Important Note on Thread Pools
///
/// While this allocator can be used with dynamic thread pools where threads may
/// come and go (such as those created by `tokio::spawn_blocking`), it is
/// critical to call [`stop_tracking`] whenever a thread terminates. Since
/// tracking nodes are never physically removed from the global list, failing to
/// halt a thread’s tracking will leave the node permanently active. Over time,
/// repeatedly spawning and exiting threads without calling [`stop_tracking`]
/// can clutter the global list with stale nodes.
///
/// # Design Note
///
///  Why an explicit `stop_tracking` instead of relying on `Drop`?
///
///  We initially considered implementing `Drop` on `ThreadCounterNode` so that,
///  when the node goes out of scope or the thread terminates, the node would
///  automatically be removed from the global list.
///
///  However, we have to manually allocate the node as part of the global
///  allocator logic. Thus, we cannot rely on the compiler to automatically call
///  `Drop` in every situation (particularly at program shutdown times or in
///  complex allocation/deallocation paths). These constraints make it
/// unreliable  to depend on Rust’s destructor mechanism for a globally
/// allocated structure.
#[derive(Debug)]
pub struct TrackAlloc<Alloc = System> {
	alloc: Alloc,
	#[cfg(feature = "allocation-tracking")]
	node_layout: Layout,
}

impl<A> TrackAlloc<A> {
	#[inline]
	pub const fn new(alloc: A) -> Self {
		Self {
			alloc,
			#[cfg(feature = "allocation-tracking")]
			node_layout: Layout::new::<ThreadCounterNode>(),
		}
	}
}

impl<A: GlobalAlloc> TrackAlloc<A> {
	/// Returns a tuple with the current total allocated bytes (summed across
	/// all threads), and the number of threads that have allocated memory.
	#[cfg(not(feature = "allocation-tracking"))]
	pub fn current_usage(&self) -> (usize, usize) {
		(0, 0)
	}

	/// Checks whether the allocator is above the memory limit threshold
	#[cfg(not(feature = "allocation-tracking"))]
	pub fn is_beyond_threshold(&self) -> bool {
		false
	}

	/// Returns a tuple with the current total allocated bytes (summed across
	/// all threads), and the number of threads that have allocated memory.
	///
	/// We traverse a global linked list of thread nodes.
	/// Each node has a counter of allocated bytes.
	#[cfg(feature = "allocation-tracking")]
	pub fn current_usage(&self) -> (usize, usize) {
		let mut total = 0;
		let mut threads = 0;

		let mut current = GLOBAL_LIST_HEAD.load(Ordering::Relaxed);

		// Acquire the lock here for read access
		let guard = GLOBAL_LIST_LOCK.lock();

		while !current.is_null() {
			unsafe {
				// `current` points to a `ThreadCounterNode` allocated by `self.alloc`.
				// We know it's valid and initialized before being inserted into the list.
				total += (*current).counter.load(Ordering::Relaxed);
				current = (*current).next.load(Ordering::Relaxed);
				threads += 1;
			}
		}

		drop(guard);

		// In rare cases, due to concurrent updates or mismatched add/sub calls,
		// the net tracked usage can temporarily go negative.
		// We clamp it to zero so we don't report a negative total.
		let total = total.max(0) as usize;
		(total, threads)
	}

	/// Checks if the current usage exceeds a configured threshold. No tracking
	/// if the feature is off.
	#[cfg(feature = "allocation-tracking")]
	pub fn is_beyond_threshold(&self) -> bool {
		match *crate::cnf::MEMORY_THRESHOLD {
			0 => false,
			v => self.current_usage().0 > v,
		}
	}

	#[cfg(feature = "allocation-tracking")]
	fn add(&self, size: usize) {
		// Retrieves or initializes this thread's `ThreadCounterNode` and increments its
		// counter.
		let node = self.get_or_create_thread_node();
		unsafe {
			// Using `unsafe` because we are dereferencing a raw pointer.
			// This is safe here because:
			// 1. `node` was allocated and initialized properly.
			// 2. `node` never moves after insertion, and we don't free it.
			(*node).counter.fetch_add(size as isize, Ordering::Relaxed);
		}
	}

	/// Subtracts the specified number of bytes from the current thread's
	/// allocated byte counter, if a tracking node exists.
	///
	/// This method does **not** create a new `ThreadCounterNode` if none is
	/// present. Instead, it immediately returns if the thread is not currently
	/// tracked. For instance, a thread may have already called
	/// `stop_tracking`, yet deallocation requests can still arrive in that
	/// window, so we intentionally avoid re-creating a node that would be
	/// deactivated anyway.
	///
	/// # Behavior
	/// - If the thread has a valid tracking node and is still active, the specified amount is
	///   subtracted from its total allocation counter.
	/// - If no node exists (or tracking has been stopped), this call does nothing.
	/// - Avoiding `get_or_create_thread_node` ensures that no new node is created after
	///   `stop_tracking` has already concluded the tracking for this thread.
	///
	/// # Parameter
	/// - `amount`: The number of bytes to subtract from the currently tracked allocation.
	#[cfg(feature = "allocation-tracking")]
	fn sub(&self, size: usize) {
		THREAD_NODE.with(|cell| {
			let node = *cell.borrow();
			if !node.is_null() {
				unsafe {
					// Same reasoning as in `add()`: pointer is always valid and not moved.
					(*node).counter.fetch_sub(size as isize, Ordering::Relaxed);
				}
			}
		});
	}

	/// Retrieves the thread's node, creating and registering it if necessary.
	///
	/// The `ThreadCounterNode` structure holds a per-thread atomic counter of
	/// allocated bytes and a pointer to the next node in a global
	/// singly-linked list of thread counters.
	///
	/// **Why `unsafe` is used here:**
	/// - We use `unsafe` when we allocate and write to raw pointers. However, this is controlled:
	///   1. We allocate memory with `self.alloc` to avoid recursion, ensuring the allocation does
	///      not go through the tracked allocator and cause infinite recursion.
	///   2. We immediately initialize the newly allocated memory with `node_raw.write(...)`.
	///   3. Once written, we link the node into a global list. Other threads will only see a fully
	///      initialized node because the list insertion is done under a lock.
	/// - After insertion, the node remains alive until it is explicitly removed. We remove the node
	///   in a controlled manner (in another function) under a global lock, which guarantees that no
	///   other threads see a partially-initialized or freed node.
	///
	/// **Thread Local Storage (TLS):**
	/// - Each thread stores a pointer to its `ThreadCounterNode` in a TLS variable (`THREAD_NODE`).
	/// - The first time this thread calls `get_or_create_thread_node()`, we allocate and insert the
	///   node.
	/// - Subsequent calls just return the cached pointer. As long as it has not been removed, this
	///   pointer remains valid.
	#[cfg(feature = "allocation-tracking")]
	fn get_or_create_thread_node(&self) -> *mut ThreadCounterNode {
		THREAD_NODE.with(|cell| {
			let mut node_ptr = *cell.borrow();
			if node_ptr.is_null() {
				// Allocate a new node from the wrapped allocator, bypassing the global
				// allocator to avoid recursion.
				let node_raw =
					unsafe { self.alloc.alloc(self.node_layout) } as *mut ThreadCounterNode;
				if node_raw.is_null() {
					handle_alloc_error(self.node_layout);
				}

				// Safely initialize the memory.
				// This is `unsafe` because we're directly writing into a raw pointer.
				// It's safe because:
				// - The pointer came from a successful allocation of the correct size.
				// - We write a fully initialized `ThreadCounterNode` with known fields.
				unsafe {
					node_raw.write(ThreadCounterNode {
						next: AtomicPtr::new(null_mut()),
						counter: AtomicIsize::new(0),
					});
				}

				// Insert this thread's node into the global list of nodes.
				// We lock here to ensure that no other thread modifies the list concurrently,
				// guaranteeing that when the node is visible to other threads, it is fully
				// initialized.
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

				// Store the node pointer in thread-local storage for fast access in future
				// calls.
				*cell.borrow_mut() = node_raw;
				node_ptr = node_raw;
			}
			node_ptr
		})
	}

	/// Removes the `ThreadCounterNode` associated with the current thread, if
	/// one exists.
	///
	/// This method unlinks the node from the global linked list and marks it
	/// for deallocation, ensuring that no other thread sees a
	/// partially-initialized or freed node. The operation is performed under a
	/// global lock, so it is safe to call even if multiple threads attempt
	/// removals concurrently.
	///
	/// # Behavior
	/// - If the thread has not yet created a node or has already removed it, this function does
	///   nothing.
	/// - Otherwise, it removes the node from the global list, preventing further tracking for this
	///   thread.
	/// - The TLS pointer is cleared so the thread will not see the old node again if
	///   `get_or_create_thread_node()` is called later.
	///
	/// # Safety
	/// - Removal must be performed while holding the global lock; otherwise, other threads could
	///   observe inconsistent or invalid data.
	/// - The pointer to the node in TLS becomes invalid as soon as removal succeeds, so it must not
	///   be used afterward.
	#[cfg(feature = "allocation-tracking")]
	fn remove_tracking(&self, node: *mut ThreadCounterNode) {
		// We lock here to ensure that no other thread modifies the list concurrently,
		let guard = GLOBAL_LIST_LOCK.lock();
		// Load the head of the list
		let mut current = GLOBAL_LIST_HEAD.load(Ordering::Relaxed);
		let mut prev: *mut ThreadCounterNode = null_mut();
		// Traverse the list until we find the node or reach the end
		while !current.is_null() {
			if std::ptr::eq(current, node) {
				// Found the node to remove
				let next = unsafe { (*current).next.load(Ordering::Relaxed) };

				if prev.is_null() {
					// Removing the head node
					GLOBAL_LIST_HEAD.store(next, Ordering::Relaxed);
				} else {
					// Link the previous node to the next node in the chain
					unsafe {
						(*prev).next.store(next, Ordering::Relaxed);
					}
				}
				unsafe {
					self.alloc.dealloc(node as *mut u8, self.node_layout);
				}
				// We can break here since we've successfully removed the node
				break;
			}
			// Move to the next node
			prev = current;
			current = unsafe { (*current).next.load(Ordering::Relaxed) };
		}
		drop(guard);
	}

	/// Stops memory tracking for the current thread, finalizing any bookkeeping
	/// and preventing further updates.
	///
	/// This function is intended to be called as a thread is shutting down.
	/// Once invoked, no subsequent allocations or deallocations in this thread
	/// will be tracked. There is no corresponding "resume" function because
	/// the thread is assumed to be terminating.
	///
	/// # Behavior
	/// - If the thread has not created a tracking node or has already stopped tracking, this
	///   function does nothing.
	/// - Otherwise, the thread's node is marked so that no further updates are recorded. As this
	///   typically happens right before termination, it effectively concludes tracking for the
	///   thread's lifetime.
	/// - The node remains in the global list until it is fully removed, but it becomes effectively
	///   inert, as the thread will not resume execution.
	///
	/// # Usage
	/// A common usage scenario in asynchronous runtimes (like Tokio) is to
	/// invoke this method in a thread shutdown callback (e.g.,
	/// `on_thread_stop`) to ensure tracking is cleanly deactivated before the
	/// thread exits.
	#[cfg(feature = "allocation-tracking")]
	pub fn stop_tracking(&self) {
		THREAD_NODE.with(|cell| {
			let node = *cell.borrow_mut();
			if !node.is_null() {
				self.remove_tracking(node);
				*cell.borrow_mut() = null_mut();
			}
		});
	}
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
		let ret = unsafe { self.alloc.alloc(layout) };
		if !ret.is_null() {
			self.add(layout.size());
		}
		ret
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		unsafe { self.alloc.dealloc(ptr, layout) };
		self.sub(layout.size());
	}

	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		let ret = unsafe { self.alloc.alloc_zeroed(layout) };
		if !ret.is_null() {
			self.add(layout.size());
		}
		ret
	}

	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
		let ret = unsafe { self.alloc.realloc(ptr, layout, new_size) };
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

/// `GLOBAL_LIST_HEAD` points to the start of the linked list of
/// `ThreadCounterNode`s. Each node is appended at initialization time for each
/// thread, never removed.
#[cfg(feature = "allocation-tracking")]
static GLOBAL_LIST_HEAD: AtomicPtr<ThreadCounterNode> = AtomicPtr::new(null_mut());

/// A lock to ensure that only one thread at a time modifies the global list of
/// nodes. We use `parking_lot::Mutex` because it's known not to allocate at
/// runtime.
#[cfg(feature = "allocation-tracking")]
static GLOBAL_LIST_LOCK: Mutex<()> = Mutex::new(());

#[cfg(feature = "allocation-tracking")]
thread_local! {
	/// `THREAD_NODE` stores a pointer to this thread's `ThreadCounterNode`.
	/// It's initially null, and once the thread first allocates, we initialize the node and store it here.
	static THREAD_NODE: RefCell<*mut ThreadCounterNode> = const {RefCell::new(null_mut())};
}
