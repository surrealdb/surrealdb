#![cfg(feature = "allocator")]

#[cfg(all(feature = "allocation-tracking", not(target_os = "macos")))]
use parking_lot::Mutex;
use std::alloc::{GlobalAlloc, Layout, System};
#[cfg(all(feature = "allocation-tracking", not(target_os = "macos")))]
use std::cell::Cell;
#[cfg(all(feature = "allocation-tracking", not(target_os = "macos")))]
use std::ptr::NonNull;
#[cfg(all(feature = "allocation-tracking", not(target_os = "macos")))]
use std::sync::atomic::{AtomicIsize, Ordering};

/// This structure implements a wrapper around the
/// system allocator, or around a user-specified
/// allocator. It tracks the current memory which
/// is allocated, allowing the memory use to be
/// checked at runtime.
///
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

#[cfg(any(not(feature = "allocation-tracking"), target_os = "macos"))]
impl<A: GlobalAlloc> TrackAlloc<A> {
	/// Returns a tuple with the current total allocated bytes (summed across all threads),
	/// and the number of threads that have allocated memory.
	pub fn current_usage(&self) -> (usize, usize) {
		(0, 0)
	}

	/// Checks whether the allocator is above the memory limit threshold
	pub fn is_beyond_threshold(&self) -> bool {
		false
	}
}

#[cfg(all(feature = "allocation-tracking", not(target_os = "macos")))]
impl<A: GlobalAlloc> TrackAlloc<A> {
	/// Returns a tuple with the current total allocated bytes (summed across all threads),
	/// and the number of threads that have allocated memory.
	///
	/// We traverse a global linked list of thread nodes.
	/// Each node has a counter of allocated bytes.
	pub fn current_usage(&self) -> (usize, usize) {
		let mut total = 0;
		let mut threads = 0;

		{
			// Acquire the lock here for read access
			let guard = GLOBAL_LIST.lock();
			let mut cur = guard.0;

			while let Some(next) = cur {
				total += unsafe { next.as_ref().counter.load(Ordering::Relaxed) };
				threads += 1;
				cur = unsafe { next.as_ref().next.get() };
			}

			drop(guard);
		}

		// In rare cases, due to concurrent updates or mismatched add/sub calls,
		// the net tracked usage can temporarily go negative.
		// We clamp it to zero so we don't report a negative total.
		let total = total.max(0) as usize;
		(total, threads)
	}

	/// Checks if the current usage exceeds a configured threshold. No tracking if the feature is off.
	pub fn is_beyond_threshold(&self) -> bool {
		match *crate::cnf::MEMORY_THRESHOLD {
			0 => false,
			v => self.current_usage().0 > v,
		}
	}

	fn add(&self, size: usize) {
		Self::with_thread_node(|c| {
			c.fetch_add(size as isize, Ordering::Relaxed);
		});
	}

	fn sub(&self, size: usize) {
		Self::with_thread_node(|c| {
			c.fetch_sub(size as isize, Ordering::Relaxed);
		});
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
	/// - After insertion, the node remains alive until it is explicitly removed.
	///   We remove the node in a controlled manner (in another function) under a global lock,
	///   which guarantees that no other threads see a partially-initialized or freed node.
	///
	/// **Thread Local Storage (TLS):**
	/// - Each thread stores a pointer to its `ThreadCounterNode` in a TLS variable (`THREAD_NODE`).
	/// - The first time this thread calls `with_thread_node()`, we allocate and insert the node.
	/// - Subsequent calls just return the cached pointer. As long as it has not been removed,
	///   this pointer remains valid.
	fn with_thread_node<F>(f: F)
	where
		F: FnOnce(&AtomicIsize),
	{
		// Thread node is fully initialized here because we need a stable location to point to in
		// the list, which cant be retrieved within the thread_local! macro.
		let _ = THREAD_NODE.try_with(|cell| {
			if !cell.initialized.get() {
				cell.initialized.set(true);
				let ptr = NonNull::from(cell);
				let mut guard = GLOBAL_LIST.lock();
				let old_head = guard.0.replace(ptr);
				cell.next.set(old_head);
				drop(guard);
			}

			f(&cell.counter)
		});
	}
}

#[cfg(any(not(feature = "allocation-tracking"), target_os = "macos"))]
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

#[cfg(all(feature = "allocation-tracking", not(target_os = "macos")))]
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

/// The list of tracking threads is protected by a mutex, this mutex only extends protection to the
/// list itself, the counter within the values of the list are not protected and therefore use
/// atomics.
#[cfg(all(feature = "allocation-tracking", not(target_os = "macos")))]
static GLOBAL_LIST: Mutex<ListHead> = Mutex::new(ListHead(None));

#[cfg(all(feature = "allocation-tracking", not(target_os = "macos")))]
thread_local! {
	/// `THREAD_NODE` stores a pointer to this thread's `ThreadCounterNode`.
	/// It's initially null, and once the thread first allocates, we initialize the node and store it here.
	static THREAD_NODE: ThreadCounterNode = const {
		ThreadCounterNode{
			next: Cell::new(None),
			counter: AtomicIsize::new(0),
			initialized: Cell::new(false),
		}
	};
}

/// `ThreadCounterNode` stores:
/// - `next`: pointer to the next node in a singly-linked list of per-thread counters.
/// - `counter`: the number of bytes allocated by the thread associated with this node.
/// - `initialized`: indicates whether this particular node has already been inserted
///   into the global list, ensuring it is only inserted once.
///
/// Each thread gets one `ThreadCounterNode`.
/// The global list is used to sum memory usage.
#[cfg(all(feature = "allocation-tracking", not(target_os = "macos")))]
struct ThreadCounterNode {
	next: Cell<Option<NonNull<ThreadCounterNode>>>,
	counter: AtomicIsize,
	initialized: Cell<bool>,
}

#[cfg(all(feature = "allocation-tracking", not(target_os = "macos")))]
struct ListHead(Option<NonNull<ThreadCounterNode>>);

#[cfg(all(feature = "allocation-tracking", not(target_os = "macos")))]
unsafe impl Sync for ListHead {}
#[cfg(all(feature = "allocation-tracking", not(target_os = "macos")))]
unsafe impl Send for ListHead {}

// Drop impl for ThreadCounterNode removes itself from the list.
#[cfg(all(feature = "allocation-tracking", not(target_os = "macos")))]
impl Drop for ThreadCounterNode {
	fn drop(&mut self) {
		if !self.initialized.get() {
			return;
		}

		let this_ptr = NonNull::from(&*self);

		let mut guard = GLOBAL_LIST.lock();
		let mut cur = guard.0.expect("there should be atleast one value in the list");

		if this_ptr == cur {
			guard.0 = self.next.get();
			return;
		}

		loop {
			// We exists somewhere in the list and cur isn't it so next can't be empty.
			let next = unsafe { cur.as_ref().next.get().unwrap() };
			if this_ptr == next {
				unsafe { cur.as_ref().next.set(self.next.get()) }
				return;
			}
			cur = next;
		}
	}
}
