#![cfg(feature = "allocator")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::ptr::NonNull;

#[cfg(feature = "allocation-tracking")]
use parking_lot::Mutex;
#[cfg(feature = "allocation-tracking")]
use std::sync::atomic::{AtomicIsize, Ordering};

/// This structure implements a wrapper around the
/// system allocator, or around a user-specified
/// allocator. It tracks the current memory which
/// is allocated, allowing the memory use to be
/// checked at runtime.
///
/// # Important Note on Thread Pools
///
/// While this allocator can be used with dynamic thread pools where threads may come
/// and go (such as those created by `tokio::spawn_blocking`), it is critical to call
/// [`stop_tracking`] whenever a thread terminates. Since tracking nodes are never
/// physically removed from the global list, failing to halt a thread’s tracking
/// will leave the node permanently active. Over time, repeatedly spawning and
/// exiting threads without calling [`stop_tracking`] can clutter the global list
/// with stale nodes.
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
///  complex allocation/deallocation paths). These constraints make it unreliable
///  to depend on Rust’s destructor mechanism for a globally allocated structure.
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

#[cfg(not(feature = "allocation-tracking"))]
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
}

#[cfg(feature = "allocation-tracking")]
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

	/// Subtracts the specified number of bytes from the current thread's allocated byte counter,
	/// if a tracking node exists.
	///
	/// This method does **not** create a new `ThreadCounterNode` if none is present. Instead, it
	/// immediately returns if the thread is not currently tracked. For instance, a thread may have
	/// already called `stop_tracking`, yet deallocation requests can still arrive in that window,
	/// so we intentionally avoid re-creating a node that would be deactivated anyway.
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
	/// - The first time this thread calls `get_or_create_thread_node()`, we allocate and insert the node.
	/// - Subsequent calls just return the cached pointer. As long as it has not been removed,
	///   this pointer remains valid.
	fn with_thread_node<F>(f: F)
	where
		F: FnOnce(&AtomicIsize),
	{
		// Thread node is fully initialzed here because we need a stable location to point to in
		// the list, which cant be retrieved within the thread_local! macro.
		let _ = THREAD_NODE.try_with(|cell| {
			cell.link();

			f(&cell.counter)
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
#[cfg(feature = "allocation-tracking")]
static GLOBAL_LIST: Mutex<ListHead> = Mutex::new(ListHead(None));

#[cfg(feature = "allocation-tracking")]
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
///
/// Each thread gets one `ThreadCounterNode`.
/// The global list is used to sum memory usage.
#[cfg(feature = "allocation-tracking")]
struct ThreadCounterNode {
	next: Cell<Option<NonNull<ThreadCounterNode>>>,
	counter: AtomicIsize,
	initialized: Cell<bool>,
}

impl ThreadCounterNode {
	fn link(&self) {
		if !self.initialized.get() {
			// register possible thread exit handlers.
			self.register();
			self.initialized.set(true);
			let ptr = NonNull::from(&*self);
			let mut guard = GLOBAL_LIST.lock();
			let old_head = guard.0.replace(ptr);
			self.next.set(old_head);
			drop(guard);
		}
	}

	fn unlink(&self) {
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
			// We exists somewhere in the lsit and cur isn't it so next can't be empty.
			let next = unsafe { cur.as_ref().next.get().unwrap() };
			if this_ptr == next {
				unsafe { cur.as_ref().next.set(self.next.get()) }
				return;
			}
			cur = next;
		}
	}
}

struct ListHead(Option<NonNull<ThreadCounterNode>>);

unsafe impl Sync for ListHead {}
unsafe impl Send for ListHead {}

// HACK: Below is code to work around the problem that the global allocator can't access thread
// local variables with a drop implementation on some platforms.
//
// The functionality here is based on how the destructors are implemented within the standard
// library.
#[cfg(feature = "allocation-tracking")]
cfg_if::cfg_if! {

	if #[cfg(any(
		target_os = "linux",
		target_os = "android",
		target_os = "fuchsia",
		target_os = "redox",
		target_os = "hurd",
		target_os = "netbsd",
		target_os = "dragonfly",
	))] {
		// For linux like platforms adding a drop implementation seems to work without issue.
		// List taken from standart library at src/sys/thread_local/mod.rs:56

		impl ThreadCounterNode{
			// nothin to do.
			fn register(&self){}
		}

		impl Drop for ThreadCounterNode {
			fn drop(&mut self) {
				self.unlink()
			}
		}
	} else if #[cfg(target_vendor = "apple")] {
		// For mac we hack into the thread exit handler and register an additional function to do the
		// unlinking. This function can be called in any ordering with the rust std thread exit handler
		// so it uses a pointer to the thread local

		// Link into the platform thread atexit handler.
		unsafe extern "C" {
			fn _tlv_atexit(dtor: unsafe extern "C" fn(*mut u8), arg: *mut u8);
		}

		unsafe extern "C" fn run_unlink(ptr: *mut u8) {
			let ptr = NonNull::new(ptr)
				.expect("mem-track thread exit pointer should not be null")
				.cast::<ThreadCounterNode>();
			unsafe { ptr.as_ref().unlink() }
		}

		impl ThreadCounterNode {
			fn register(&self) {
				unsafe { _tlv_atexit(run_unlink, NonNull::from(self).cast().as_ptr()) }
			}
		}

	} else if #[cfg(target_os = "windows")] {

		impl ThreadCounterNode {
			fn register(&self) {
				// When destructors are used, we don't want LLVM eliminating CALLBACK for any
				// reason. Once the symbol makes it to the linker, it will do the rest.
				unsafe { std::ptr::from_ref(&CALLBACK).read_volatile() };
			}
		}

		// This places this function in a specialy section which puts
		#[unsafe(link_section = ".CRT$XLB")]
		pub static CALLBACK: unsafe extern "system" fn(*mut std::ffi::c_void, u32, *mut std::ffi::c_void) =
			tls_callback;

		const DLL_THREAD_DETACH: u32 = 3u32;

		// Unsure of if this works reliably, the destructor for the thread_local might run before this here, causing the with to panic.
		//
		// Maybe this is easier to handle with `#[thread_local]` when that is ever stabilized.
		unsafe extern "system" fn tls_callback(
			_h: *mut std::ffi::c_void,
			dw_reason: u32,
			_pv: *mut std::ffi::c_void,
		) {
			if dw_reason == DLL_THREAD_DETACH {
				THREAD_NODE.with(|x| x.unlink())
			}
		}


	} else{
		compile_error!("The `allocation-tracking` feature is not supported on your platform")
	}
}
