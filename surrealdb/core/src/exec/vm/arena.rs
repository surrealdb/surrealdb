//! A bump arena for memory whose lifetime is a stretch of a run: a batch of
//! output rows, a breaker's state, a statement's write images.
//!
//! An [`Arena`] hands out memory through a shared reference, so any number of
//! values borrowed from it are live at once, and takes it all back at
//! [`Arena::reset`], which needs the arena exclusively: the borrow checker
//! proves nothing borrowed from the arena outlives the reset.
//!
//! Only types without drop glue are placed in it (`T: Copy`), so a reset frees
//! nothing but the chunks' fill. The chunks themselves are kept: an arena that
//! is reset and filled again to the same high-water mark asks the global
//! allocator for nothing. A fill that outgrew the first chunk is coalesced at
//! the reset into one chunk the size of everything handed out, so the
//! steady state is a single chunk.
//!
//! [`Arena::allocated`] reports the bytes the arena holds from the global
//! allocator, which is what a memory budget charges for it.
//!
//! Its users are the batch, breaker and statement arenas
//! `doc/executor/RFC_ZERO_ALLOC_EXECUTION.md` lays out.
#![cfg_attr(not(test), allow(dead_code))]

use std::alloc::Layout;
use std::cell::{Cell, UnsafeCell};
use std::ptr::NonNull;

/// The size of an arena's first chunk when none is asked for.
const DEFAULT_CHUNK: usize = 4 * 1024;

/// The alignment every chunk starts at, enough for any `T` the VM places.
const CHUNK_ALIGN: usize = 16;

/// One chunk: a block of uninitialised memory from the global allocator,
/// aligned to [`CHUNK_ALIGN`]. Held as a raw pointer rather than a box, so
/// that reading where it lies asserts nothing about the memory a live
/// reference into it covers.
struct Chunk {
	ptr: NonNull<u8>,
	bytes: usize,
}

impl Chunk {
	fn new(bytes: usize) -> Self {
		let bytes = bytes.max(CHUNK_ALIGN).next_multiple_of(CHUNK_ALIGN);
		let layout = Layout::from_size_align(bytes, CHUNK_ALIGN).expect("a chunk's layout");
		// SAFETY: the layout has a non-zero size.
		let ptr = unsafe { std::alloc::alloc(layout) };
		let Some(ptr) = NonNull::new(ptr) else {
			std::alloc::handle_alloc_error(layout)
		};
		Self {
			ptr,
			bytes,
		}
	}
}

impl Drop for Chunk {
	fn drop(&mut self) {
		let layout = Layout::from_size_align(self.bytes, CHUNK_ALIGN).expect("a chunk's layout");
		// SAFETY: allocated in `Chunk::new` with this layout, and freed once.
		unsafe { std::alloc::dealloc(self.ptr.as_ptr(), layout) }
	}
}

/// A bump arena. See the module documentation.
pub(crate) struct Arena {
	/// Every chunk the arena holds; the last is the one being filled. A
	/// chunk's memory never moves while the arena holds it, whatever the
	/// vector does, so a reference into one stays valid until the reset.
	/// Only [`Self::grow`] and [`Self::reset`] mutate the vector.
	chunks: UnsafeCell<Vec<Chunk>>,
	/// The next free byte of the chunk being filled, as an offset into it.
	cursor: Cell<usize>,
	/// The bytes handed out since the last reset, including alignment.
	used: Cell<usize>,
}

// SAFETY: the arena owns its chunks; moving it to another thread moves that
// ownership. Every reference into it borrows the arena, so none can be live
// on the old thread once it has moved. It is not `Sync`: `Cell` makes it
// usable from one thread at a time.
unsafe impl Send for Arena {}

impl Default for Arena {
	fn default() -> Self {
		Self::new()
	}
}

impl Arena {
	/// An arena that allocates nothing until its first use.
	pub(crate) const fn new() -> Self {
		Self {
			chunks: UnsafeCell::new(Vec::new()),
			cursor: Cell::new(0),
			used: Cell::new(0),
		}
	}

	/// An arena with one chunk of at least `bytes`.
	pub(crate) fn with_capacity(bytes: usize) -> Self {
		Self {
			chunks: UnsafeCell::new(vec![Chunk::new(bytes)]),
			cursor: Cell::new(0),
			used: Cell::new(0),
		}
	}

	/// The bytes handed out since the last reset.
	pub(crate) fn used(&self) -> usize {
		self.used.get()
	}

	/// The bytes the arena holds from the global allocator.
	pub(crate) fn allocated(&self) -> usize {
		// SAFETY: only the length of each chunk is read, and no `&mut` to the
		// vector is live: every mutation is confined to `grow`, which hands out
		// no reference to it.
		let chunks = unsafe { &*self.chunks.get() };
		chunks.iter().map(|chunk| chunk.bytes).sum()
	}

	/// Take back everything handed out. A fill that spread over several
	/// chunks leaves one chunk large enough for all of it.
	pub(crate) fn reset(&mut self) {
		let used = self.used.get();
		let chunks = self.chunks.get_mut();
		if chunks.len() > 1 {
			let total: usize = chunks.iter().map(|chunk| chunk.bytes).sum();
			chunks.clear();
			chunks.push(Chunk::new(total.max(used)));
		}
		self.cursor.set(0);
		self.used.set(0);
	}

	/// Uninitialised memory for `layout`, valid until the next reset.
	#[inline]
	fn alloc_layout(&self, layout: Layout) -> NonNull<u8> {
		debug_assert!(layout.align() <= CHUNK_ALIGN, "an arena value aligned past a chunk");
		if let Some(ptr) = self.bump(layout) {
			return ptr;
		}
		self.grow(layout.size());
		self.bump(layout).expect("a fresh chunk holds the allocation it was sized for")
	}

	/// Memory for `layout` from the chunk being filled, if it has room.
	#[inline]
	fn bump(&self, layout: Layout) -> Option<NonNull<u8>> {
		// SAFETY: no `&mut` to the vector is live (see `allocated`); the last
		// chunk is only read through a raw pointer below.
		let chunks = unsafe { &*self.chunks.get() };
		let chunk = chunks.last()?;
		let (base, capacity) = (chunk.ptr.as_ptr(), chunk.bytes);
		let cursor = self.cursor.get();
		// Chunks start aligned to `CHUNK_ALIGN`, so aligning the offset aligns
		// the address.
		let start = cursor.checked_add(layout.align() - 1)? & !(layout.align() - 1);
		let end = start.checked_add(layout.size())?;
		if end > capacity {
			return None;
		}
		self.cursor.set(end);
		self.used.set(self.used.get() + (end - cursor));
		// SAFETY: `start <= capacity`, so the pointer is within the chunk or
		// one past its end, and the chunk's pointer is non-null.
		Some(unsafe { NonNull::new_unchecked(base.add(start)) })
	}

	/// Start a new chunk that holds at least `size` bytes.
	#[cold]
	#[inline(never)]
	fn grow(&self, size: usize) {
		// SAFETY: this is the only place the vector is mutated, and it hands out
		// no reference to it. Pushing may move the vector's buffer but not the
		// chunks it points at, so every reference into an earlier chunk stays
		// valid.
		let chunks = unsafe { &mut *self.chunks.get() };
		let last = chunks.last().map_or(DEFAULT_CHUNK / 2, |chunk| chunk.bytes);
		let bytes = (last * 2).max(size + CHUNK_ALIGN).max(DEFAULT_CHUNK);
		chunks.push(Chunk::new(bytes));
		self.cursor.set(0);
	}

	/// `value`, moved into the arena.
	#[inline]
	pub(crate) fn alloc<T: Copy>(&self, value: T) -> &T {
		let ptr = self.alloc_layout(Layout::new::<T>()).cast::<T>();
		// SAFETY: the memory is fresh, aligned and sized for `T`, and handed
		// out once; it lives until the reset, which the borrow of `self`
		// outlives.
		unsafe {
			ptr.as_ptr().write(value);
			&*ptr.as_ptr()
		}
	}

	/// A copy of `src` in the arena.
	#[inline]
	pub(crate) fn alloc_slice<T: Copy>(&self, src: &[T]) -> &[T] {
		if src.is_empty() {
			return &[];
		}
		let layout = Layout::array::<T>(src.len()).expect("a slice's layout");
		let ptr = self.alloc_layout(layout).cast::<T>();
		// SAFETY: the memory is fresh, aligned and sized for `src.len()`
		// values of `T`, and does not overlap `src`.
		unsafe {
			std::ptr::copy_nonoverlapping(src.as_ptr(), ptr.as_ptr(), src.len());
			std::slice::from_raw_parts(ptr.as_ptr(), src.len())
		}
	}

	/// A slice of `len` values in the arena, each `fill(index)`.
	#[inline]
	pub(crate) fn alloc_slice_with<T: Copy>(
		&self,
		len: usize,
		mut fill: impl FnMut(usize) -> T,
	) -> &[T] {
		if len == 0 {
			return &[];
		}
		let layout = Layout::array::<T>(len).expect("a slice's layout");
		let ptr = self.alloc_layout(layout).cast::<T>();
		for i in 0..len {
			// SAFETY: `i < len`, within the fresh allocation. A `fill` that
			// panics leaves the rest uninitialised but unreachable, and `T` has
			// no drop glue to run over it.
			unsafe { ptr.as_ptr().add(i).write(fill(i)) };
		}
		// SAFETY: every element was written above.
		unsafe { std::slice::from_raw_parts(ptr.as_ptr(), len) }
	}

	/// A copy of `src` in the arena.
	#[inline]
	pub(crate) fn alloc_bytes(&self, src: &[u8]) -> &[u8] {
		self.alloc_slice(src)
	}

	/// A copy of `src` in the arena.
	#[inline]
	pub(crate) fn alloc_str(&self, src: &str) -> &str {
		let bytes = self.alloc_slice(src.as_bytes());
		// SAFETY: the bytes are a copy of a `str`'s.
		unsafe { std::str::from_utf8_unchecked(bytes) }
	}
}

impl std::fmt::Debug for Arena {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Arena")
			.field("used", &self.used())
			.field("allocated", &self.allocated())
			.finish()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn values_borrowed_at_once_keep_their_contents() {
		let arena = Arena::new();
		let mut held = Vec::new();
		for i in 0..10_000u64 {
			held.push((i, arena.alloc(i * 3)));
		}
		let s = arena.alloc_str("hello");
		let b = arena.alloc_bytes(&[1, 2, 3]);
		for (i, v) in &held {
			assert_eq!(**v, i * 3);
		}
		assert_eq!(s, "hello");
		assert_eq!(b, &[1, 2, 3]);
	}

	#[test]
	fn allocations_are_aligned() {
		let arena = Arena::new();
		arena.alloc(1u8);
		let wide = arena.alloc(7u128);
		assert_eq!((wide as *const u128 as usize) % std::mem::align_of::<u128>(), 0);
		arena.alloc_bytes(&[0; 3]);
		let words = arena.alloc_slice(&[1u64, 2, 3]);
		assert_eq!((words.as_ptr() as usize) % std::mem::align_of::<u64>(), 0);
		assert_eq!(words, &[1, 2, 3]);
	}

	#[test]
	fn an_allocation_larger_than_a_chunk_gets_a_chunk_of_its_own() {
		let arena = Arena::with_capacity(64);
		let big = arena.alloc_slice_with(100_000, |i| i as u32);
		assert_eq!(big.len(), 100_000);
		assert!(big.iter().enumerate().all(|(i, v)| *v == i as u32));
		assert!(arena.allocated() >= 400_000);
	}

	#[test]
	fn a_reset_coalesces_the_chunks_and_keeps_them() {
		let mut arena = Arena::new();
		for _ in 0..1_000 {
			arena.alloc_bytes(&[7; 100]);
		}
		let used = arena.used();
		assert!(used >= 100_000);
		arena.reset();
		assert_eq!(arena.used(), 0);
		// SAFETY: test-only read of the chunk count; nothing is borrowed.
		let chunks = unsafe { &*arena.chunks.get() }.len();
		assert_eq!(chunks, 1, "the fill is coalesced into one chunk");
		let allocated = arena.allocated();
		assert!(allocated >= used);
		// Filling it to the same mark again needs no new chunk.
		for _ in 0..1_000 {
			arena.alloc_bytes(&[7; 100]);
		}
		assert_eq!(arena.allocated(), allocated);
		// SAFETY: as above.
		assert_eq!(unsafe { &*arena.chunks.get() }.len(), 1);
	}

	#[test]
	fn empty_slices_take_no_memory() {
		let arena = Arena::new();
		assert!(arena.alloc_slice::<u64>(&[]).is_empty());
		assert_eq!(arena.alloc_str(""), "");
		assert_eq!(arena.allocated(), 0);
	}
}
