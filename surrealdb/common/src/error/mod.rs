use core::fmt;
use std::fmt::{Debug, Display};
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;
use std::result::Result as StdResult;

mod code;
mod raw;
pub mod source;

pub use code::ErrorCode;
use raw::{RawError, RawTypedError};

pub trait ErrorTrait: Display + Debug + 'static {
	fn error_code(&self) -> ErrorCode {
		ErrorCode::default()
	}
}

impl<E: std::error::Error + 'static> ErrorTrait for E {}

pub type Result<T, E = Error> = StdResult<T, E>;

/// Generic error type, optimized to have little overhead on the happy path.
///
/// This error will always be the size of a pointer, regardless of the errors it might contain.
pub struct Error(RawError);

impl Error {
	/// Create a new error.
	#[cold]
	pub fn new<E>(e: E) -> Self
	where
		E: ErrorTrait,
	{
		Error(RawError::new(e))
	}

	/// Returns the error code for the error.
	pub fn error_code(&self) -> ErrorCode {
		self.0.error_code()
	}

	/// Obtain a reference to the internal error if the error is of the right type.
	pub fn downcast_ref<T: ErrorTrait>(&self) -> Option<&T> {
		self.0.is::<T>().then(|| unsafe { self.0.unchecked_ref() })
	}

	/// Obtain a mutable reference to the internal error if the error is of the right type.
	pub fn downcast_mut<T: ErrorTrait>(&mut self) -> Option<&mut T> {
		self.0.is::<T>().then(|| unsafe { self.0.unchecked_mut() })
	}

	/// Convert value to the internal error if the error is of the right type.
	pub fn into_inner<T: ErrorTrait>(self) -> Result<T, Self> {
		if self.0.is::<T>() {
			Ok(unsafe { self.0.unchecked_into_inner() })
		} else {
			Err(self)
		}
	}

	/// Returns a typed version of the error if the error is of the right type.
	pub fn downcast<T: ErrorTrait>(self) -> Result<TypedError<T>, Self> {
		if self.0.is::<T>() {
			Ok(TypedError(unsafe { self.0.unchecked_cast() }))
		} else {
			Err(self)
		}
	}
}

impl fmt::Debug for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		self.0.debug(f)
	}
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		self.0.display(f)
	}
}

/// Error type, optimized to have little overhead on the happy path.
///
/// This error can be efficiently cast into [`Error`] without any allocation.
///
/// This error will always be the size of a pointer, regardless of the errors it might contain.
pub struct TypedError<T: ErrorTrait>(RawTypedError<T>);

impl<T: ErrorTrait> TypedError<T> {
	/// Creates a new typed error. Boxing the given error.
	pub fn new(e: T) -> Self {
		TypedError(RawTypedError::new(e))
	}

	/// Convert the error into a type erased version.
	pub fn erase(self) -> Error {
		Error(self.0.erase())
	}

	/// Returns the underlying error
	pub fn into_inner(self) -> T {
		self.0.into_inner()
	}

	/// Returns a raw pointer to the error.
	pub fn into_raw(self) -> NonNull<()> {
		self.0.into_raw()
	}

	/// Create a type error from a pointer.
	///
	/// # Safety
	/// Pointer must have previously been returned from [`TypedError::into_raw`] and after calling
	/// from_raw the pointer must no longer be used.
	pub unsafe fn from_raw(ptr: NonNull<()>) -> Self {
		unsafe { TypedError(RawTypedError::from_raw(ptr)) }
	}

	/// Create a type error from a pointer.
	///
	/// # Safety
	/// Pointer must have previously been returned from [`TypedError::into_raw`] and the pointer
	/// must not be mutably accessed with for example [`TypedError::ref_mut_from_raw`].
	pub unsafe fn ref_from_raw<'a>(ptr: NonNull<()>) -> &'a T {
		unsafe { RawTypedError::<T>::ref_from_raw(ptr) }
	}

	/// Create a type error from a pointer.
	///
	/// # Safety
	/// Pointer must have previously been returned from [`TypedError::into_raw`] and the pointer
	/// must not be borrowed with for example [`TypedError::ref_from_raw`].
	pub unsafe fn ref_mut_from_raw<'a>(ptr: NonNull<()>) -> &'a mut T {
		unsafe { RawTypedError::<T>::ref_mut_from_raw(ptr) }
	}
}

impl<T: ErrorTrait> Deref for TypedError<T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		self.0.deref()
	}
}

impl<T: ErrorTrait> DerefMut for TypedError<T> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.0.deref_mut()
	}
}

impl<T: ErrorTrait> fmt::Debug for TypedError<T> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		Debug::fmt(self.0.deref(), f)
	}
}

impl<T: ErrorTrait> fmt::Display for TypedError<T> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		Display::fmt(self.0.deref(), f)
	}
}

#[cfg(test)]
mod tests {
	use std::fmt;
	use std::sync::atomic::{AtomicUsize, Ordering};

	use super::{Error, TypedError};

	const SENTINEL: u64 = 0xDEAD_BEEF_CAFE_BABE;

	/// Error type for regression tests covering the `Error`/`TypedError` ownership-transfer
	/// paths (`erase`, `downcast`, `into_inner`).
	///
	/// Holds a `Box<u64>` so any double-free of the inner allocation also corrupts the heap
	/// (caught by the system allocator and tools like Miri/ASan), and increments a counter on
	/// `Drop` so the tests can assert that the inner value is dropped exactly once across each
	/// conversion path.
	struct DropCounted {
		value: Box<u64>,
		counter: &'static AtomicUsize,
	}

	impl DropCounted {
		fn new(counter: &'static AtomicUsize) -> Self {
			DropCounted {
				value: Box::new(SENTINEL),
				counter,
			}
		}
	}

	impl fmt::Debug for DropCounted {
		fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
			write!(f, "DropCounted({:#x})", *self.value)
		}
	}

	impl fmt::Display for DropCounted {
		fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
			write!(f, "drop-counted error: {:#x}", *self.value)
		}
	}

	impl std::error::Error for DropCounted {}

	impl Drop for DropCounted {
		fn drop(&mut self) {
			self.counter.fetch_add(1, Ordering::SeqCst);
		}
	}

	/// Unrelated error type used to exercise the `downcast` failure branch.
	#[derive(Debug)]
	struct OtherType;

	impl fmt::Display for OtherType {
		fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
			f.write_str("OtherType")
		}
	}

	impl std::error::Error for OtherType {}

	#[test]
	fn error_new_format_then_drop_runs_once() {
		static COUNTER: AtomicUsize = AtomicUsize::new(0);
		{
			let err = Error::new(DropCounted::new(&COUNTER));
			let debug = format!("{:?}", err);
			let display = format!("{}", err);
			assert!(debug.contains("DropCounted"), "unexpected debug: {debug}");
			assert!(display.contains("drop-counted error"), "unexpected display: {display}",);
			assert_eq!(COUNTER.load(Ordering::SeqCst), 0, "value dropped early");
		}
		assert_eq!(COUNTER.load(Ordering::SeqCst), 1, "expected exactly one drop");
	}

	#[test]
	fn error_downcast_then_into_inner_drops_once() {
		static COUNTER: AtomicUsize = AtomicUsize::new(0);
		{
			let err = Error::new(DropCounted::new(&COUNTER));
			let typed: TypedError<DropCounted> =
				err.downcast::<DropCounted>().expect("downcast should succeed");
			assert_eq!(*typed.value, SENTINEL);
			let inner = typed.into_inner();
			assert_eq!(*inner.value, SENTINEL);
		}
		assert_eq!(COUNTER.load(Ordering::SeqCst), 1, "expected exactly one drop");
	}

	#[test]
	fn error_into_inner_drops_once() {
		static COUNTER: AtomicUsize = AtomicUsize::new(0);
		{
			let err = Error::new(DropCounted::new(&COUNTER));
			let inner = err.into_inner::<DropCounted>().expect("into_inner should succeed");
			assert_eq!(*inner.value, SENTINEL);
		}
		assert_eq!(COUNTER.load(Ordering::SeqCst), 1, "expected exactly one drop");
	}

	#[test]
	fn error_downcast_failure_preserves_original() {
		static COUNTER: AtomicUsize = AtomicUsize::new(0);
		{
			let err = Error::new(DropCounted::new(&COUNTER));
			let err =
				err.downcast::<OtherType>().expect_err("downcast to unrelated type should fail");
			let _ = format!("{:?}", err);
			let _ = format!("{}", err);
			assert_eq!(COUNTER.load(Ordering::SeqCst), 0, "value dropped during failed downcast",);
		}
		assert_eq!(COUNTER.load(Ordering::SeqCst), 1, "expected exactly one drop");
	}

	#[test]
	fn typed_error_erase_then_drop_runs_once() {
		static COUNTER: AtomicUsize = AtomicUsize::new(0);
		{
			let typed = TypedError::new(DropCounted::new(&COUNTER));
			let err: Error = typed.erase();
			let _ = format!("{:?}", err);
		}
		assert_eq!(COUNTER.load(Ordering::SeqCst), 1, "expected exactly one drop");
	}

	#[test]
	fn typed_error_erase_then_downcast_into_inner_drops_once() {
		static COUNTER: AtomicUsize = AtomicUsize::new(0);
		{
			let typed = TypedError::new(DropCounted::new(&COUNTER));
			let err = typed.erase();
			let typed_back = err.downcast::<DropCounted>().expect("downcast should succeed");
			let inner = typed_back.into_inner();
			assert_eq!(*inner.value, SENTINEL);
		}
		assert_eq!(COUNTER.load(Ordering::SeqCst), 1, "expected exactly one drop");
	}
}
