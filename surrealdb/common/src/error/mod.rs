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
	#[cold]
	pub fn new<E>(e: E) -> Self
	where
		E: ErrorTrait,
	{
		Error(RawError::new(e))
	}

	pub fn error_code(&self) -> ErrorCode {
		self.0.error_code()
	}

	pub fn downcast_ref<T: ErrorTrait>(&self) -> Option<&T> {
		self.0.is::<T>().then(|| unsafe { self.0.unchecked_ref() })
	}

	pub fn downcast_mut<T: ErrorTrait>(&mut self) -> Option<&mut T> {
		self.0.is::<T>().then(|| unsafe { self.0.unchecked_mut() })
	}

	pub fn into_inner<T: ErrorTrait>(self) -> Result<T, Self> {
		if self.0.is::<T>() {
			Ok(unsafe { self.0.unchecked_into_inner() })
		} else {
			Err(self)
		}
	}

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
/// This error can be efficiently cast into	[`Error`] without any allocation.
///
/// This error will always be the size of a pointer, regardless of the errors it might contain.
pub struct TypedError<T: ErrorTrait>(RawTypedError<T>);

impl<T: ErrorTrait> TypedError<T> {
	pub fn new(e: T) -> Self {
		TypedError(RawTypedError::new(e))
	}

	/// Convert the error into a type erased version.
	pub fn erase(self) -> Error {
		Error(self.0.erase())
	}

	pub fn into_inner(self) -> T {
		self.0.into_inner()
	}

	pub fn into_raw(self) -> NonNull<()> {
		self.0.into_raw()
	}

	pub unsafe fn from_raw(ptr: NonNull<()>) -> Self {
		unsafe { TypedError(RawTypedError::from_raw(ptr)) }
	}

	pub unsafe fn ref_from_raw<'a>(ptr: NonNull<()>) -> &'a T {
		unsafe { RawTypedError::<T>::ref_from_raw(ptr) }
	}

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
