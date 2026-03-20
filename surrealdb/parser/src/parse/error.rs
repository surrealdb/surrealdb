use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::ptr::NonNull;

use common::TypedError;
use common::source_error::Diagnostic;

/// Shorthand for `Result<T, ParseError>`
pub type ParseResult<T> = Result<T, ParseError>;

/// Parser internal error type that is optimized for use within the parser, providing special error
/// types for speculative parsing and streaming, all packed into a single pointer-width value.
///
/// # Safety
/// This error type uses values 1 and 2 to report specific error states.
/// This is safe as the 0 is guarenteed to not be valid pointer and  the pointer returned from
/// [`TypedError`] is guarenteed to be atleast 4 byte aligned, so even if 1 is a valid pointer
/// address it would still be an invalid address for the pointer from TypedError.
pub struct ParseError(NonNull<()>);

impl Debug for ParseError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if self.is_speculative() {
			f.debug_tuple("ParseError::Speculative").finish()
		} else if self.is_missing_data() {
			f.debug_tuple("ParseError::MissingData").finish()
		} else if let Some(diag) = self.as_diagnostic() {
			f.debug_tuple("ParseError::Diagnostic").field(diag).finish()
		} else {
			unreachable!()
		}
	}
}

impl ParseError {
	/// Create an error for in a speculative context, indicating that this speculating failed and
	/// we should recover to another branch.
	///
	/// Be carefull to not return this error outside of a speculating context in the parser.
	pub fn speculate() -> Self {
		// Safety: 1 is not a valid pointer for typederror, See documentation of `ParseError`.
		unsafe { ParseError(NonNull::without_provenance(NonZeroUsize::new_unchecked(1))) }
	}

	/// Create an error for in a partial context, indicating that we did not have enough data to
	/// correctly parse the current syntax production.
	///
	/// This function should not be manually called, but only created inside parser methods.
	#[cold]
	pub fn missing_data() -> Self {
		// Safety: 2 is not a valid pointer for typederror, See documentation of `ParseError`.
		unsafe { ParseError(NonNull::without_provenance(NonZeroUsize::new_unchecked(2))) }
	}

	/// Returns if the error is a speculative error.
	pub fn is_speculative(&self) -> bool {
		self.0.addr().get() == 1
	}

	/// Returns if the error is a missing data error.
	pub fn is_missing_data(&self) -> bool {
		self.0.addr().get() == 2
	}

	/// Returns if the error is diagnostic error..
	pub fn is_diagnostic(&self) -> bool {
		self.0.addr().get() > 2
	}

	/// Create an ParseError from a diagnostic.
	pub fn diagnostic(d: Diagnostic<'static>) -> Self {
		let error = TypedError::new(d).into_raw();
		ParseError(error)
	}

	/// Return the underlying diagnostic if the error is not missing data or speculative.
	pub fn to_diagnostic(self) -> Option<TypedError<Diagnostic<'static>>> {
		if !self.is_diagnostic() {
			return None;
		}
		let diag = unsafe { TypedError::<Diagnostic<'static>>::from_raw(self.0) };
		std::mem::forget(self);
		Some(diag)
	}

	/// Returns a reference to the underlying diagnostic if the error is not missing data
	/// or speculative.
	pub fn as_diagnostic(&self) -> Option<&Diagnostic<'static>> {
		if !self.is_diagnostic() {
			return None;
		}
		unsafe { Some(TypedError::<Diagnostic<'static>>::ref_from_raw(self.0)) }
	}

	/// Returns a mutable reference to the underlying diagnostic if the error is not missing data
	/// or speculative.
	pub fn as_mut_diagnostic(&mut self) -> Option<&mut Diagnostic<'static>> {
		if !self.is_diagnostic() {
			return None;
		}
		unsafe { Some(TypedError::<Diagnostic<'static>>::ref_mut_from_raw(self.0)) }
	}
}

impl Drop for ParseError {
	fn drop(&mut self) {
		if self.is_diagnostic() {
			unsafe { TypedError::<Diagnostic<'static>>::from_raw(self.0) };
		}
	}
}
