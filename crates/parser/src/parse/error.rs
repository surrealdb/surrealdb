use std::{num::NonZeroUsize, ptr::NonNull};

use common::{TypedError, source_error::Diagnostic};

pub type ParseResult<T> = Result<T, ParseError>;

pub struct ParseError(NonZeroUsize);

impl ParseError {
	pub fn speculate_error() -> Self {
		unsafe { ParseError(NonZeroUsize::new_unchecked(1)) }
	}

	pub fn recover_error() -> Self {
		unsafe { ParseError(NonZeroUsize::new_unchecked(2)) }
	}

	pub fn is_speculative(&self) -> bool {
		self.0.get() == 1
	}

	pub fn is_recover(&self) -> bool {
		self.0.get() == 2
	}

	pub fn is_diagnostic(&self) -> bool {
		self.0.get() > 2
	}

	pub fn diagnostic(d: Diagnostic<'static>) -> Self {
		let error = TypedError::new(d).into_raw();
		ParseError(error.addr())
	}

	pub fn to_diagnostic(self) -> Option<TypedError<Diagnostic<'static>>> {
		if !self.is_diagnostic() {
			return None;
		}
		let diag = unsafe {
			TypedError::<Diagnostic<'static>>::from_raw(NonNull::with_exposed_provenance(self.0))
		};
		std::mem::forget(self);
		Some(diag)
	}
}

impl Drop for ParseError {
	fn drop(&mut self) {
		if self.is_diagnostic() {
			unsafe {
				TypedError::<Diagnostic<'static>>::from_raw(NonNull::with_exposed_provenance(
					self.0,
				))
			};
		}
	}
}
