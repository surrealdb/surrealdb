use std::fmt::Debug;
use std::num::NonZeroUsize;
use std::ptr::NonNull;

use common::TypedError;
use common::source_error::Diagnostic;

pub type ParseResult<T> = Result<T, ParseError>;

pub struct ParseError(NonNull<()>);

impl Debug for ParseError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if self.is_speculative() {
			f.debug_tuple("ParseError::Speculative").finish()
		} else if self.is_recover() {
			f.debug_tuple("ParseError::Recover").finish()
		} else if let Some(diag) = self.as_diagnostic() {
			f.debug_tuple("ParseError::Diagnostic").field(diag).finish()
		} else {
			unreachable!()
		}
	}
}

impl ParseError {
	pub fn speculate_error() -> Self {
		unsafe { ParseError(NonNull::without_provenance(NonZeroUsize::new_unchecked(1))) }
	}

	pub fn recover_error() -> Self {
		unsafe { ParseError(NonNull::without_provenance(NonZeroUsize::new_unchecked(2))) }
	}

	pub fn is_speculative(&self) -> bool {
		self.0.addr().get() == 1
	}

	pub fn is_recover(&self) -> bool {
		self.0.addr().get() == 2
	}

	pub fn is_diagnostic(&self) -> bool {
		self.0.addr().get() > 2
	}

	pub fn diagnostic(d: Diagnostic<'static>) -> Self {
		let error = TypedError::new(d).into_raw();
		ParseError(error)
	}

	pub fn to_diagnostic(self) -> Option<TypedError<Diagnostic<'static>>> {
		if !self.is_diagnostic() {
			return None;
		}
		let diag = unsafe { TypedError::<Diagnostic<'static>>::from_raw(self.0) };
		std::mem::forget(self);
		Some(diag)
	}

	pub fn as_diagnostic<'a>(&'a self) -> Option<&'a Diagnostic<'static>> {
		if !self.is_diagnostic() {
			return None;
		}
		unsafe { Some(TypedError::<Diagnostic<'static>>::ref_from_raw(self.0)) }
	}
}

impl Drop for ParseError {
	fn drop(&mut self) {
		if self.is_diagnostic() {
			unsafe { TypedError::<Diagnostic<'static>>::from_raw(self.0) };
		}
	}
}
