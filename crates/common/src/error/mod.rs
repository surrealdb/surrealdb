use core::fmt;
use std::{
	fmt::{Debug, Display},
	ptr::NonNull,
};

mod code;
pub use code::ErrorCode;

type ErrorPtr<T> = NonNull<ErrorImpl<T>>;

struct ErrorVTable {
	drop_in_place: unsafe fn(ErrorPtr<()>),
	error_code: unsafe fn(ErrorPtr<()>) -> ErrorCode,
	debug: unsafe fn(ErrorPtr<()>, &mut fmt::Formatter) -> fmt::Result,
	display: unsafe fn(ErrorPtr<()>, &mut fmt::Formatter) -> fmt::Result,
}

impl ErrorVTable {
	unsafe fn drop_in_place<T: ErrorTrait>(ptr: ErrorPtr<()>) {
		let ptr = ptr.cast::<ErrorImpl<T>>();
		unsafe { Box::from_raw(ptr.as_ptr()) };
	}

	unsafe fn error_code<T: ErrorTrait>(ptr: ErrorPtr<()>) -> ErrorCode {
		let ptr = ptr.cast::<ErrorImpl<T>>();
		unsafe { ptr.as_ref().t.error_code() }
	}

	unsafe fn debug<T: ErrorTrait>(ptr: ErrorPtr<()>, f: &mut fmt::Formatter) -> fmt::Result {
		let ptr = ptr.cast::<ErrorImpl<T>>();
		unsafe { fmt::Debug::fmt(&ptr.as_ref().t, f) }
	}

	unsafe fn display<T: ErrorTrait>(ptr: ErrorPtr<()>, f: &mut fmt::Formatter) -> fmt::Result {
		let ptr = ptr.cast::<ErrorImpl<T>>();
		unsafe { fmt::Display::fmt(&ptr.as_ref().t, f) }
	}

	pub fn for_error<E>() -> &'static Self
	where
		E: ErrorTrait,
	{
		trait HasVTable {
			const VTABLE: ErrorVTable;
		}

		impl<E: ErrorTrait> HasVTable for E {
			const VTABLE: ErrorVTable = ErrorVTable {
				drop_in_place: ErrorVTable::drop_in_place::<E>,
				error_code: ErrorVTable::error_code::<E>,
				debug: ErrorVTable::debug::<E>,
				display: ErrorVTable::display::<E>,
			};
		}

		&<E as HasVTable>::VTABLE
	}
}

pub trait ErrorTrait: Display + Debug {
	fn error_code(&self) -> ErrorCode {
		ErrorCode::default()
	}
}

impl<E: std::error::Error> ErrorTrait for E {}

#[repr(C)]
struct ErrorImpl<T> {
	vtable: &'static ErrorVTable,
	t: T,
}

pub struct Error(ErrorPtr<()>);

impl Error {
	pub fn new<E>(e: E) -> Self
	where
		E: ErrorTrait,
	{
		let vtable = ErrorVTable::for_error::<E>();
		let ptr = Box::new(ErrorImpl {
			vtable,
			t: e,
		});
		let ptr = unsafe { NonNull::new_unchecked(Box::into_raw(ptr)) };
		Error(ptr.cast())
	}

	pub fn error_code(&self) -> ErrorCode {
		unsafe {
			let error_code_fn = self.0.as_ref().vtable.error_code;
			(error_code_fn)(self.0)
		}
	}
}

impl fmt::Debug for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		unsafe {
			let debug = self.0.as_ref().vtable.debug;
			(debug)(self.0, f)
		}
	}
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		unsafe {
			let display = self.0.as_ref().vtable.display;
			(display)(self.0, f)
		}
	}
}

impl Drop for Error {
	fn drop(&mut self) {
		unsafe {
			let drop = self.0.as_ref().vtable.drop_in_place;
			(drop)(self.0)
		}
	}
}
