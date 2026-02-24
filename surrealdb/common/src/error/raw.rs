use core::fmt;
use std::alloc::{self, Layout};
use std::any::TypeId;
use std::ops::{Deref, DerefMut};
use std::ptr::NonNull;

use super::{ErrorCode, ErrorTrait};

struct ErrorVTable {
	id: fn() -> TypeId,
	drop_in_place: unsafe fn(ErrorPtr<()>),
	error_code: unsafe fn(ErrorPtr<()>) -> ErrorCode,
	debug: unsafe fn(ErrorPtr<()>, &mut fmt::Formatter) -> fmt::Result,
	display: unsafe fn(ErrorPtr<()>, &mut fmt::Formatter) -> fmt::Result,
}

impl ErrorVTable {
	unsafe fn drop_in_place<T: ErrorTrait>(ptr: ErrorPtr<()>) {
		let ptr = ptr.cast::<ErrorImpl<T>>();
		let _ = unsafe { ptr.read() };
		unsafe { alloc::dealloc(ptr.as_ptr().cast(), Layout::new::<ErrorImpl<T>>()) }
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

	pub const fn for_error<E>() -> &'static Self
	where
		E: ErrorTrait,
	{
		trait HasVTable {
			const VTABLE: ErrorVTable;
		}

		impl<E: ErrorTrait> HasVTable for E {
			const VTABLE: ErrorVTable = ErrorVTable {
				id: TypeId::of::<E>,
				drop_in_place: ErrorVTable::drop_in_place::<E>,
				error_code: ErrorVTable::error_code::<E>,
				debug: ErrorVTable::debug::<E>,
				display: ErrorVTable::display::<E>,
			};
		}

		&<E as HasVTable>::VTABLE
	}
}

#[repr(C)]
struct ErrorImpl<T> {
	vtable: &'static ErrorVTable,
	t: T,
}

type ErrorPtr<T> = NonNull<ErrorImpl<T>>;

/// Raw implementation for error.
pub struct RawError(ErrorPtr<()>);

impl RawError {
	pub fn new<E: ErrorTrait>(e: E) -> Self {
		RawTypedError::new(e).erase()
	}

	pub fn is<T: ErrorTrait>(&self) -> bool {
		let vtable = unsafe { self.0.as_ref().vtable };

		// When the VTable pointer is the same it must be the same type as each type has a
		// different VTable. Doing this check first means we don't have to do an indirect call.
		// However there is no guarentee that a type has a single vtable so we still need to check
		// the typeid if the VTable is not the same.
		if std::ptr::addr_eq(ErrorVTable::for_error::<T>(), vtable) {
			return true;
		}

		let id = unsafe { (self.0.as_ref().vtable.id)() };
		id == TypeId::of::<T>()
	}

	pub unsafe fn unchecked_into_inner<T: ErrorTrait>(self) -> T {
		unsafe { self.unchecked_cast::<T>() }.into_inner()
	}

	pub unsafe fn unchecked_cast<T: ErrorTrait>(self) -> RawTypedError<T> {
		RawTypedError(self.0.cast())
	}

	pub unsafe fn unchecked_ref<T: ErrorTrait>(&self) -> &T {
		unsafe { &self.0.cast::<ErrorImpl<T>>().as_ref().t }
	}

	pub unsafe fn unchecked_mut<T: ErrorTrait>(&mut self) -> &mut T {
		unsafe { &mut self.0.cast::<ErrorImpl<T>>().as_mut().t }
	}

	pub fn debug(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let ptr = self.0;
		unsafe { (self.0.as_ref().vtable.debug)(ptr, f) }
	}

	pub fn display(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let ptr = self.0;
		unsafe { (self.0.as_ref().vtable.display)(ptr, f) }
	}

	pub fn error_code(&self) -> ErrorCode {
		let ptr = self.0;
		unsafe { (self.0.as_ref().vtable.error_code)(ptr) }
	}
}

impl Drop for RawError {
	fn drop(&mut self) {
		unsafe {
			let drop = self.0.as_ref().vtable.drop_in_place;
			(drop)(self.0)
		}
	}
}

pub struct RawTypedError<T>(ErrorPtr<T>);

impl<E: ErrorTrait> RawTypedError<E> {
	pub fn new(e: E) -> Self {
		let vtable = ErrorVTable::for_error::<E>();

		let ptr = unsafe { alloc::alloc(Layout::new::<ErrorImpl<E>>()) }.cast::<ErrorImpl<E>>();
		let ptr = NonNull::new(ptr).expect("Allocation failed");
		unsafe {
			ptr.write(ErrorImpl {
				vtable,
				t: e,
			})
		};
		RawTypedError(ptr.cast())
	}

	pub fn into_inner(self) -> E {
		let data = unsafe { self.0.read() };

		unsafe { alloc::dealloc(self.0.as_ptr().cast(), Layout::new::<ErrorImpl<E>>()) };
		std::mem::forget(self);

		data.t
	}

	pub fn erase(self) -> RawError {
		RawError(self.0.cast())
	}

	pub fn into_raw(self) -> NonNull<()> {
		let ptr = self.0;
		std::mem::forget(self);
		ptr.cast()
	}

	pub unsafe fn from_raw(ptr: NonNull<()>) -> Self {
		Self(ptr.cast())
	}

	pub unsafe fn ref_from_raw<'a>(ptr: NonNull<()>) -> &'a E {
		unsafe {
			let ptr: ErrorPtr<E> = ptr.cast();
			&ptr.as_ref().t
		}
	}

	pub unsafe fn ref_mut_from_raw<'a>(ptr: NonNull<()>) -> &'a mut E {
		unsafe {
			let mut ptr: ErrorPtr<E> = ptr.cast();
			&mut ptr.as_mut().t
		}
	}
}

impl<T> Deref for RawTypedError<T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		unsafe { &self.0.as_ref().t }
	}
}

impl<T> DerefMut for RawTypedError<T> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		unsafe { &mut self.0.as_mut().t }
	}
}

impl<T> Drop for RawTypedError<T> {
	fn drop(&mut self) {
		unsafe { std::ptr::drop_in_place(self.0.as_ptr()) };
		unsafe { alloc::dealloc(self.0.as_ptr().cast(), Layout::new::<ErrorImpl<T>>()) };
	}
}
