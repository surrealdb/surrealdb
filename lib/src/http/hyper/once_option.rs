use std::{mem::ManuallyDrop, sync::Once};

pub struct OnceOption<T: ?Sized> {
	once: Once,
	value: ManuallyDrop<T>,
}

unsafe impl<T> Send for OnceOption<T> {}
unsafe impl<T> Sync for OnceOption<T> {}

impl<T: ?Sized> Drop for OnceOption<T> {
	fn drop(&mut self) {
		self.once.call_once(|| {
			// SAFETY: This can only be called once, so it is safe to access mutably,
			// and since it was not executed yet the value is still present so we need to call
			// drop,
			unsafe { ManuallyDrop::drop(&mut self.value) }
		})
	}
}

impl<T: Sized> OnceOption<T> {
	pub fn new(t: T) -> Self {
		Self {
			once: Once::new(),
			value: ManuallyDrop::new(t),
		}
	}

	pub fn take(&self) -> Option<T> {
		let mut res = None;
		self.once.call_once(|| {
			// SAFETY: Since this function can only be called once, the value is still present in
			// value and we can move out of it safely.
			let value =
				unsafe { ManuallyDrop::into_inner((&self.value as *const ManuallyDrop<T>).read()) };
			res = Some(value);
		});
		res
	}
}

impl<T: ?Sized> OnceOption<T> {
	pub fn is_taken(&self) -> bool {
		self.once.is_completed()
	}
}
