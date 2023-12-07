use crate::sync::{write_file, LockState, LOCKS};
use async_std::sync::Mutex as RealMutex;
use async_std::sync::MutexGuard as RealMutexGuard;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use ulid::Ulid;

pub enum MutexLockState {
	MutexRequested {
		name: &'static str,
		id: Ulid,
		event_id: Ulid,
	},
	MutexLocked {
		name: &'static str,
		id: Ulid,
		event_id: Ulid,
	},
	MutexUnlocked {
		name: &'static str,
		id: Ulid,
		event_id: Ulid,
		previous_guard: Option<Ulid>,
	},
	MutexDestroyed {
		name: &'static str,
		id: Ulid,
		event_id: Ulid,
	},
}

pub struct Mutex<T: ?Sized> {
	name: &'static str,
	id: Ulid,
	mutex: RealMutex<T>,
}

// unsafe impl<T: ?Sized + Send> Send for Mutex<T> {}

impl<T: ?Sized> Mutex<T> {
	/// Creates a new instance of a `Mutex<T>` which is unlocked.
	/// This particular implementation is for traceability
	#[track_caller]
	pub fn new(value: T, name: &'static str) -> Mutex<T>
	where
		T: Sized,
	{
		let id = Ulid::new();
		Mutex::<T>::create_event(id, name);
		Mutex {
			name,
			id,
			mutex: RealMutex::new(value),
		}
	}

	pub async fn lock(&self) -> MutexGuard<T> {
		let request_event = Ulid::new();
		Mutex::<T>::lock_request_event(self.id, self.name, request_event);
		let guard = self.mutex.lock().await;
		let guard = MutexGuard {
			name: self.name,
			id: self.id,
			lock_event_id: request_event,
			guard,
			_phantom: Default::default(),
		};
		Mutex::<T>::lock_ack_event(self.id, self.name);
		guard
	}

	pub fn try_lock(&self) -> Result<MutexGuard<T>, ()> {
		let request_event = Ulid::new();
		// TODO annotate that not real? Or only after acqui?
		Mutex::<T>::lock_request_event(self.id, self.name, request_event);
		let guard = self.mutex.try_lock().ok_or(())?;
		let guard = MutexGuard {
			name: self.name,
			id: self.id,
			lock_event_id: request_event,
			guard,
			_phantom: Default::default(),
		};
		Mutex::<T>::lock_ack_event(self.id, self.name);
		Ok(guard)
	}

	fn create_event(id: Ulid, name: &'static str) {
		unsafe {
			let lock_state = LockState::Mutex(MutexLockState::MutexUnlocked {
				name,
				id,
				event_id: Ulid::new(),
				previous_guard: None,
			});
			write_file(&lock_state);
			LOCKS.insert(id, lock_state);
		}
	}

	fn lock_request_event(id: Ulid, name: &'static str, request_event: Ulid) {
		unsafe {
			let lock_state = LockState::Mutex(MutexLockState::MutexRequested {
				name,
				id,
				event_id: request_event,
			});
			write_file(&lock_state);
			LOCKS.insert(id, lock_state);
		}
	}

	fn lock_ack_event(id: Ulid, name: &'static str) {
		unsafe {
			let lock_state = LockState::Mutex(MutexLockState::MutexLocked {
				name,
				id,
				event_id: Ulid::new(),
			});
			write_file(&lock_state);
			LOCKS.insert(id, lock_state);
		}
	}

	fn lock_destroy_event(id: Ulid, name: &'static str) {
		unsafe {
			let lock_state = LockState::Mutex(MutexLockState::MutexDestroyed {
				name,
				id,
				event_id: Ulid::new(),
			});
			write_file(&lock_state);
			LOCKS.insert(id, lock_state);
		}
	}
}

impl<T: ?Sized + Default> Default for Mutex<T> {
	fn default() -> Self {
		Self::new(Default::default(), "default")
	}
}

impl<T: ?Sized> Drop for Mutex<T> {
	fn drop(&mut self) {
		Mutex::<T>::lock_destroy_event(self.id, self.name);
	}
}

#[must_use = "if unused the RwLock will immediately unlock"]
// experimental
// #[must_not_suspend = "holding a RwLockWriteGuard across suspend \
//                       points can cause deadlocks, delays, \
//                       and cause Future's to not implement `Send`"]
#[clippy::has_significant_drop]
pub struct MutexGuard<'a, T: ?Sized + 'a> {
	name: &'static str,
	id: Ulid,
	lock_event_id: Ulid,
	// The absolute irony that this must be Send across threads
	guard: RealMutexGuard<'a, T>,
	_phantom: PhantomData<&'a T>,
}

// unsafe impl<T: ?Sized + Send> Send for MutexGuard<'_, T> {}

impl<'a, T: ?Sized + 'a> Drop for MutexGuard<'a, T> {
	fn drop(&mut self) {
		unsafe {
			let lock_state = LockState::Mutex(MutexLockState::MutexUnlocked {
				name: self.name,
				id: self.id,
				event_id: Ulid::new(),
				previous_guard: Some(self.lock_event_id),
			});
			write_file(&lock_state);
			LOCKS.insert(self.id, lock_state);
		}
	}
}

impl<T: Send> Deref for MutexGuard<'_, T> {
	type Target = T;

	fn deref(&self) -> &T {
		self.guard.deref()
	}
}

impl<T: Send> DerefMut for MutexGuard<'_, T> {
	fn deref_mut(&mut self) -> &mut T {
		self.guard.deref_mut()
	}
}
