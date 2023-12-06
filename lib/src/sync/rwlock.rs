use crate::sync::{write_file, CsvEntry, LockState, LOCKS};
use async_std::sync::RwLock as RealRwLock;
use async_std::sync::RwLockWriteGuard as RealRwLockWriteGuard;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use ulid::Ulid;

pub enum RwLockState {
	RwLockRequested {
		name: &'static str,
		id: Ulid,
		event_id: Ulid,
	},
	RwLocked {
		name: &'static str,
		id: Ulid,
		event_id: Ulid,
		lock_event_id: Ulid,
	},
	RwUnlocked {
		name: &'static str,
		id: Ulid,
		event_id: Ulid,
		previous_guard: Option<Ulid>,
	},
	RwReadRequested {
		name: &'static str,
		id: Ulid,
		event_id: Ulid,
	},
	RwReadLocked {
		name: &'static str,
		id: Ulid,
		event_id: Ulid,
	},
	RwDestroyed {
		name: &'static str,
		id: Ulid,
		event_id: Ulid,
	},
}

pub struct RwLock<T: ?Sized + Send> {
	name: &'static str,
	id: Ulid,
	rwlock: RealRwLock<T>,
}

unsafe impl<T: ?Sized + Send> Send for RwLock<T> {}

impl<T: ?Sized + Send> RwLock<T> {
	/// Creates a new instance of an `RwLock<T>` which is unlocked.
	/// This particular implementation is for traceability
	#[track_caller]
	pub fn new(value: T, name: &'static str) -> RwLock<T>
	where
		T: Sized,
	{
		let id = Ulid::new();
		RwLock::unlock_event(id, name, None);
		RwLock {
			name,
			id,
			rwlock: RealRwLock::new(value),
		}
	}

	pub async fn write(&self) -> RwLockWriteGuard<T> {
		let request_event = Ulid::new();
		RwLock::lock_requested_event(self.id, self.name, request_event);
		let guard = self.rwlock.write().await;
		let guard = RwLockWriteGuard {
			name: self.name,
			id: self.id,
			lock_event_id: request_event,
			guard,
			_phantom: Default::default(),
		};
		RwLock::lock_ack_event(self.id, self.name, request_event);
		guard
	}

	pub fn lock_requested_event(id: Ulid, name: &'static str, request_event: Ulid) {
		unsafe {
			let lock_state = LockState::RwLock(RwLockState::RwLockRequested {
				name,
				id,
				event_id: request_event,
			});
			write_file(&lock_state);
			LOCKS.insert(id, lock_state);
		}
	}

	pub fn lock_ack_event(id: Ulid, name: &'static str, request_event: Ulid) {
		unsafe {
			let lock_state = LockState::RwLock(RwLockState::RwLocked {
				name,
				id,
				event_id: Ulid::new(),
				lock_event_id: request_event,
			});
			write_file(&lock_state);
			LOCKS.insert(id, lock_state);
		}
	}

	pub fn unlock_event(id: Ulid, name: &str, lock_event_id: Option<Ulid>) {
		unsafe {
			let lock_state = LockState::RwLock(RwLockState::RwUnlocked {
				name,
				id,
				event_id: Ulid::new(),
				previous_guard: lock_event_id,
			});
			write_file(&lock_state);
			LOCKS.insert(id, lock_state);
		}
	}
}

impl<T: ?Sized + Send> Drop for RwLock<T> {
	fn drop(&mut self) {
		unsafe {
			let lock_state = LockState::RwLock(RwLockState::RwUnlocked {
				name: self.name,
				id: self.id,
				event_id: Ulid::new(),
				previous_guard: None,
			});
			LOCKS.insert(self.id, lock_state);
		}
	}
}

#[must_use = "if unused the RwLock will immediately unlock"]
// experimental
// #[must_not_suspend = "holding a RwLockWriteGuard across suspend \
//                       points can cause deadlocks, delays, \
//                       and cause Future's to not implement `Send`"]
pub struct RwLockWriteGuard<'a, T: ?Sized + 'a + Send> {
	name: &'static str,
	id: Ulid,
	lock_event_id: Ulid,
	guard: RealRwLockWriteGuard<'a, T>,
	_phantom: PhantomData<&'a T>,
}

impl<'a, T: ?Sized + 'a + Send> Drop for RwLockWriteGuard<'a, T> {
	fn drop(&mut self) {
		RwLock::unlock_event(self.id, self.name, Some(self.lock_event_id))
	}
}

impl<T: Send> Deref for RwLockWriteGuard<'_, T> {
	type Target = T;

	fn deref(&self) -> &T {
		self.guard.deref()
	}
}

impl<T: Send> DerefMut for RwLockWriteGuard<'_, T> {
	fn deref_mut(&mut self) -> &mut T {
		self.guard.deref_mut()
	}
}
