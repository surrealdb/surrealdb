use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::MutexGuard as RealMutexGuard;
use std::sync::RwLock as RealRwLock;
use std::sync::RwLockWriteGuard as RealRwLockWriteGuard;
use std::sync::{Arc, Mutex as RealMutex};
use ulid::Ulid;

static mut LOCKS: lockfree::map::Map<Ulid, LockState> = lockfree::map::Map::new();

enum LockState {
	RwLock(RwLockState),
	Mutex(MutexLockState),
}

enum RwLockState {
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
}

enum MutexLockState {
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
}

pub struct Mutex<T: ?Sized> {
	name: &'static str,
	id: Ulid,
	mutex: RealMutex<T>,
}

unsafe impl<T: ?Sized + Send> Send for Mutex<T> {}

impl<T: ?Sized + Send> Mutex<T> {
	/// Creates a new instance of a `Mutex<T>` which is unlocked.
	/// This particular implementation is for traceability
	#[track_caller]
	pub fn new(value: T, name: &str) -> Mutex<T>
	where
		T: Sized,
	{
		let id = Ulid::new();
		unsafe {
			LOCKS.insert(
				id,
				LockState::Mutex(MutexLockState::MutexUnlocked {
					name,
					id,
					event_id: Ulid::new(),
					previous_guard: None,
				}),
			);
		}
		Mutex {
			name,
			id,
			mutex: RealMutex::new(value),
		}
	}

	pub async fn lock(&self) -> MutexGuard<T> {
		let request_event = Ulid::new();
		unsafe {
			LOCKS.insert(
				self.id,
				LockState::Mutex(MutexLockState::MutexRequested {
					name: self.name,
					id: self.id,
					event_id: request_event,
				}),
			);
		}
		let guard = loop {
			match self.mutex.try_lock() {
				Ok(guard) => break guard,
				Err(_) => {
					tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
				}
			}
		};
		let guard = MutexGuard {
			name: self.name,
			id: self.id,
			lock_event_id: request_event,
			guard: Arc::new(RealRwLock::new(guard)),
			_phantom: Default::default(),
		};
		unsafe {
			LOCKS.insert(
				self.id,
				LockState::Mutex(MutexLockState::MutexLocked {
					name: self.name,
					id: self.id,
					event_id: Ulid::new(),
				}),
			);
		}
		guard
	}
}

#[must_use = "if unused the RwLock will immediately unlock"]
// experimental
// #[must_not_suspend = "holding a RwLockWriteGuard across suspend \
//                       points can cause deadlocks, delays, \
//                       and cause Future's to not implement `Send`"]
#[clippy::has_significant_drop]
pub struct MutexGuard<'a, T: ?Sized + 'a + Send> {
	name: &'static str,
	id: Ulid,
	lock_event_id: Ulid,
	// The absolute irony that this must be Send across threads
	guard: Arc<RealRwLock<RealMutexGuard<'a, T>>>,
	_phantom: PhantomData<&'a T>,
}

unsafe impl<T: ?Sized + Send> Send for MutexGuard<'_, T> {}

impl<'a, T: ?Sized + 'a + Send> Drop for MutexGuard<'a, T> {
	fn drop(&mut self) {
		unsafe {
			LOCKS.insert(
				self.id,
				LockState::Mutex(MutexLockState::MutexUnlocked {
					name: self.name,
					id: self.id,
					event_id: Ulid::new(),
					previous_guard: Some(self.lock_event_id),
				}),
			);
		}
	}
}

impl<T: Send> Deref for MutexGuard<'_, T> {
	type Target = T;

	fn deref(&self) -> &T {
		self.guard.read().unwrap().deref()
	}
}

impl<T: Send> DerefMut for MutexGuard<'_, T> {
	fn deref_mut(&mut self) -> &mut T {
		self.guard.write().unwrap().deref_mut()
	}
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
	pub fn new(value: T, name: &str) -> RwLock<T>
	where
		T: Sized,
	{
		let id = Ulid::new();
		unsafe {
			LOCKS.insert(
				id,
				LockState::RwLock(RwLockState::RwUnlocked {
					name,
					id,
					event_id: Ulid::new(),
					previous_guard: None,
				}),
			);
		}
		RwLock {
			name,
			id,
			rwlock: RealRwLock::new(value),
		}
	}

	pub async fn write(&self) -> RwLockWriteGuard<T> {
		let request_event = Ulid::new();
		unsafe {
			LOCKS.insert(
				self.id,
				LockState::RwLock(RwLockState::RwLockRequested {
					name: self.name,
					id: self.id,
					event_id: request_event,
				}),
			);
		}
		let guard = loop {
			match self.rwlock.try_write() {
				Ok(guard) => break guard,
				Err(_) => {
					tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
				}
			}
		};
		let guard = RwLockWriteGuard {
			name: self.name,
			id: self.id,
			lock_event_id: request_event,
			guard,
			_phantom: Default::default(),
		};
		unsafe {
			LOCKS.insert(
				self.id,
				LockState::RwLock(RwLockState::RwLocked {
					name: self.name,
					id: self.id,
					event_id: Ulid::new(),
					lock_event_id: request_event,
				}),
			);
		}
		guard
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
		unsafe {
			LOCKS.insert(
				self.id,
				LockState::RwLock(RwLockState::RwUnlocked {
					name: self.name,
					id: self.id,
					event_id: Ulid::new(),
					previous_guard: Some(self.lock_event_id),
				}),
			);
		}
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
