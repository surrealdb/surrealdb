pub(crate) mod mutex;
mod rwlock;

use async_std::sync::RwLock as RealRwLock;
use once_cell::sync::Lazy;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::Write;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender};
use ulid::Ulid;

pub use mutex::Mutex;
pub use mutex::MutexLockState;
pub use rwlock::RwLock;
pub use rwlock::RwLockState;

static mut LOCKS: Lazy<lockfree::map::Map<Ulid, LockState>> =
	Lazy::new(|| lockfree::map::Map::new());

static mut LOG: RealRwLock<Lazy<File>> = RealRwLock::new(Lazy::new(|| {
	let mut file = File::create("lock.csv").unwrap();
	write_header(&mut file);
	file
}));

static mut FILE_BUF_CHAN: Lazy<(Sender<String>, Receiver<String>)> =
	Lazy::new(|| tokio::sync::mpsc::channel(100));
static mut BLOCKED_CHAN: AtomicBool = AtomicBool::new(false);

fn write_header(bw: &mut File) {
	println!("Writing header");
	let header = format!(
		"\
		timestamp\
		,id\
		,name\
		,event_type\
		,event_id\
		,previous_event\
		,current_event\
		,metadata\
		\n"
	);
	bw.write(header.as_bytes()).unwrap();
}

pub fn write_file(lock_state: &LockState) {
	unsafe {
		let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
		let id = lock_state.id();
		let name = lock_state.name();
		let event_type = lock_state.to_string();
		let event_id = lock_state.event_id().map_or("".to_string(), |id| id.to_string());
		let previous_event =
			lock_state.previous_event().map(|id| id.to_string()).unwrap_or("".to_string());
		let current_event = id
			.map(|id| LOCKS.get(id))
			.flatten()
			.map(|state| state.val().event_id().copied())
			.flatten()
			.map_or("".to_string(), |id| id.to_string());
		let metadata = lock_state.metadata();

		let id = id.map_or("".to_string(), |id| id.to_string());

		let msg = format!(
		"{timestamp},{id},{name},{event_type},{event_id},{previous_event},{current_event},{metadata}\n");
		write_file_raw(msg);
	}
}

unsafe fn write_file_raw(msg: String) {
	loop {
		// We don't need to maintain lock, only prevent excessive constant writes
		// NOTE this isn't safe, as it isn't a lock.
		// After the read something can come in and acquire the lock - it is a lockless read.
		// But we don't care about it, as it is here only to prevent consecutive writes leading to
		// infinite loop of consuming events during leader write.
		if let true = BLOCKED_CHAN.load(Ordering::Relaxed) {
			// tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
			sleep(std::time::Duration::from_millis(1));
			continue;
		}
		match FILE_BUF_CHAN.0.try_send(msg.clone()) {
			Ok(_) => {
				break;
			}
			Err(TrySendError::Full(_string)) => {
				// block and prevent other writer leaders
				if let Err(e) =
					BLOCKED_CHAN.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
				{
					// Something else acquired the lock, so we become a writer follower by continuing the loop
					continue;
				}
				// as writer leader, now we need to write previous messages
				loop {
					match LOG.try_write() {
						None => {
							sleep(std::time::Duration::from_millis(1));
						}
						Some(mut log_lock) => {
							while let Ok(msg) = FILE_BUF_CHAN.1.try_recv() {
								let file = log_lock.deref_mut().deref_mut();
								file.write(msg.as_bytes()).unwrap();
							}
							break;
						}
					}
				}
				// unblock other writers; this shouldn't actually fail
				BLOCKED_CHAN
					.compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
					.unwrap();
			}
			Err(TrySendError::Closed(_string)) => {
				panic!("FILE_BUF_CHAN closed");
			}
		}
	}
}

pub(crate) enum LockState {
	RwLock(RwLockState),
	Mutex(MutexLockState),
	Metadata {
		name: &'static str,
		event_id: Ulid,
		metadata: String,
	},
}

impl LockState {
	pub fn id(&self) -> Option<&Ulid> {
		match self {
			LockState::RwLock(RwLockState::RwLocked {
				id,
				..
			}) => Some(id),
			LockState::RwLock(RwLockState::RwLockRequested {
				id,
				..
			}) => Some(id),
			LockState::RwLock(RwLockState::RwUnlocked {
				id,
				..
			}) => Some(id),
			LockState::RwLock(RwLockState::RwReadRequested {
				id,
				..
			}) => Some(id),
			LockState::RwLock(RwLockState::RwReadLocked {
				id,
				..
			}) => Some(id),
			LockState::Mutex(MutexLockState::MutexRequested {
				id,
				..
			}) => Some(id),
			LockState::Mutex(MutexLockState::MutexLocked {
				id,
				..
			}) => Some(id),
			LockState::Mutex(MutexLockState::MutexUnlocked {
				id,
				..
			}) => Some(id),
			LockState::Mutex(MutexLockState::MutexDestroyed {
				id,
				..
			}) => Some(id),
			LockState::RwLock(RwLockState::RwDestroyed {
				id,
				..
			}) => Some(id),
			LockState::Metadata {
				..
			} => None,
		}
	}

	pub fn name(&self) -> &str {
		match self {
			LockState::RwLock(RwLockState::RwLocked {
				name,
				..
			}) => name,
			LockState::RwLock(RwLockState::RwLockRequested {
				name,
				..
			}) => name,
			LockState::RwLock(RwLockState::RwUnlocked {
				name,
				..
			}) => name,
			LockState::RwLock(RwLockState::RwReadRequested {
				name,
				..
			}) => name,
			LockState::RwLock(RwLockState::RwReadLocked {
				name,
				..
			}) => name,
			LockState::Mutex(MutexLockState::MutexRequested {
				name,
				..
			}) => name,
			LockState::Mutex(MutexLockState::MutexLocked {
				name,
				..
			}) => name,
			LockState::Mutex(MutexLockState::MutexUnlocked {
				name,
				..
			}) => name,
			LockState::Mutex(MutexLockState::MutexDestroyed {
				name,
				..
			}) => name,
			LockState::RwLock(RwLockState::RwDestroyed {
				name,
				..
			}) => name,
			LockState::Metadata {
				name,
				..
			} => name,
		}
	}

	pub fn event_id(&self) -> Option<&Ulid> {
		match self {
			LockState::RwLock(RwLockState::RwLocked {
				event_id,
				..
			}) => Some(event_id),
			LockState::RwLock(RwLockState::RwLockRequested {
				event_id,
				..
			}) => Some(event_id),
			LockState::RwLock(RwLockState::RwUnlocked {
				event_id,
				..
			}) => Some(event_id),
			LockState::RwLock(RwLockState::RwReadRequested {
				event_id,
				..
			}) => Some(event_id),
			LockState::RwLock(RwLockState::RwReadLocked {
				event_id,
				..
			}) => Some(event_id),
			LockState::Mutex(MutexLockState::MutexRequested {
				event_id,
				..
			}) => Some(event_id),
			LockState::Mutex(MutexLockState::MutexLocked {
				event_id,
				..
			}) => Some(event_id),
			LockState::Mutex(MutexLockState::MutexUnlocked {
				event_id,
				..
			}) => Some(event_id),
			LockState::Mutex(MutexLockState::MutexDestroyed {
				event_id,
				..
			}) => Some(event_id),
			LockState::RwLock(RwLockState::RwDestroyed {
				event_id,
				..
			}) => Some(event_id),
			LockState::Metadata {
				event_id,
				..
			} => Some(event_id),
		}
	}

	pub fn previous_event(&self) -> &Option<Ulid> {
		match self {
			LockState::RwLock(RwLockState::RwUnlocked {
				previous_guard,
				..
			}) => previous_guard,
			LockState::Mutex(MutexLockState::MutexUnlocked {
				previous_guard,
				..
			}) => previous_guard,
			_ => &None,
		}
	}

	pub fn metadata(&self) -> &str {
		match self {
			LockState::Metadata {
				metadata,
				..
			} => metadata,
			_ => "",
		}
	}
}

impl Display for LockState {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			LockState::RwLock(RwLockState::RwLocked {
				..
			}) => f.write_str("RwLocked")?,
			LockState::RwLock(RwLockState::RwLockRequested {
				..
			}) => f.write_str("RwLockRequested")?,
			LockState::RwLock(RwLockState::RwUnlocked {
				..
			}) => f.write_str("RwUnlocked")?,
			LockState::RwLock(RwLockState::RwReadRequested {
				..
			}) => f.write_str("RwReadRequested")?,
			LockState::RwLock(RwLockState::RwReadLocked {
				..
			}) => f.write_str("RwReadLocked")?,
			LockState::Mutex(MutexLockState::MutexRequested {
				..
			}) => f.write_str("MutexRequested")?,
			LockState::Mutex(MutexLockState::MutexLocked {
				..
			}) => f.write_str("MutexLocked")?,
			LockState::Mutex(MutexLockState::MutexUnlocked {
				..
			}) => f.write_str("MutexUnlocked")?,
			LockState::Mutex(MutexLockState::MutexDestroyed {
				..
			}) => f.write_str("MutexDestroyed")?,
			LockState::RwLock(RwLockState::RwDestroyed {
				..
			}) => f.write_str("RwDestroyed")?,
			LockState::Metadata {
				..
			} => {
				f.write_str("Metadata")?;
			}
		}
		Ok(())
	}
}
