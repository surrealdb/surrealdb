mod mutex;
mod rwlock;

use async_std::fs::File;
use async_std::io::{BufWriter, WriteExt};
use async_std::sync::{RwLock as RealRwLock, RwLockWriteGuard};
use futures::AsyncWriteExt;
use once_cell::sync::Lazy;
use std::fmt::{Display, Formatter};
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
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

static mut LOG: RealRwLock<Lazy<BufWriter<File>>> = RealRwLock::new(Lazy::new(|| {
	let file = File::create("lock.log").unwrap();
	let mut bw = BufWriter::new(file);
	write_header(&mut bw);
	bw
}));

static mut file_buf_chan: (Sender<String>, Receiver<String>) = tokio::sync::mpsc::channel(100);
static mut blocked_chan: AtomicBool = AtomicBool::new(false);

struct CsvEntry<'a> {
	pub id: Ulid,
	pub name: &'a str,
	pub event_type: &'a LockState,
}

fn write_header(bw: &mut BufWriter<File>) {
	let header = format!(
		"id,\
		name,\
		event_type,\
		previous_event,\n
		\n"
	);
	bw.write(header.as_bytes());
}

unsafe fn write_file(lock_state: &LockState) {
	let id = lock_state.id();
	let name = lock_state.name();
	let event_type = lock_state.to_string();
	let previous_event = lock_state.previous_event();
	let msg = format!("{id},{name},{event_type}\n",);
	write_file_raw(msg);
}

unsafe fn write_file_raw(msg: String) {
	loop {
		// We don't need to maintain lock, only prevent excessive constant writes
		// NOTE this isn't safe, as it isn't a lock.
		// After the read something can come in and acquire the lock - it is a lockless read.
		// But we don't care about it, as it is here only to prevent consecutive writes leading to
		// infinite loop of consuming events during leader write.
		if let false = blocked_chan.load(Ordering::Relaxed) {
			// tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
			sleep(std::time::Duration::from_millis(1));
			continue;
		}
		match file_buf_chan.0.try_send(msg.clone()) {
			Ok(_) => {
				break;
			}
			Err(TrySendError::Full(_string)) => {
				// block and prevent other writer leaders
				if let Err(e) =
					blocked_chan.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
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
						Some(log_lock) => {
							while let Some(msg) = file_buf_chan.1.try_recv() {
								log_lock.write(msg.as_bytes()).unwrap();
							}
							break;
						}
					}
				}
				// unblock other writers; this shouldn't actually fail
				blocked_chan
					.compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
					.unwrap();
			}
			Err(TrySendError::Closed(_string)) => {
				panic!("file_buf_chan closed");
			}
		}
	}
}

enum LockState {
	RwLock(RwLockState),
	Mutex(MutexLockState),
}

impl LockState {
	pub fn id(&self) -> &Ulid {
		match self {
			LockState::RwLock(RwLockState::RwLocked {
				id,
				..
			}) => id,
			LockState::RwLock(RwLockState::RwLockRequested {
				id,
				..
			}) => id,
			LockState::RwLock(RwLockState::RwUnlocked {
				id,
				..
			}) => id,
			LockState::RwLock(RwLockState::RwReadRequested {
				id,
				..
			}) => id,
			LockState::RwLock(RwLockState::RwReadLocked {
				id,
				..
			}) => id,
			LockState::Mutex(MutexLockState::MutexRequested {
				id,
				..
			}) => id,
			LockState::Mutex(MutexLockState::MutexLocked {
				id,
				..
			}) => id,
			LockState::Mutex(MutexLockState::MutexUnlocked {
				id,
				..
			}) => id,
			LockState::Mutex(MutexLockState::MutexDestroyed {
				id,
				..
			}) => id,
			LockState::RwLock(RwLockState::RwDestroyed {
				id,
				..
			}) => id,
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
		}
	}

	pub fn
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
		}
		Ok(())
	}
}
