//! Stores a LIVE SELECT query notification on the table, ordered by time
use crate::dbs::node::Timestamp;
use derive::Key;
use serde::{Deserialize, Serialize};

/// Nt is used to track live query notifications for remote nodes (nodes, from which
/// the notification wasn't generated, but which the notification belongs to).
/// They are ordered by time, where time depends on the storage engine.
///
/// Notifications are on a different prefix to the table live queries, because when we scan table
/// live queries, we do not want to pick up notifications.
///
/// The value of the entry is a Notification model.
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Key)]
pub struct Nt<'a> {
	__: u8,
	_a: u8,
	pub ns: &'a str,
	_b: u8,
	pub db: &'a str,
	_c: u8,
	pub tb: &'a str,
	_d: u8,
	_e: u8,
	_f: u8,
	// We will only be interested in a specific lq notification list
	#[serde(with = "uuid::serde::compact")]
	pub lq: uuid::Uuid,
	_g: u8,
	// We want the notifications to be ordered by timestamp
	pub ts: Timestamp,
	_h: u8,
	// If timestamps collide, we still want uniqueness
	#[serde(with = "uuid::serde::compact")]
	pub nt: uuid::Uuid,
}

pub fn new<'a>(
	ns: &'a str,
	db: &'a str,
	tb: &'a str,
	lq: crate::sql::Uuid,
	ts: Timestamp,
	id: crate::sql::Uuid,
) -> Nt<'a> {
	Nt::new(ns, db, tb, lq, ts, id)
}

pub fn prefix(ns: &str, db: &str, tb: &str, lq: crate::sql::Uuid) -> Vec<u8> {
	let mut k = super::all::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[b'!', b'n', b't']);
	k.extend_from_slice(lq.0.as_bytes());
	k.extend_from_slice(&[b'!', 0x00]);
	k
}

pub fn suffix(ns: &str, db: &str, tb: &str, lq: crate::sql::Uuid) -> Vec<u8> {
	let mut k = super::all::new(ns, db, tb).encode().unwrap();
	k.extend_from_slice(&[b'!', b'n', b't']);
	k.extend_from_slice(lq.0.as_bytes());
	k.extend_from_slice(&[b'!', 0xff]);
	k
}

impl<'a> Nt<'a> {
	pub fn new(
		ns: &'a str,
		db: &'a str,
		tb: &'a str,
		lq: crate::sql::Uuid,
		ts: Timestamp,
		id: crate::sql::Uuid,
	) -> Self {
		Self {
			__: b'/',
			_a: b'*',
			ns,
			_b: b'*',
			db,
			_c: b'*',
			tb,
			_d: b'!',
			_e: b'n',
			_f: b't',
			lq: lq.0,
			_g: b'!',
			ts,
			_h: b'!',
			nt: id.0,
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::key::debug;

	#[test]
	fn key() {
		use super::*;
		#[rustfmt::skip]
        let live_query_id = crate::sql::Uuid::from(uuid::Uuid::from_bytes([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]));
		let ts: Timestamp = Timestamp {
			value: 0x0102030405060708,
		};
		let id = crate::sql::Uuid::from(uuid::Uuid::from_bytes([
			0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
			0x1E, 0x1F,
		]));
		let key = Nt::new("testns", "testdb", "testtb", live_query_id, ts, id);
		// let enc = Nt::encode(&key).unwrap();
		let key_enc = key.encode().unwrap();
		assert_eq!(
			key_enc,
			b"/*testns\x00\
            *testdb\x00\
            *testtb\x00\
            !nt\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\
            !\x01\x02\x03\x04\x05\x06\x07\x08\
            !\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f"
		);

		let dec = Nt::decode(&key_enc).unwrap();
		assert_eq!(key, dec);
	}

	#[test]
	fn prefix() {
		let live_query_id = crate::sql::Uuid::from(uuid::Uuid::from_bytes([
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		]));

		let val = super::prefix("testns", "testdb", "testtb", live_query_id);
		assert_eq!(
			val,
			b"/*testns\x00\
            *testdb\x00\
            *testtb\x00\
            !nt\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\
            !\0"
		)
	}

	#[test]
	fn suffix() {
		let live_query_id = crate::sql::Uuid::from(uuid::Uuid::from_bytes([
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		]));
		let val = super::suffix("testns", "testdb", "testtb", live_query_id);
		assert_eq!(
			val,
			b"/*testns\x00\
            *testdb\x00\
            *testtb\x00\
            !nt\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\
            !\xff"
		)
	}
}
