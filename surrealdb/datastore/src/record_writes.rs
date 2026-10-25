//! The tables a transaction has written records of.
//!
//! A reader that scans a table in transactions of its own sees the table as
//! last committed, never what the transaction that started it has written. An
//! index build that blocks the statement defining the index is such a reader,
//! so it would leave out every record its defining transaction wrote before the
//! definition. [`crate::Transaction::wrote_records_of`] is how that statement
//! asks whether there is anything the build would miss.

use std::collections::HashSet;

/// The bytes every key of one table's keyspace starts with: `/*{ns}*{db}*{tb}`,
/// the table's name escaped and terminated as a key spells it. `None` when
/// `key` lies in no table's keyspace.
fn table_keyspace(key: &[u8]) -> Option<&[u8]> {
	// `/*`, the namespace id, `*`, the database id, `*`: the ids are four
	// bytes each.
	if key.len() < 12 || key[..2] != *b"/*" || key[6] != b'*' || key[11] != b'*' {
		return None;
	}
	// A zero byte ends the name; a byte below two inside it is escaped by a one.
	let mut at = 12;
	loop {
		match *key.get(at)? {
			0 => return Some(&key[..=at]),
			1 => at += 2,
			_ => at += 1,
		}
	}
}

/// The record prefix of the table whose record `key` is, `/*{ns}*{db}*{tb}*`,
/// which every record key of that table starts with; `None` when `key` is no
/// record's key.
pub(crate) fn record_prefix(key: &[u8]) -> Option<&[u8]> {
	let table = table_keyspace(key)?;
	(key.get(table.len()) == Some(&b'*')).then(|| &key[..=table.len()])
}

/// The first key past every key starting with `prefix`, which ends with the `*`
/// of a step down the keyspace: `prefix` with its last byte raised by one.
fn past(prefix: &[u8]) -> Vec<u8> {
	let (last, stem) = prefix.split_last().expect("a prefix ends with `*`");
	[stem, &[last + 1]].concat()
}

/// Whether `[start, end)` holds a key starting with `prefix`, which ends with
/// the `*` of a step down the keyspace.
fn overlaps(start: &[u8], end: &[u8], prefix: &[u8]) -> bool {
	start < past(prefix).as_slice() && prefix < end
}

/// The part of `[start, end)` that holds the keys starting with `prefix`, which
/// ends with the `*` of a step down the keyspace. Empty unless
/// [`overlaps`]`(start, end, prefix)`.
pub(crate) fn among(start: &[u8], end: &[u8], prefix: &[u8]) -> (Vec<u8>, Vec<u8>) {
	(start.max(prefix).to_vec(), end.min(past(prefix).as_slice()).to_vec())
}

/// What a write reached of the tables' records, noted once the write succeeds
/// ([`RecordWrites::note`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Reached {
	/// The records of the table with this record prefix.
	Table(Vec<u8>),
	/// Whatever records lie in the range `[start, end)`, which reaches beyond
	/// one table's keyspace.
	Range(Vec<u8>, Vec<u8>),
}

impl Reached {
	/// What a clear of `[start, end)` reaches if every key it covers is stored:
	/// `None` when no record key lies in the range. A [`Reached::Table`] is the
	/// whole answer only when a record of the table is stored in the range, which
	/// the caller confirms.
	pub(crate) fn by_range(start: &[u8], end: &[u8]) -> Option<Reached> {
		// The range lies in one table's keyspace when it starts there and ends
		// no later than the keyspace does: it reaches that table's records where
		// it meets them.
		if let Some(table) = table_keyspace(start)
			&& end <= past(table).as_slice()
		{
			let records = [table, b"*"].concat();
			return overlaps(start, end, &records).then_some(Reached::Table(records));
		}
		// Every key of the range starts with the bytes both bounds start with.
		let shared = &start[..start.iter().zip(end).take_while(|(a, b)| a == b).count()];
		// A record key is `/*{ns}*{db}*{tb}*{id}`, so every record beneath the
		// deepest level the shared bytes name — the root, a namespace or a
		// database — starts with that level's bytes and a `*`. A range that
		// meets no such key holds no record, and one that does is kept whole.
		let range = || Some(Reached::Range(start.to_vec(), end.to_vec()));
		let Some(step) = [11, 6, 1].into_iter().find(|&at| shared.len() >= at) else {
			return range();
		};
		let named = &shared[..step];
		let sigils = [(0, b'/'), (1, b'*'), (6, b'*')];
		if sigils.iter().any(|&(at, sigil)| named.get(at).is_some_and(|&byte| byte != sigil)) {
			return None;
		}
		match overlaps(start, end, &[named, b"*"].concat()) {
			true => range(),
			false => None,
		}
	}
}

/// Which tables' records a transaction has written, as its storage writes
/// spell them.
///
/// A single-key write or delete of a record's key, `/*{ns}*{db}*{tb}*{id}`,
/// reaches that record's table; so does a range delete within one table's
/// keyspace that removes a record the transaction can see. Any other range
/// delete is kept as its range, and reaches every table whose records it
/// overlaps, whether or not it removes one. What a write reached is noted once
/// the write succeeds, and forgotten when a save point rollback undoes it.
#[derive(Debug, Default)]
pub(crate) struct RecordWrites {
	/// The record prefix of each table noted, in the order noted.
	tables: Vec<Vec<u8>>,
	/// The prefixes in [`Self::tables`], for the check every record write
	/// makes.
	noted: HashSet<Vec<u8>>,
	/// The ranges noted, in the order noted.
	ranges: Vec<(Vec<u8>, Vec<u8>)>,
	/// How many tables and ranges were noted when each open save point
	/// opened, innermost last.
	save_points: Vec<(usize, usize)>,
}

impl RecordWrites {
	/// Whether the table with record prefix `records` is noted.
	pub(crate) fn noted(&self, records: &[u8]) -> bool {
		// A run of writes mostly writes one table, the one noted last.
		self.tables.last().is_some_and(|last| last == records) || self.noted.contains(records)
	}

	/// Note what a write that succeeded reached.
	pub(crate) fn note(&mut self, reached: Reached) {
		match reached {
			Reached::Table(records) => {
				if !self.noted.contains(&records) {
					self.noted.insert(records.clone());
					self.tables.push(records);
				}
			}
			Reached::Range(start, end) => self.ranges.push((start, end)),
		}
	}

	/// Whether a noted write reached a record of the table with record prefix
	/// `records`.
	pub(crate) fn wrote(&self, records: &[u8]) -> bool {
		self.noted.contains(records)
			|| self.ranges.iter().any(|(start, end)| overlaps(start, end, records))
	}

	/// Open a save point: what is noted from here on is forgotten if it rolls
	/// back.
	pub(crate) fn push_save_point(&mut self) {
		self.save_points.push((self.tables.len(), self.ranges.len()));
	}

	/// Release the last save point: what was noted inside it now belongs to
	/// the enclosing one.
	pub(crate) fn release_save_point(&mut self) {
		self.save_points.pop();
	}

	/// Roll back the last save point, forgetting what was noted inside it.
	pub(crate) fn rollback_save_point(&mut self) {
		let Some((tables, ranges)) = self.save_points.pop() else {
			return;
		};
		for records in self.tables.drain(tables.min(self.tables.len())..) {
			self.noted.remove(&records);
		}
		self.ranges.truncate(ranges);
	}
}

#[cfg(test)]
mod tests {
	use std::borrow::Cow;

	use surrealdb_catalog::{DatabaseId, IndexId, NamespaceId};
	use surrealdb_expr::val::{RecordIdKey, TableName};
	use surrealdb_kvs::key::{KVKey, KVSubspace};

	use super::*;
	use crate::key::schema::{
		ChangeFeedPrefix, DbRoot, GraphIdPrefix, IdxRoot, IndexDefKey, NamespacePrefix, RecordKey,
		RecordPrefix, TableKey, TablePrefix, TblRoot,
	};

	const NS: NamespaceId = NamespaceId(1);
	const DB: DatabaseId = DatabaseId(2);

	/// Names whose encodings escape bytes, or hold the sigils that follow a
	/// table's name in its keyspace.
	fn names() -> Vec<TableName> {
		["user", "a\u{1}b", "a\u{0}b", "\u{1}", "x*y", "x+y", "x~y", "x!y", "ünï", "u"]
			.into_iter()
			.map(TableName::from)
			.collect()
	}

	fn record(tb: &TableName, id: i64) -> Vec<u8> {
		let id = RecordIdKey::Number(id);
		RecordKey::new(NS, DB, Cow::Borrowed(tb), Cow::Borrowed(&id)).encode_key().unwrap().to_vec()
	}

	fn records(tb: &TableName) -> Vec<u8> {
		RecordPrefix {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(tb),
		}
		.encode_bound()
		.unwrap()
		.to_vec()
	}

	fn range(bound: &impl KVSubspace) -> (Vec<u8>, Vec<u8>) {
		let range = surrealdb_kvs::key::AnyRange::into_key_range(bound.raw_range().unwrap());
		(range.start.to_vec(), range.end.to_vec())
	}

	#[test]
	fn a_record_key_names_its_table_by_the_prefix_a_scan_of_its_records_uses() {
		for tb in names() {
			assert_eq!(record_prefix(&record(&tb, 7)), Some(records(&tb).as_slice()), "{tb:?}");
		}
	}

	#[test]
	fn no_other_key_of_a_table_is_a_record_key() {
		for tb in names() {
			let table = Cow::Borrowed(&tb);
			let id = RecordIdKey::Number(7);
			let keys = [
				TableKey::new(NS, DB, table.clone()).encode_key().unwrap().to_vec(),
				IndexDefKey::new(NS, DB, table.clone(), Cow::Borrowed("ix"))
					.encode_key()
					.unwrap()
					.to_vec(),
				GraphIdPrefix {
					ns: NS,
					db: DB,
					tb: table.clone(),
					id: Cow::Borrowed(&id),
				}
				.encode_bound()
				.unwrap()
				.to_vec(),
				IdxRoot {
					ns: NS,
					db: DB,
					tb: table.clone(),
					ix: IndexId(3),
				}
				.encode_bound()
				.unwrap()
				.to_vec(),
			];
			for key in keys {
				assert_eq!(record_prefix(&key), None, "{tb:?}: {key:?}");
			}
		}
	}

	#[test]
	fn a_range_within_a_table_reaches_its_records_only_where_it_meets_them() {
		let tb = TableName::from("user");
		let (start, end) = range(&RecordPrefix {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(&tb),
		});
		assert_eq!(Reached::by_range(&start, &end), Some(Reached::Table(records(&tb))));
		// What removing the table clears: its whole keyspace.
		let (start, end) = range(&TblRoot {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(&tb),
		});
		assert_eq!(Reached::by_range(&start, &end), Some(Reached::Table(records(&tb))));
		let (start, end) = range(&IdxRoot {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(&tb),
			ix: IndexId(3),
		});
		assert_eq!(Reached::by_range(&start, &end), None);
		let id = RecordIdKey::Number(7);
		let (start, end) = range(&GraphIdPrefix {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(&tb),
			id: Cow::Borrowed(&id),
		});
		assert_eq!(Reached::by_range(&start, &end), None);
	}

	#[test]
	fn the_part_of_a_range_among_a_tables_records_is_the_records_it_holds() {
		let tb = TableName::from("user");
		let records = records(&tb);
		let past = past(&records);
		// The table's whole keyspace holds the records and what lies beside them.
		let (start, end) = range(&TblRoot {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(&tb),
		});
		assert_eq!(among(&start, &end, &records), (records.clone(), past.clone()));
		// A range among the records is itself, and one that starts before them or ends
		// after them is cut at their bounds.
		let (first, last) = (record(&tb, 1), record(&tb, 5));
		assert_eq!(among(&first, &last, &records), (first.clone(), last.clone()));
		assert_eq!(among(&start, &last, &records), (records.clone(), last));
		assert_eq!(among(&first, &end, &records), (first, past));
	}

	#[test]
	fn a_range_beneath_no_table_reaches_no_record() {
		let (start, end) = range(&TablePrefix {
			ns: NS,
			db: DB,
		});
		assert_eq!(Reached::by_range(&start, &end), None);
		let (start, end) = range(&ChangeFeedPrefix {
			ns: NS,
			db: DB,
		});
		assert_eq!(Reached::by_range(&start, &end), None);
		let (start, end) = range(&NamespacePrefix {});
		assert_eq!(Reached::by_range(&start, &end), None);
	}

	#[test]
	fn a_range_beyond_one_table_reaches_the_records_it_overlaps() {
		let user = TableName::from("user");
		let other = TableName::from("other");
		// What removing a database clears: every table in it.
		let mut writes = RecordWrites::default();
		let (start, end) = range(&DbRoot {
			ns: NS,
			db: DB,
		});
		writes.note(Reached::by_range(&start, &end).unwrap());
		assert!(writes.wrote(&records(&user)));
		assert!(writes.wrote(&records(&other)));
	}

	#[test]
	fn a_table_whose_name_extends_another_is_another_table() {
		let mut writes = RecordWrites::default();
		let user = TableName::from("user");
		writes.note(Reached::Table(record_prefix(&record(&user, 1)).unwrap().to_vec()));
		let (start, end) = range(&TblRoot {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(&user),
		});
		writes.note(Reached::by_range(&start, &end).unwrap());
		for tb in ["users", "use", "user\u{1}", "u"] {
			assert!(!writes.wrote(&records(&TableName::from(tb))), "{tb}");
		}
	}

	#[test]
	fn a_rolled_back_save_point_forgets_what_was_noted_inside_it() {
		let user = TableName::from("user");
		let other = TableName::from("other");
		let mut writes = RecordWrites::default();
		writes.push_save_point();
		writes.note(Reached::Table(records(&user)));
		writes.push_save_point();
		writes.note(Reached::Table(records(&other)));
		let (start, end) = range(&DbRoot {
			ns: NS,
			db: DB,
		});
		writes.note(Reached::by_range(&start, &end).unwrap());
		writes.rollback_save_point();
		assert!(writes.wrote(&records(&user)));
		assert!(!writes.wrote(&records(&other)));
		// A released save point folds into the one around it, and goes with it.
		writes.push_save_point();
		writes.note(Reached::Table(records(&other)));
		writes.release_save_point();
		writes.rollback_save_point();
		assert!(!writes.wrote(&records(&user)));
		assert!(!writes.wrote(&records(&other)));
		// A table noted again after its note was rolled back is noted.
		writes.note(Reached::Table(records(&user)));
		assert!(writes.wrote(&records(&user)));
	}
}
