use crate::cf::{TableMutation, TableMutations};
use crate::kvs::Key;
use crate::sql::ident::Ident;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use std::borrow::Cow;
use std::collections::HashMap;

// PreparedWrite is a tuple of (versionstamp key, key prefix, key suffix, serialized table mutations).
// The versionstamp key is the key that contains the current versionstamp and might be used by the
// specific transaction implementation to make the versionstamp unique and monotonic.
// The key prefix and key suffix are used to construct the key for the table mutations.
// The consumer of this library should write KV pairs with the following format:
// key = key_prefix + versionstamp + key_suffix
// value = serialized table mutations
type PreparedWrite = (Vec<u8>, Vec<u8>, Vec<u8>, crate::kvs::Val);

pub struct Writer {
	buf: Buffer,
}

pub struct Buffer {
	pub b: HashMap<ChangeKey, TableMutations>,
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct ChangeKey {
	pub ns: String,
	pub db: String,
	pub tb: String,
}

impl Buffer {
	pub fn new() -> Self {
		Self {
			b: HashMap::new(),
		}
	}

	pub fn push(&mut self, ns: String, db: String, tb: String, m: TableMutation) {
		let tb2 = tb.clone();
		let ms = self
			.b
			.entry(ChangeKey {
				ns,
				db,
				tb,
			})
			.or_insert(TableMutations::new(tb2));
		ms.1.push(m);
	}
}

// Writer is a helper for writing table mutations to a transaction.
impl Writer {
	pub(crate) fn new() -> Self {
		Self {
			buf: Buffer::new(),
		}
	}

	pub(crate) fn update(&mut self, ns: &str, db: &str, tb: Ident, id: Thing, v: Cow<'_, Value>) {
		if v.is_some() {
			self.buf.push(
				ns.to_string(),
				db.to_string(),
				tb.0,
				TableMutation::Set(id, v.into_owned()),
			);
		} else {
			self.buf.push(ns.to_string(), db.to_string(), tb.0, TableMutation::Del(id));
		}
	}

	// get returns all the mutations buffered for this transaction,
	// that are to be written onto the key composed of the specified prefix + the current timestamp + the specified suffix.
	pub(crate) fn get(&self) -> Vec<PreparedWrite> {
		let mut r = Vec::<(Vec<u8>, Vec<u8>, Vec<u8>, crate::kvs::Val)>::new();
		// Get the current timestamp
		for (
			ChangeKey {
				ns,
				db,
				tb,
			},
			mutations,
		) in self.buf.b.iter()
		{
			let ts_key: Key = crate::key::database::vs::new(ns, db).into();
			let tc_key_prefix: Key = crate::key::change::versionstamped_key_prefix(ns, db);
			let tc_key_suffix: Key = crate::key::change::versionstamped_key_suffix(tb.as_str());

			r.push((ts_key, tc_key_prefix, tc_key_suffix, mutations.into()))
		}
		r
	}
}

#[cfg(test)]
mod tests {
	use std::borrow::Cow;
	use std::time::Duration;

	use crate::cf::{ChangeSet, DatabaseMutation, TableMutation, TableMutations};
	use crate::kvs::Datastore;
	use crate::sql::changefeed::ChangeFeed;
	use crate::sql::id::Id;
	use crate::sql::statements::DefineTableStatement;
	use crate::sql::thing::Thing;
	use crate::sql::value::Value;
	use crate::vs;

	#[tokio::test]
	async fn test_changefeed_read_write() {
		let ns = "myns";
		let db = "mydb";
		let tb = super::Ident("mytb".to_string());
		let mut dtb = DefineTableStatement::default();
		dtb.name = tb.clone();
		dtb.changefeed = Some(ChangeFeed {
			expiry: Duration::from_secs(0),
		});

		let ds = Datastore::new("memory").await.unwrap();

		let mut tx1 = ds.transaction(true, false).await.unwrap();
		let thing_a = Thing {
			tb: tb.clone().0,
			id: Id::String("A".to_string()),
		};
		let value_a: super::Value = "a".into();
		tx1.record_change(ns, db, &dtb, &thing_a, Cow::Borrowed(&value_a));
		tx1.complete_changes(true).await.unwrap();
		let _r1 = tx1.commit().await.unwrap();

		let mut tx2 = ds.transaction(true, false).await.unwrap();
		let thing_c = Thing {
			tb: tb.clone().0,
			id: Id::String("C".to_string()),
		};
		let value_c: Value = "c".into();
		tx2.record_change(ns, db, &dtb, &thing_c, Cow::Borrowed(&value_c));
		tx2.complete_changes(true).await.unwrap();
		let _r2 = tx2.commit().await.unwrap();

		let x = ds.transaction(true, false).await;
		let mut tx3 = x.unwrap();
		let thing_b = Thing {
			tb: tb.clone().0,
			id: Id::String("B".to_string()),
		};
		let value_b: Value = "b".into();
		tx3.record_change(ns, db, &dtb, &thing_b, Cow::Borrowed(&value_b));
		let thing_c2 = Thing {
			tb: tb.clone().0,
			id: Id::String("C".to_string()),
		};
		let value_c2: Value = "c2".into();
		tx3.record_change(ns, db, &dtb, &thing_c2, Cow::Borrowed(&value_c2));
		tx3.complete_changes(true).await.unwrap();
		tx3.commit().await.unwrap();

		// Note that we committed tx1, tx2, and tx3 in this order so far.
		// Therfore, the change feeds should give us
		// the mutations in the commit order, which is tx1, tx3, then tx2.

		let start: u64 = 0;

		let mut tx4 = ds.transaction(true, false).await.unwrap();
		let tb = tb.clone();
		let tb = Some(tb.0.as_ref());
		let r = crate::cf::read(&mut tx4, ns, db, tb, Some(start), Some(10)).await.unwrap();
		tx4.commit().await.unwrap();

		let mut want: Vec<ChangeSet> = Vec::new();
		want.push(ChangeSet(
			vs::u64_to_versionstamp(1),
			DatabaseMutation(vec![TableMutations(
				"mytb".to_string(),
				vec![TableMutation::Set(
					Thing::from(("mytb".to_string(), "A".to_string())),
					Value::from("a"),
				)],
			)]),
		));
		want.push(ChangeSet(
			vs::u64_to_versionstamp(2),
			DatabaseMutation(vec![TableMutations(
				"mytb".to_string(),
				vec![TableMutation::Set(
					Thing::from(("mytb".to_string(), "C".to_string())),
					Value::from("c"),
				)],
			)]),
		));
		want.push(ChangeSet(
			vs::u64_to_versionstamp(3),
			DatabaseMutation(vec![TableMutations(
				"mytb".to_string(),
				vec![
					TableMutation::Set(
						Thing::from(("mytb".to_string(), "B".to_string())),
						Value::from("b"),
					),
					TableMutation::Set(
						Thing::from(("mytb".to_string(), "C".to_string())),
						Value::from("c2"),
					),
				],
			)]),
		));

		assert_eq!(r, want);

		let mut tx5 = ds.transaction(true, false).await.unwrap();
		// gc_all needs to be committed before we can read the changes
		crate::cf::gc_db(&mut tx5, ns, db, vs::u64_to_versionstamp(3), Some(10)).await.unwrap();
		// We now commit tx5, which should persist the gc_all resullts
		tx5.commit().await.unwrap();

		// Now we should see the gc_all results
		let mut tx6 = ds.transaction(true, false).await.unwrap();
		let r = crate::cf::read(&mut tx6, ns, db, tb, Some(start), Some(10)).await.unwrap();
		tx6.commit().await.unwrap();

		let mut want: Vec<ChangeSet> = Vec::new();
		want.push(ChangeSet(
			vs::u64_to_versionstamp(3),
			DatabaseMutation(vec![TableMutations(
				"mytb".to_string(),
				vec![
					TableMutation::Set(
						Thing::from(("mytb".to_string(), "B".to_string())),
						Value::from("b"),
					),
					TableMutation::Set(
						Thing::from(("mytb".to_string(), "C".to_string())),
						Value::from("c2"),
					),
				],
			)]),
		));
		assert_eq!(r, want);
	}
}
