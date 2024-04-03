use crate::cf::{TableMutation, TableMutations};
use crate::kvs::Key;
use crate::sql::statements::DefineTableStatement;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use crate::sql::Idiom;
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

#[non_exhaustive]
pub struct Writer {
	buf: Buffer,
}

#[non_exhaustive]
pub struct Buffer {
	pub b: HashMap<ChangeKey, TableMutations>,
}

#[derive(Hash, Eq, PartialEq, Debug)]
#[non_exhaustive]
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

	#[allow(clippy::too_many_arguments)]
	pub(crate) fn update(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
		id: Thing,
		previous: Cow<'_, Value>,
		current: Cow<'_, Value>,
		store_difference: bool,
	) {
		if current.is_some() {
			self.buf.push(
				ns.to_string(),
				db.to_string(),
				tb.to_string(),
				match store_difference {
					true => {
						let patches = current.diff(&previous, Idiom(Vec::new()));
						let new_record = !previous.is_some();
						trace!("The record is new_record={new_record} because previous is {previous:?}");
						if previous.is_none() {
							TableMutation::Set(id, current.into_owned())
						} else {
							TableMutation::SetWithDiff(id, current.into_owned(), patches)
						}
					}
					false => TableMutation::Set(id, current.into_owned()),
				},
			);
		} else {
			self.buf.push(ns.to_string(), db.to_string(), tb.to_string(), TableMutation::Del(id));
		}
	}

	pub(crate) fn define_table(&mut self, ns: &str, db: &str, tb: &str, dt: &DefineTableStatement) {
		self.buf.push(
			ns.to_string(),
			db.to_string(),
			tb.to_string(),
			TableMutation::Def(dt.to_owned()),
		)
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
	use crate::fflags::FFLAGS;
	use crate::key::key_req::KeyRequirements;
	use crate::kvs::{Datastore, LockType::*, TransactionType::*};
	use crate::sql::changefeed::ChangeFeed;
	use crate::sql::id::Id;
	use crate::sql::statements::show::ShowSince;
	use crate::sql::statements::{
		DefineDatabaseStatement, DefineNamespaceStatement, DefineTableStatement,
	};
	use crate::sql::thing::Thing;
	use crate::sql::value::Value;
	use crate::vs;

	const DONT_STORE_PREVIOUS: bool = false;

	#[tokio::test]
	async fn test_changefeed_read_write() {
		let ts = crate::sql::Datetime::default();
		let ns = "myns";
		let db = "mydb";
		let tb = "mytb";
		let dns = DefineNamespaceStatement {
			name: crate::sql::Ident(ns.to_string()),
			..Default::default()
		};
		let ddb = DefineDatabaseStatement {
			name: crate::sql::Ident(db.to_string()),
			changefeed: Some(ChangeFeed {
				expiry: Duration::from_secs(10),
				store_original: false,
			}),
			..Default::default()
		};
		let dtb = DefineTableStatement {
			name: tb.into(),
			changefeed: Some(ChangeFeed {
				expiry: Duration::from_secs(10),
				store_original: false,
			}),
			..Default::default()
		};

		let ds = Datastore::new("memory").await.unwrap();

		//
		// Create the ns, db, and tb to let the GC and the timestamp-to-versionstamp conversion
		// work.
		//

		let mut tx0 = ds.transaction(Write, Optimistic).await.unwrap();
		let ns_root = crate::key::root::ns::new(ns);
		tx0.put(ns_root.key_category(), &ns_root, dns).await.unwrap();
		let db_root = crate::key::namespace::db::new(ns, db);
		tx0.put(db_root.key_category(), &db_root, ddb).await.unwrap();
		let tb_root = crate::key::database::tb::new(ns, db, tb);
		tx0.put(tb_root.key_category(), &tb_root, dtb.clone()).await.unwrap();
		tx0.commit().await.unwrap();

		// Let the db remember the timestamp for the current versionstamp
		// so that we can replay change feeds from the timestamp later.
		ds.tick_at(ts.0.timestamp().try_into().unwrap()).await.unwrap();

		//
		// Write things to the table.
		//

		let mut tx1 = ds.transaction(Write, Optimistic).await.unwrap();
		let thing_a = Thing {
			tb: tb.to_owned(),
			id: Id::String("A".to_string()),
		};
		let value_a: super::Value = "a".into();
		let previous = Cow::from(Value::None);
		tx1.record_change(
			ns,
			db,
			tb,
			&thing_a,
			previous.clone(),
			Cow::Borrowed(&value_a),
			DONT_STORE_PREVIOUS,
		);
		tx1.complete_changes(true).await.unwrap();
		tx1.commit().await.unwrap();

		let mut tx2 = ds.transaction(Write, Optimistic).await.unwrap();
		let thing_c = Thing {
			tb: tb.to_owned(),
			id: Id::String("C".to_string()),
		};
		let value_c: Value = "c".into();
		tx2.record_change(
			ns,
			db,
			tb,
			&thing_c,
			previous.clone(),
			Cow::Borrowed(&value_c),
			DONT_STORE_PREVIOUS,
		);
		tx2.complete_changes(true).await.unwrap();
		tx2.commit().await.unwrap();

		let x = ds.transaction(Write, Optimistic).await;
		let mut tx3 = x.unwrap();
		let thing_b = Thing {
			tb: tb.to_owned(),
			id: Id::String("B".to_string()),
		};
		let value_b: Value = "b".into();
		tx3.record_change(
			ns,
			db,
			tb,
			&thing_b,
			previous.clone(),
			Cow::Borrowed(&value_b),
			DONT_STORE_PREVIOUS,
		);
		let thing_c2 = Thing {
			tb: tb.to_owned(),
			id: Id::String("C".to_string()),
		};
		let value_c2: Value = "c2".into();
		tx3.record_change(
			ns,
			db,
			tb,
			&thing_c2,
			previous.clone(),
			Cow::Borrowed(&value_c2),
			DONT_STORE_PREVIOUS,
		);
		tx3.complete_changes(true).await.unwrap();
		tx3.commit().await.unwrap();

		// Note that we committed tx1, tx2, and tx3 in this order so far.
		// Therefore, the change feeds should give us
		// the mutations in the commit order, which is tx1, tx3, then tx2.

		let start: u64 = 0;

		let mut tx4 = ds.transaction(Write, Optimistic).await.unwrap();
		let r =
			crate::cf::read(&mut tx4, ns, db, Some(tb), ShowSince::Versionstamp(start), Some(10))
				.await
				.unwrap();
		tx4.commit().await.unwrap();

		let want: Vec<ChangeSet> = vec![
			ChangeSet(
				vs::u64_to_versionstamp(2),
				DatabaseMutation(vec![TableMutations(
					"mytb".to_string(),
					match FFLAGS.change_feed_live_queries.enabled() {
						true => vec![TableMutation::SetWithDiff(
							Thing::from(("mytb".to_string(), "A".to_string())),
							Value::None,
							vec![],
						)],
						false => vec![TableMutation::Set(
							Thing::from(("mytb".to_string(), "A".to_string())),
							Value::from("a"),
						)],
					},
				)]),
			),
			ChangeSet(
				vs::u64_to_versionstamp(3),
				DatabaseMutation(vec![TableMutations(
					"mytb".to_string(),
					match FFLAGS.change_feed_live_queries.enabled() {
						true => vec![TableMutation::SetWithDiff(
							Thing::from(("mytb".to_string(), "C".to_string())),
							Value::None,
							vec![],
						)],
						false => vec![TableMutation::Set(
							Thing::from(("mytb".to_string(), "C".to_string())),
							Value::from("c"),
						)],
					},
				)]),
			),
			ChangeSet(
				vs::u64_to_versionstamp(4),
				DatabaseMutation(vec![TableMutations(
					"mytb".to_string(),
					match FFLAGS.change_feed_live_queries.enabled() {
						true => vec![
							TableMutation::SetWithDiff(
								Thing::from(("mytb".to_string(), "B".to_string())),
								Value::None,
								vec![],
							),
							TableMutation::SetWithDiff(
								Thing::from(("mytb".to_string(), "C".to_string())),
								Value::None,
								vec![],
							),
						],
						false => vec![
							TableMutation::Set(
								Thing::from(("mytb".to_string(), "B".to_string())),
								Value::from("b"),
							),
							TableMutation::Set(
								Thing::from(("mytb".to_string(), "C".to_string())),
								Value::from("c2"),
							),
						],
					},
				)]),
			),
		];

		assert_eq!(r, want);

		let mut tx5 = ds.transaction(Write, Optimistic).await.unwrap();
		// gc_all needs to be committed before we can read the changes
		crate::cf::gc_db(&mut tx5, ns, db, vs::u64_to_versionstamp(4), Some(10)).await.unwrap();
		// We now commit tx5, which should persist the gc_all resullts
		tx5.commit().await.unwrap();

		// Now we should see the gc_all results
		let mut tx6 = ds.transaction(Write, Optimistic).await.unwrap();
		let r =
			crate::cf::read(&mut tx6, ns, db, Some(tb), ShowSince::Versionstamp(start), Some(10))
				.await
				.unwrap();
		tx6.commit().await.unwrap();

		let want: Vec<ChangeSet> = vec![ChangeSet(
			vs::u64_to_versionstamp(4),
			DatabaseMutation(vec![TableMutations(
				"mytb".to_string(),
				match FFLAGS.change_feed_live_queries.enabled() {
					true => vec![
						TableMutation::SetWithDiff(
							Thing::from(("mytb".to_string(), "B".to_string())),
							Value::None,
							vec![],
						),
						TableMutation::SetWithDiff(
							Thing::from(("mytb".to_string(), "C".to_string())),
							Value::None,
							vec![],
						),
					],
					false => vec![
						TableMutation::Set(
							Thing::from(("mytb".to_string(), "B".to_string())),
							Value::from("b"),
						),
						TableMutation::Set(
							Thing::from(("mytb".to_string(), "C".to_string())),
							Value::from("c2"),
						),
					],
				},
			)]),
		)];
		assert_eq!(r, want);

		// Now we should see the gc_all results
		ds.tick_at((ts.0.timestamp() + 5).try_into().unwrap()).await.unwrap();

		let mut tx7 = ds.transaction(Write, Optimistic).await.unwrap();
		let r = crate::cf::read(&mut tx7, ns, db, Some(tb), ShowSince::Timestamp(ts), Some(10))
			.await
			.unwrap();
		tx7.commit().await.unwrap();
		assert_eq!(r, want);
	}
}
