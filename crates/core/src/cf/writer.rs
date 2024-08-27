use std::collections::HashMap;

use crate::cf::{TableMutation, TableMutations};
use crate::doc::CursorValue;
use crate::kvs::Key;
use crate::sql::statements::DefineTableStatement;
use crate::sql::thing::Thing;
use crate::sql::Idiom;

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
	pub(crate) fn record_cf_change(
		&mut self,
		ns: &str,
		db: &str,
		tb: &str,
		id: Thing,
		previous: CursorValue,
		current: CursorValue,
		store_difference: bool,
	) {
		if current.as_ref().is_some() {
			self.buf.push(
				ns.to_string(),
				db.to_string(),
				tb.to_string(),
				match store_difference {
					true => {
						if previous.as_ref().is_none() {
							TableMutation::Set(id, current.into_owned())
						} else {
							// We intentionally record the patches in reverse (current -> previous)
							// because we cannot otherwise resolve operations such as "replace" and "remove".
							let patches_to_create_previous =
								current.diff(&previous, Idiom::default());
							TableMutation::SetWithDiff(
								id,
								current.into_owned(),
								patches_to_create_previous,
							)
						}
					}
					false => TableMutation::Set(id, current.into_owned()),
				},
			);
		} else {
			self.buf.push(
				ns.to_string(),
				db.to_string(),
				tb.to_string(),
				match store_difference {
					true => TableMutation::DelWithOriginal(id, previous.into_owned()),
					false => TableMutation::Del(id),
				},
			);
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
	use std::time::Duration;

	use crate::cf::{ChangeSet, DatabaseMutation, TableMutation, TableMutations};
	use crate::dbs::Session;
	use crate::fflags::FFLAGS;
	use crate::kvs::{Datastore, LockType::*, Transaction, TransactionType::*};
	use crate::sql::changefeed::ChangeFeed;
	use crate::sql::id::Id;
	use crate::sql::statements::show::ShowSince;
	use crate::sql::statements::{
		DefineDatabaseStatement, DefineNamespaceStatement, DefineTableStatement,
	};
	use crate::sql::thing::Thing;
	use crate::sql::value::Value;
	use crate::sql::{Datetime, Idiom, Number, Object, Operation, Strand};
	use crate::vs;
	use crate::vs::{conv, Versionstamp};

	const DONT_STORE_PREVIOUS: bool = false;

	const NS: &str = "myns";
	const DB: &str = "mydb";
	const TB: &str = "mytb";

	#[tokio::test]
	async fn changefeed_read_write() {
		let ts = Datetime::default();
		let ds = init(false).await;

		// Let the db remember the timestamp for the current versionstamp
		// so that we can replay change feeds from the timestamp later.
		ds.tick_at(ts.0.timestamp().try_into().unwrap()).await.unwrap();

		//
		// Write things to the table.
		//

		let mut tx1 = ds.transaction(Write, Optimistic).await.unwrap().inner();
		let thing_a = Thing {
			tb: TB.to_owned(),
			id: Id::from("A"),
		};
		let value_a: Value = "a".into();
		let previous = Value::None;
		tx1.record_change(
			NS,
			DB,
			TB,
			&thing_a,
			previous.clone().into(),
			value_a.into(),
			DONT_STORE_PREVIOUS,
		);
		tx1.complete_changes(true).await.unwrap();
		tx1.commit().await.unwrap();

		let mut tx2 = ds.transaction(Write, Optimistic).await.unwrap().inner();
		let thing_c = Thing {
			tb: TB.to_owned(),
			id: Id::from("C"),
		};
		let value_c: Value = "c".into();
		tx2.record_change(
			NS,
			DB,
			TB,
			&thing_c,
			previous.clone().into(),
			value_c.into(),
			DONT_STORE_PREVIOUS,
		);
		tx2.complete_changes(true).await.unwrap();
		tx2.commit().await.unwrap();

		let mut tx3 = ds.transaction(Write, Optimistic).await.unwrap().inner();
		let thing_b = Thing {
			tb: TB.to_owned(),
			id: Id::from("B"),
		};
		let value_b: Value = "b".into();
		tx3.record_change(
			NS,
			DB,
			TB,
			&thing_b,
			previous.clone().into(),
			value_b.into(),
			DONT_STORE_PREVIOUS,
		);
		let thing_c2 = Thing {
			tb: TB.to_owned(),
			id: Id::from("C"),
		};
		let value_c2: Value = "c2".into();
		tx3.record_change(
			NS,
			DB,
			TB,
			&thing_c2,
			previous.clone().into(),
			value_c2.into(),
			DONT_STORE_PREVIOUS,
		);
		tx3.complete_changes(true).await.unwrap();
		tx3.commit().await.unwrap();

		// Note that we committed tx1, tx2, and tx3 in this order so far.
		// Therefore, the change feeds should give us
		// the mutations in the commit order, which is tx1, tx3, then tx2.

		let start: u64 = 0;

		let tx4 = ds.transaction(Write, Optimistic).await.unwrap();
		let r = crate::cf::read(&tx4, NS, DB, Some(TB), ShowSince::Versionstamp(start), Some(10))
			.await
			.unwrap();
		tx4.commit().await.unwrap();

		let want: Vec<ChangeSet> = vec![
			ChangeSet(
				vs::u64_to_versionstamp(2),
				DatabaseMutation(vec![TableMutations(
					TB.to_string(),
					match FFLAGS.change_feed_live_queries.enabled() {
						true => vec![TableMutation::SetWithDiff(
							Thing::from((TB.to_string(), "A".to_string())),
							Value::None,
							vec![],
						)],
						false => vec![TableMutation::Set(
							Thing::from((TB.to_string(), "A".to_string())),
							Value::from("a"),
						)],
					},
				)]),
			),
			ChangeSet(
				vs::u64_to_versionstamp(3),
				DatabaseMutation(vec![TableMutations(
					TB.to_string(),
					match FFLAGS.change_feed_live_queries.enabled() {
						true => vec![TableMutation::SetWithDiff(
							Thing::from((TB.to_string(), "C".to_string())),
							Value::None,
							vec![],
						)],
						false => vec![TableMutation::Set(
							Thing::from((TB.to_string(), "C".to_string())),
							Value::from("c"),
						)],
					},
				)]),
			),
			ChangeSet(
				vs::u64_to_versionstamp(4),
				DatabaseMutation(vec![TableMutations(
					TB.to_string(),
					match FFLAGS.change_feed_live_queries.enabled() {
						true => vec![
							TableMutation::SetWithDiff(
								Thing::from((TB.to_string(), "B".to_string())),
								Value::None,
								vec![],
							),
							TableMutation::SetWithDiff(
								Thing::from((TB.to_string(), "C".to_string())),
								Value::None,
								vec![],
							),
						],
						false => vec![
							TableMutation::Set(
								Thing::from((TB.to_string(), "B".to_string())),
								Value::from("b"),
							),
							TableMutation::Set(
								Thing::from((TB.to_string(), "C".to_string())),
								Value::from("c2"),
							),
						],
					},
				)]),
			),
		];

		assert_eq!(r, want);

		let tx5 = ds.transaction(Write, Optimistic).await.unwrap();
		// gc_all needs to be committed before we can read the changes
		crate::cf::gc_range(&tx5, NS, DB, vs::u64_to_versionstamp(4)).await.unwrap();
		// We now commit tx5, which should persist the gc_all resullts
		tx5.commit().await.unwrap();

		// Now we should see the gc_all results
		let tx6 = ds.transaction(Write, Optimistic).await.unwrap();
		let r = crate::cf::read(&tx6, NS, DB, Some(TB), ShowSince::Versionstamp(start), Some(10))
			.await
			.unwrap();
		tx6.commit().await.unwrap();

		let want: Vec<ChangeSet> = vec![ChangeSet(
			vs::u64_to_versionstamp(4),
			DatabaseMutation(vec![TableMutations(
				TB.to_string(),
				match FFLAGS.change_feed_live_queries.enabled() {
					true => vec![
						TableMutation::SetWithDiff(
							Thing::from((TB.to_string(), "B".to_string())),
							Value::None,
							vec![],
						),
						TableMutation::SetWithDiff(
							Thing::from((TB.to_string(), "C".to_string())),
							Value::None,
							vec![],
						),
					],
					false => vec![
						TableMutation::Set(
							Thing::from((TB.to_string(), "B".to_string())),
							Value::from("b"),
						),
						TableMutation::Set(
							Thing::from((TB.to_string(), "C".to_string())),
							Value::from("c2"),
						),
					],
				},
			)]),
		)];
		assert_eq!(r, want);

		// Now we should see the gc_all results
		ds.tick_at((ts.0.timestamp() + 5).try_into().unwrap()).await.unwrap();

		let tx7 = ds.transaction(Write, Optimistic).await.unwrap();
		let r = crate::cf::read(&tx7, NS, DB, Some(TB), ShowSince::Timestamp(ts), Some(10))
			.await
			.unwrap();
		tx7.commit().await.unwrap();
		assert_eq!(r, want);
	}

	#[test_log::test(tokio::test)]
	async fn scan_picks_up_from_offset() {
		// Given we have 2 entries in change feeds
		let ds = init(false).await;
		ds.tick_at(5).await.unwrap();
		let _id1 = record_change_feed_entry(
			ds.transaction(Write, Optimistic).await.unwrap(),
			"First".to_string(),
		)
		.await;
		ds.tick_at(10).await.unwrap();
		let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
		let vs1 = tx.get_versionstamp_from_timestamp(5, NS, DB, false).await.unwrap().unwrap();
		let vs2 = tx.get_versionstamp_from_timestamp(10, NS, DB, false).await.unwrap().unwrap();
		tx.cancel().await.unwrap();
		let _id2 = record_change_feed_entry(
			ds.transaction(Write, Optimistic).await.unwrap(),
			"Second".to_string(),
		)
		.await;

		// When we scan from the versionstamp between the changes
		let r = change_feed_vs(ds.transaction(Write, Optimistic).await.unwrap(), &vs2).await;

		// Then there is only 1 change
		assert_eq!(r.len(), 1);
		assert!(r[0].0 >= vs2, "{:?}", r);

		// And scanning with previous offset includes both values (without table definitions)
		let r = change_feed_vs(ds.transaction(Write, Optimistic).await.unwrap(), &vs1).await;
		assert_eq!(r.len(), 2);
	}

	#[test_log::test(tokio::test)]
	async fn set_with_diff_records_diff_to_achieve_original() {
		if !FFLAGS.change_feed_live_queries.enabled() {
			return;
		}
		let ts = Datetime::default();
		let ds = init(true).await;

		// Create a doc
		ds.tick_at(ts.0.timestamp().try_into().unwrap()).await.unwrap();
		let thing = Thing {
			tb: TB.to_owned(),
			id: Id::from("A"),
		};
		let ses = Session::owner().with_ns(NS).with_db(DB);
		let res =
			ds.execute(format!("CREATE {thing} SET value=50").as_str(), &ses, None).await.unwrap();
		assert_eq!(res.len(), 1, "{:?}", res);
		let res = res.into_iter().next().unwrap();
		res.result.unwrap();

		// Now update it
		ds.tick_at((ts.0.timestamp() + 10).try_into().unwrap()).await.unwrap();
		let res = ds
			.execute(
				format!("UPDATE {thing} SET value=100, new_field=\"new_value\"").as_str(),
				&ses,
				None,
			)
			.await
			.unwrap();
		assert_eq!(res.len(), 1, "{:?}", res);
		let res = res.into_iter().next().unwrap();
		res.result.unwrap();

		// Now read the change feed
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		let r = change_feed_ts(tx, &ts).await;
		let expected_obj_first = Value::Object(Object::from(map! {
			"id".to_string() => Value::Thing(thing.clone()),
			"value".to_string() => Value::Number(Number::Int(50)),
		}));
		let expected_obj_second = Value::Object(Object::from(map! {
			"id".to_string() => Value::Thing(thing.clone()),
			"value".to_string() => Value::Number(Number::Int(100)),
			"new_field".to_string() => Value::Strand(Strand::from("new_value")),
		}));
		assert_eq!(r.len(), 2, "{:?}", r);
		let expected: Vec<ChangeSet> = vec![
			ChangeSet(
				vs::u64_to_versionstamp(2),
				DatabaseMutation(vec![TableMutations(
					TB.to_string(),
					vec![TableMutation::Set(
						Thing::from((TB.to_string(), "A".to_string())),
						expected_obj_first,
					)],
				)]),
			),
			ChangeSet(
				vs::u64_to_versionstamp(4),
				DatabaseMutation(vec![TableMutations(
					TB.to_string(),
					vec![TableMutation::SetWithDiff(
						Thing::from((TB.to_string(), "A".to_string())),
						expected_obj_second,
						vec![
							// We need to remove the field to achieve the previous value
							Operation::Remove {
								path: Idiom::from("new_field"),
							},
							Operation::Replace {
								path: Idiom::from("value"),
								value: Value::Number(Number::Int(50)),
							},
						],
					)],
				)]),
			),
		];
		assert_eq!(r, expected);
	}

	async fn change_feed_ts(tx: Transaction, ts: &Datetime) -> Vec<ChangeSet> {
		let r = crate::cf::read(&tx, NS, DB, Some(TB), ShowSince::Timestamp(ts.clone()), Some(10))
			.await
			.unwrap();
		tx.cancel().await.unwrap();
		r
	}

	async fn change_feed_vs(tx: Transaction, vs: &Versionstamp) -> Vec<ChangeSet> {
		let r = crate::cf::read(
			&tx,
			NS,
			DB,
			Some(TB),
			ShowSince::Versionstamp(conv::versionstamp_to_u64(vs)),
			Some(10),
		)
		.await
		.unwrap();
		tx.cancel().await.unwrap();
		r
	}

	async fn record_change_feed_entry(tx: Transaction, id: String) -> Thing {
		let thing = Thing {
			tb: TB.to_owned(),
			id: Id::from(id),
		};
		let value_a: Value = "a".into();
		let previous = Value::None.into();
		tx.lock().await.record_change(
			NS,
			DB,
			TB,
			&thing,
			previous,
			value_a.into(),
			DONT_STORE_PREVIOUS,
		);
		tx.lock().await.complete_changes(true).await.unwrap();
		tx.commit().await.unwrap();
		thing
	}

	async fn init(store_diff: bool) -> Datastore {
		let dns = DefineNamespaceStatement {
			name: crate::sql::Ident(NS.to_string()),
			..Default::default()
		};
		let ddb = DefineDatabaseStatement {
			name: crate::sql::Ident(DB.to_string()),
			changefeed: Some(ChangeFeed {
				expiry: Duration::from_secs(10),
				store_diff,
			}),
			..Default::default()
		};
		let dtb = DefineTableStatement {
			name: TB.into(),
			changefeed: Some(ChangeFeed {
				expiry: Duration::from_secs(10 * 60),
				store_diff,
			}),
			..Default::default()
		};

		let ds = Datastore::new("memory").await.unwrap();

		//
		// Create the ns, db, and tb to let the GC and the timestamp-to-versionstamp conversion
		// work.
		//

		let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
		let ns_root = crate::key::root::ns::new(NS);
		tx.put(&ns_root, dns, None).await.unwrap();
		let db_root = crate::key::namespace::db::new(NS, DB);
		tx.put(&db_root, ddb, None).await.unwrap();
		let tb_root = crate::key::database::tb::new(NS, DB, TB);
		tx.put(&tb_root, dtb.clone(), None).await.unwrap();
		tx.commit().await.unwrap();
		ds
	}
}
