use crate::cf::{TableMutation, TableMutations};
use crate::sql::ident::Ident;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use std::borrow::Cow;
use std::collections::HashMap;

// Writer is a helper for writing table mutations within a transaction.
// As same as how the SurrealDB transaction works, each transaction and therefore each Writer
// can contain multiple table mutations across databases within a namespace.
// Crossing namespaces isn't allowed.
// To alleviate the slowness due to how versionstamps are generated in some key-value stores,
// Writer buffer table mutations, add compose written keys containing versionstamps immediately before
// committing the transaction.
pub struct Writer {
	buf: Buffer,
}

pub struct Buffer {
	pub b: HashMap<ChangeKey, TableMutations>,
}

// ChangeKey distinguishes table mutations within a transaction.
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

	pub(crate) fn drain(&mut self) -> Vec<(String, String, String, TableMutations)> {
		let mut changes = Vec::new();
		for (
			ChangeKey {
				ns,
				db,
				tb,
			},
			mutations,
		) in self.buf.b.drain()
		{
			changes.push((ns, db, tb, mutations));
		}
		changes
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
	use crate::sql::statements::show::ShowSince;
	use crate::sql::statements::{
		DefineDatabaseStatement, DefineNamespaceStatement, DefineTableStatement,
	};
	use crate::sql::thing::Thing;
	use crate::sql::value::Value;
	use crate::vs;

	#[tokio::test]
	async fn test_changefeed_read_write() {
		let ts = crate::sql::Datetime::default();
		let ns = "myns";
		let db = "mydb";
		let tb = super::Ident("mytb".to_string());
		let mut dns = DefineNamespaceStatement::default();
		dns.name = super::Ident(ns.to_string());
		dns.id = Some(1);
		let dns2 = dns.clone();
		let mut ddb = DefineDatabaseStatement::default();
		ddb.name = super::Ident(db.to_string());
		ddb.changefeed = Some(ChangeFeed {
			expiry: Duration::from_secs(10),
		});
		ddb.id = Some(2);
		let ddb2 = ddb.clone();
		let mut dtb = DefineTableStatement::default();
		dtb.name = tb.clone();
		dtb.changefeed = Some(ChangeFeed {
			expiry: Duration::from_secs(10),
		});
		dtb.id = Some(3);
		let dtb2 = dtb.clone();

		let ds = Datastore::new("memory").await.unwrap();

		//
		// Create the ns, db, and tb to let the GC and the timestamp-to-versionstamp conversion
		// work.
		//

		let ns_id = dns.clone().id.unwrap();
		let db_id = ddb.clone().id.unwrap();
		let tb_id = dtb.clone().id.unwrap();

		let mut tx0 = ds.transaction(true, false).await.unwrap();
		tx0.put(&crate::key::root::ns::new(ns), dns).await.unwrap();
		tx0.put(&crate::key::namespace::ns::new(ns_id), dns2).await.unwrap();
		tx0.put(&crate::key::namespace::db::new(ns_id, db), ddb).await.unwrap();
		tx0.put(&crate::key::database::db::new(ns_id, db_id), ddb2).await.unwrap();
		let tb = tb.clone();
		tx0.put(&crate::key::database::tb::new(ns_id, db_id, tb.as_ref()), dtb.clone())
			.await
			.unwrap();
		tx0.put(&crate::key::table::tb::new(ns_id, db_id, tb_id), dtb2.clone()).await.unwrap();
		tx0.commit().await.unwrap();

		// Let the db remember the timestamp for the current versionstamp
		// so that we can replay change feeds from the timestamp later.
		ds.tick_at(ts.0.timestamp().try_into().unwrap()).await.unwrap();

		//
		// Write things to the table.
		//

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
		let r = crate::cf::read(
			&mut tx4,
			ns,
			db,
			Some(tb.0.as_ref()),
			ShowSince::Versionstamp(start),
			Some(10),
		)
		.await
		.unwrap();
		tx4.commit().await.unwrap();

		let mut want: Vec<ChangeSet> = Vec::new();
		want.push(ChangeSet(
			vs::u64_to_versionstamp(2),
			DatabaseMutation(vec![TableMutations(
				"mytb".to_string(),
				vec![TableMutation::Set(
					Thing::from(("mytb".to_string(), "A".to_string())),
					Value::from("a"),
				)],
			)]),
		));
		want.push(ChangeSet(
			vs::u64_to_versionstamp(3),
			DatabaseMutation(vec![TableMutations(
				"mytb".to_string(),
				vec![TableMutation::Set(
					Thing::from(("mytb".to_string(), "C".to_string())),
					Value::from("c"),
				)],
			)]),
		));
		want.push(ChangeSet(
			vs::u64_to_versionstamp(4),
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
		crate::cf::gc_db(&mut tx5, ns, db, vs::u64_to_versionstamp(4), Some(10)).await.unwrap();
		// We now commit tx5, which should persist the gc_all resullts
		tx5.commit().await.unwrap();

		// Now we should see the gc_all results
		let mut tx6 = ds.transaction(true, false).await.unwrap();
		let tb = tb.clone();
		let r = crate::cf::read(
			&mut tx6,
			ns,
			db,
			Some(tb.0.as_ref()),
			ShowSince::Versionstamp(start),
			Some(10),
		)
		.await
		.unwrap();
		tx6.commit().await.unwrap();

		let mut want: Vec<ChangeSet> = Vec::new();
		want.push(ChangeSet(
			vs::u64_to_versionstamp(4),
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

		// Now we should see the gc_all results
		ds.tick_at((ts.0.timestamp() + 5).try_into().unwrap()).await.unwrap();

		let mut tx7 = ds.transaction(true, false).await.unwrap();
		let r = crate::cf::read(
			&mut tx7,
			ns,
			db,
			Some(tb.0.as_ref()),
			ShowSince::Timestamp(ts),
			Some(10),
		)
		.await
		.unwrap();
		tx7.commit().await.unwrap();
		assert_eq!(r, want);
	}
}
