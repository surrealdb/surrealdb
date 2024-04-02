mod check_construct {
	use crate::cf::TableMutation;
	use crate::kvs::ds;
	use crate::sql::statements::DefineTableStatement;
	use crate::sql::{Strand, Thing, Value};

	#[test]
	fn test_construct_document_create() {
		let thing = Thing::from(("table", "id"));
		let value = Value::Strand(Strand::from("value"));
		let tb_mutation = TableMutation::Set(thing.clone(), value);
		let doc = ds::construct_document(&tb_mutation);
		let doc = doc.unwrap();
		assert!(doc.is_new());
		assert!(doc.initial_doc().is_none());
		assert!(doc.current_doc().is_some());
	}

	#[test]
	fn test_construct_document_update() {
		let thing = Thing::from(("table", "id"));
		let value = Value::Strand(Strand::from("value"));
		let operations = vec![];
		let tb_mutation = TableMutation::SetWithDiff(thing.clone(), value, operations);
		let doc = ds::construct_document(&tb_mutation);
		let doc = doc.unwrap();
		assert!(!doc.is_new());
		assert!(doc.initial_doc().is_strand());
		assert!(doc.current_doc().is_strand());
	}

	#[test]
	fn test_construct_document_delete() {
		let thing = Thing::from(("table", "id"));
		let tb_mutation = TableMutation::Del(thing.clone());
		let doc = ds::construct_document(&tb_mutation);
		let doc = doc.unwrap();
		// The previous and current doc values are "None", so technically this is a new doc as per
		// current==None
		assert!(doc.is_new(), "{:?}", doc);
		assert!(doc.current_doc().is_none());
		assert!(doc.initial_doc().is_none());
	}

	#[test]
	fn test_construct_document_none_for_schema() {
		let tb_mutation = TableMutation::Def(DefineTableStatement::default());
		let doc = ds::construct_document(&tb_mutation);
		assert!(doc.is_none());
	}
}

#[cfg(feature = "kv-mem")]
mod check_send {
	use crate::cf::TableMutation;
	use crate::ctx::Context;
	use crate::dbs::{Notification, Options, Statement};
	use crate::fflags::FFLAGS;
	use crate::iam::Auth;
	use crate::kvs::{ds, Datastore, LockType, TransactionType};
	use crate::sql;
	use crate::sql::paths::{OBJ_PATH_AUTH, OBJ_PATH_SCOPE, OBJ_PATH_TOKEN};
	use crate::sql::statements::LiveStatement;
	use crate::sql::{parse, Object, Strand, Thing, Value};
	use channel::Sender;
	use futures::executor::block_on;
	use once_cell::sync::Lazy;
	use std::collections::BTreeMap;
	use std::sync::Arc;
	use tokio::sync::RwLock;

	const SETUP: Lazy<Arc<TestSuite>> = Lazy::new(|| Arc::new(block_on(init_test_suite())));

	async fn init_test_suite() -> TestSuite {
		let ds = Datastore::new("memory").await.unwrap();
		let ns = "the_namespace";
		let db = "the_database";
		let tb = "the_table";
		let sc = "the_scope";
		let tk = "the_token";

		// First we define a token
		ds.execute(format!("
USE NAMESPACE {ns};
USE DATABASE {db};
DEFINE SCOPE {sc};
		DEFINE TABLE "))

		let mut tx = ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap();
		let de = tx.get_sc(&ns, &db, &sc).await?;
		TestSuite {
			ds,
			ns: ns.to_string(),
			db: db.to_string(),
			tb: tb.to_string(),
			sc: sc.to_string(),
			rid: Value::Thing(Thing::from(("user", "test"))),
		}
	}

	struct TestSuite {
		ds: Datastore,
		ns: String,
		db: String,
		tb: String,
		sc: String,
		rid: Value,
	}

	#[test_log::test(tokio::test)]
	async fn test_send_notification() {
		if !FFLAGS.change_feed_live_queries.enabled_test {
			return;
		}
		let thing = Thing::from(("table", "id"));
		let value = Value::Strand(Strand::from("value"));
		let tb_mutation = TableMutation::Set(thing.clone(), value);
		let doc = ds::construct_document(&tb_mutation).unwrap();
		let (sender, receiver) = channel::unbounded();
		let (ctx, opt, stm) = ctx_opt_stm(&sender);
		let tx = SETUP
			.ds
			.transaction(TransactionType::Write, LockType::Optimistic)
			.await
			.unwrap()
			.enclose();

		doc.check_lqs_and_send_notifications(&opt, &Statement::Live(&stm), &tx, &[&stm], &sender)
			.await
			.unwrap();

		let _notification = receiver.try_recv().expect("There should be a notification");
		assert!(receiver.try_recv().is_err());
	}

	fn ctx_opt_stm(sender: &Sender<Notification>) -> (Context, Options, LiveStatement) {
		let mut ctx = Context::default();
		ctx.add_notifications(Some(sender));
		let opt =
			Options::default().with_ns(Some("namespace".into())).with_db(Some("database".into()));
		let query = parse("LIVE SELECT * FROM table").unwrap().0;
		assert_eq!(query.len(), 1);
		let stm = query.0.into_iter().next().unwrap();
		let mut live_stm = match stm {
			sql::Statement::Live(live_stm) => live_stm,
			_ => panic!("Expected live statement"),
		};
		let mut session: BTreeMap<String, Value> = BTreeMap::new();
		session.insert(OBJ_PATH_AUTH.to_string(), Value::Strand(Strand::from("auth")));
		session.insert(OBJ_PATH_SCOPE.to_string(), Value::Strand(Strand::from("scope")));
		session.insert(OBJ_PATH_TOKEN.to_string(), Value::Strand(Strand::from("token")));
		let session = Value::Object(Object::from(session));
		live_stm.session = Some(session);
		live_stm.auth = Some(Auth::for_sc(SETUP.rid.to_string(), "namespace", "database", "scope"));
		(ctx, opt, live_stm)
	}
}
