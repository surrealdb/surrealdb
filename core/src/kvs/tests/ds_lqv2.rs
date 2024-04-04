mod test_construct_document {
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
mod test_check_lqs_and_send_notifications {
	use crate::cf::TableMutation;
	use crate::ctx::Context;
	use crate::dbs::{Notification, Options, Session, Statement};
	use crate::fflags::FFLAGS;
	use crate::iam::{Auth, Role};
	use crate::kvs::droppy_boy::DroppyBoy;
	use crate::kvs::{ds, Datastore, LockType, TransactionType};
	use crate::sql::paths::{OBJ_PATH_AUTH, OBJ_PATH_SCOPE, OBJ_PATH_TOKEN};
	use crate::sql::statements::{CreateStatement, DeleteStatement, LiveStatement};
	use crate::sql::{parse, Fields, Object, Strand, Table, Thing, Value, Values};
	use channel::Sender;
	use futures::executor::block_on;
	use once_cell::sync::Lazy;
	use std::collections::BTreeMap;
	use std::sync::Arc;

	const SETUP: Lazy<Arc<TestSuite>> = Lazy::new(|| Arc::new(block_on(setup_test_suite_init())));

	struct TestSuite {
		ds: Datastore,
		ns: String,
		db: String,
		tb: String,
		rid: Value,
	}

	async fn setup_test_suite_init() -> TestSuite {
		let ds = Datastore::new("memory").await.unwrap();
		let ns = "the_namespace";
		let db = "the_database";
		let tb = "the_table";

		// First we define levels of permissions and schemas and required CF
		let vars = Some(BTreeMap::new());
		ds.execute(
			&format!(
				"
				USE NAMESPACE {ns};
				USE DATABASE {db};
				DEFINE TABLE {tb} CHANGEFEED 1m INCLUDE ORIGINAL PERMISSIONS FULL;
				"
			),
			&Session::owner(),
			vars,
		)
		.await
		.unwrap()
		.into_iter()
		.map(|r| r.result.unwrap())
		.for_each(drop);

		let tx =
			ds.transaction(TransactionType::Write, LockType::Optimistic).await.unwrap().enclose();
		let drop_tx = tx.clone();
		let _foo = DroppyBoy::new(async move {
			drop_tx.lock().await.commit().await.unwrap();
		});
		TestSuite {
			ds,
			ns: ns.to_string(),
			db: db.to_string(),
			tb: tb.to_string(),
			rid: Value::Thing(Thing::from(("user", "test"))),
		}
	}

	#[test_log::test(tokio::test)]
	async fn test_create() {
		if !FFLAGS.change_feed_live_queries.enabled_test {
			return;
		}

		// Setup channels used for listening to LQs
		let (sender, receiver) = channel::unbounded();
		let opt = a_usable_options(&sender);
		let tx = SETUP
			.ds
			.transaction(TransactionType::Write, LockType::Optimistic)
			.await
			.unwrap()
			.enclose();
		let drop_tx = tx.clone();
		let _a = DroppyBoy::new(async move {
			drop_tx.lock().await.commit().await.unwrap();
		});

		// Construct document we are validating
		let record_id = Thing::from((SETUP.tb.as_str(), "id"));
		let value = Value::Strand(Strand::from("value"));
		let tb_mutation = TableMutation::Set(record_id.clone(), value);
		let doc = ds::construct_document(&tb_mutation).unwrap();

		// Perform "live query" on the constructed doc that we are checking
		let live_statement = a_live_query_statement();
		let executed_statement = a_create_statement();
		doc.check_lqs_and_send_notifications(
			&opt,
			&Statement::Create(&executed_statement),
			&tx,
			&[&live_statement],
			&sender,
		)
		.await
		.unwrap();

		// Asserts
		let _notification = receiver.try_recv().expect("There should be a notification");
		assert!(receiver.try_recv().is_err());
	}

	#[test_log::test(tokio::test)]
	async fn test_delete() {
		if !FFLAGS.change_feed_live_queries.enabled_test {
			return;
		}

		// Setup channels used for listening to LQs
		let (sender, receiver) = channel::unbounded();
		let opt = a_usable_options(&sender);
		let tx = SETUP
			.ds
			.transaction(TransactionType::Write, LockType::Optimistic)
			.await
			.unwrap()
			.enclose();
		let drop_tx = tx.clone();
		let _a = DroppyBoy::new(async move {
			drop_tx.lock().await.commit().await.unwrap();
		});

		// Construct document we are validating
		let record_id = Thing::from((SETUP.tb.as_str(), "id"));
		let value = Value::Strand(Strand::from("value"));
		let tb_mutation = TableMutation::Set(record_id.clone(), value);
		let doc = ds::construct_document(&tb_mutation).unwrap();

		// Perform "live query" on the constructed doc that we are checking
		let live_statement = a_live_query_statement();
		let executed_statement = a_delete_statement();
		doc.check_lqs_and_send_notifications(
			&opt,
			&Statement::Delete(&executed_statement),
			&tx,
			&[&live_statement],
			&sender,
		)
		.await
		.unwrap();

		// Asserts
		let _notification = receiver.try_recv().expect("There should be a notification");
		assert!(receiver.try_recv().is_err());
	}

	// Live queries will have authentication info associated with them
	// This is a way to fake that
	fn a_live_query_statement() -> LiveStatement {
		let mut stm = LiveStatement::new(Fields::all());
		let mut session: BTreeMap<String, Value> = BTreeMap::new();
		session.insert(OBJ_PATH_AUTH.to_string(), Value::Strand(Strand::from("auth")));
		session.insert(OBJ_PATH_SCOPE.to_string(), Value::Strand(Strand::from("scope")));
		session.insert(OBJ_PATH_TOKEN.to_string(), Value::Strand(Strand::from("token")));
		let session = Value::Object(Object::from(session));
		stm.session = Some(session);
		stm.auth = Some(Auth::for_db(Role::Owner, "namespace", "database"));
		stm
	}

	// Fake a create statement that does not involve parsing the query
	fn a_create_statement() -> CreateStatement {
		CreateStatement {
			only: false,
			what: Values(vec![Value::Table(Table::from(SETUP.tb.clone()))]),
			data: None,
			output: None,
			timeout: None,
			parallel: false,
		}
	}

	fn a_delete_statement() -> DeleteStatement {
		DeleteStatement {
			only: false,
			what: Values(vec![Value::Table(Table::from(SETUP.tb.clone()))]),
			cond: None,
			output: None,
			timeout: None,
			parallel: false,
		}
	}

	fn a_usable_options(sender: &Sender<Notification>) -> Options {
		let mut ctx = Context::default();
		ctx.add_notifications(Some(sender));
		let opt = Options::default()
			.with_ns(Some(SETUP.ns.clone().into()))
			.with_db(Some(SETUP.db.clone().into()));
		opt
	}
}
