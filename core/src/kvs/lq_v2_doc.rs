use std::borrow::Cow;

use crate::cf::TableMutation;
use crate::dbs::Workable;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::{Array, Object, Value};

const EMPTY_DOC: Value = Value::None;

/// Construct a document from a Change Feed mutation
/// This is required to perform document operations such as live query notifications
pub(in crate::kvs) fn construct_document(
	mutation: &TableMutation,
) -> Result<Option<Document>, Error> {
	match mutation {
		TableMutation::Set(id, current_value) => {
			let doc = Document::new_artificial(
				Some(id),
				None,
				Cow::Borrowed(current_value),
				Cow::Owned(EMPTY_DOC),
				Workable::Normal,
			);
			Ok(Some(doc))
		}
		TableMutation::Del(id) => {
			let fake_previous_value_because_we_need_the_id_and_del_doesnt_store_value =
				Value::Object(Object::from(map! {
					"id" => Value::Thing(id.clone()),
				}));
			let doc = Document::new_artificial(
				Some(id),
				None,
				Cow::Owned(Value::None),
				Cow::Owned(fake_previous_value_because_we_need_the_id_and_del_doesnt_store_value),
				Workable::Normal,
			);
			Ok(Some(doc))
		}
		TableMutation::Def(_) => Ok(None),
		TableMutation::SetWithDiff(id, current_value, operations) => {
			// We need a previous value otherwise the Value::compute function won't work correctly
			// This is also how IDs are carried into notifications, not via doc.rid
			let mut copy = current_value.clone();
			copy.patch(Value::Array(Array(
				operations.iter().map(|op| Value::Object(Object::from(op.clone()))).collect(),
			)))?;
			let doc = Document::new_artificial(
				Some(id),
				None,
				Cow::Borrowed(current_value),
				Cow::Owned(copy),
				Workable::Normal,
			);
			trace!("Constructed artificial document: {:?}, is_new={}", doc, doc.is_new());
			// TODO(SUR-328): reverse diff and apply to doc to retrieve original version of doc
			Ok(Some(doc))
		}
		TableMutation::DelWithOriginal(id, val) => {
			let doc = Document::new_artificial(
				Some(id),
				None,
				Cow::Owned(Value::None),
				Cow::Borrowed(val),
				Workable::Normal,
			);
			Ok(Some(doc))
		}
	}
}

#[cfg(test)]
mod test {
	use crate::cf::TableMutation;
	use crate::kvs::lq_v2_doc::construct_document;
	use crate::sql::statements::DefineTableStatement;
	use crate::sql::{Idiom, Object, Operation, Strand, Thing, Value};

	#[test]
	fn test_construct_document_create() {
		let thing = Thing::from(("table", "id"));
		let value = Value::Strand(Strand::from("value"));
		let tb_mutation = TableMutation::Set(thing.clone(), value);
		let doc = construct_document(&tb_mutation).unwrap();
		let doc = doc.unwrap();
		assert!(doc.is_new());
		assert!(doc.initial_doc().is_none());
		assert!(doc.current_doc().is_some());
	}

	#[test]
	fn test_construct_document_empty_value_is_valid() {
		let thing = Thing::from(("table", "id"));
		let value = Value::None;
		let tb_mutation = TableMutation::Set(thing.clone(), value);
		let doc = construct_document(&tb_mutation).unwrap();
		let doc = doc.unwrap();
		assert!(!doc.is_new());
		// This is actually invalid data - we are going to treat it as delete though
		assert!(doc.is_delete());
		assert!(doc.initial_doc().is_none());
		assert!(doc.current_doc().is_none());
	}

	#[test]
	fn test_construct_document_update() {
		let thing = Thing::from(("table", "id"));
		let current_value = Value::Object(Object(map! {
			"first_field".to_string() => Value::Strand(Strand::from("first_value")),
			"second_field".to_string() => Value::Strand(Strand::from("second_value")),
		}));
		let operations = vec![
			Operation::Remove {
				path: Idiom::from("first_field"),
			},
			Operation::Replace {
				path: Idiom::from("second_field"),
				value: Value::Strand(Strand::from("original_value")),
			},
			Operation::Add {
				path: Idiom::from("third_field"),
				value: Value::Strand(Strand::from("third_value")),
			},
		];
		let expected_original = Value::Object(Object(map! {
			"second_field".to_string() => Value::Strand(Strand::from("original_value")),
			"third_field".to_string() => Value::Strand(Strand::from("third_value")),
		}));
		let tb_mutation =
			TableMutation::SetWithDiff(thing.clone(), current_value.clone(), operations);
		let doc = construct_document(&tb_mutation).unwrap();
		let doc = doc.unwrap();
		assert!(!doc.is_new());
		assert!(!doc.is_delete());
		assert_eq!(doc.initial_doc(), &expected_original, "{:?}", doc.initial_doc());
		assert_eq!(doc.current_doc(), &current_value, "{:?}", doc.current_doc());
	}

	#[test]
	fn test_construct_document_delete() {
		let thing = Thing::from(("table", "id"));
		let tb_mutation = TableMutation::Del(thing.clone());
		let doc = construct_document(&tb_mutation).unwrap();
		let doc = doc.unwrap();
		// The previous and current doc values are "None", so technically this is a new doc as per
		// current == None
		assert!(!doc.is_new(), "{:?}", doc);
		assert!(doc.is_delete(), "{:?}", doc);
		assert!(doc.current_doc().is_none());
		assert!(doc.initial_doc().is_some());
		match doc.initial_doc() {
			Value::Object(o) => {
				assert!(o.contains_key("id"));
				assert_eq!(o.get("id").unwrap(), &Value::Thing(thing));
			}
			_ => panic!("Initial doc should be an object"),
		}
	}

	#[test]
	fn test_construct_document_delete_with_original() {
		let thing = Thing::from(("table", "id"));
		let original = Value::Object(Object(map! {
			"id".to_string() => Value::Thing(thing.clone()),
			"some_key".to_string() => Value::Strand(Strand::from("some_value")),
		}));
		let tb_mutation = TableMutation::DelWithOriginal(thing.clone(), original);
		let doc = construct_document(&tb_mutation).unwrap();
		let doc = doc.unwrap();
		// The previous and current doc values are "None", so technically this is a new doc as per
		// current == None
		assert!(!doc.is_new(), "{:?}", doc);
		assert!(doc.is_delete(), "{:?}", doc);
		assert!(doc.current_doc().is_none());
		assert!(doc.initial_doc().is_some());
		match doc.initial_doc() {
			Value::Object(o) => {
				assert!(o.contains_key("id"));
				assert_eq!(o.get("id").unwrap(), &Value::Thing(thing));
			}
			_ => panic!("Initial doc should be an object"),
		}
	}

	#[test]
	fn test_construct_document_none_for_schema() {
		let tb_mutation = TableMutation::Def(DefineTableStatement::default());
		let doc = construct_document(&tb_mutation).unwrap();
		assert!(doc.is_none());
	}
}

#[cfg(feature = "kv-mem")]
#[cfg(test)]
mod test_check_lqs_and_send_notifications {
	use std::collections::BTreeMap;
	use std::sync::Arc;

	use channel::Sender;
	use futures::executor::block_on;
	use once_cell::sync::Lazy;
	use reblessive::TreeStack;

	use crate::cf::TableMutation;
	use crate::ctx::Context;
	use crate::dbs::fuzzy_eq::FuzzyEq;
	use crate::dbs::{Action, Notification, Options, Session, Statement};
	use crate::fflags::FFLAGS;
	use crate::iam::{Auth, Role};
	use crate::kvs::lq_v2_doc::construct_document;
	use crate::kvs::{Datastore, LockType, TransactionType};
	use crate::sql::paths::{OBJ_PATH_ACCESS, OBJ_PATH_AUTH, OBJ_PATH_TOKEN};
	use crate::sql::statements::{CreateStatement, DeleteStatement, LiveStatement};
	use crate::sql::{Fields, Object, Strand, Table, Thing, Uuid, Value, Values};

	const SETUP: Lazy<Arc<TestSuite>> = Lazy::new(|| Arc::new(block_on(setup_test_suite_init())));

	struct TestSuite {
		ds: Datastore,
		ns: String,
		db: String,
		tb: String,
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

		TestSuite {
			ds,
			ns: ns.to_string(),
			db: db.to_string(),
			tb: tb.to_string(),
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

		// WHEN:
		// Construct document we are validating
		let record_id = Thing::from((SETUP.tb.as_str(), "id"));
		let value = Value::Strand(Strand::from("value"));
		let tb_mutation = TableMutation::Set(record_id.clone(), value);
		let doc = construct_document(&tb_mutation).unwrap().unwrap();

		// AND:
		// Perform "live query" on the constructed doc that we are checking
		let live_statement = a_live_query_statement();
		let executed_statement = a_create_statement();
		let mut stack = TreeStack::new();
		stack.enter(|stk| async {
			doc.check_lqs_and_send_notifications(
				stk,
				&opt,
				&Statement::Create(&executed_statement),
				&tx,
				&[&live_statement],
				&sender,
			)
			.await
			.unwrap();
		});

		// THEN:
		let notification = receiver.try_recv().expect("There should be a notification");
		assert!(
			notification.fuzzy_eq(&Notification::new(
				Uuid::default(),
				Action::Create,
				Value::Strand(Strand::from("value"))
			)),
			"{:?}",
			notification
		);
		assert!(receiver.try_recv().is_err());
		tx.lock().await.cancel().await.unwrap();
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

		// WHEN:
		// Construct document we are validating
		let record_id = Thing::from((SETUP.tb.as_str(), "id"));
		let value = Value::Strand(Strand::from("value"));
		let tb_mutation = TableMutation::Set(record_id.clone(), value);
		let doc = construct_document(&tb_mutation).unwrap().unwrap();

		// AND:
		// Perform "live query" on the constructed doc that we are checking
		let live_statement = a_live_query_statement();
		let executed_statement = a_delete_statement();
		let mut stack = TreeStack::new();
		stack.enter(|stk| async {
			doc.check_lqs_and_send_notifications(
				stk,
				&opt,
				&Statement::Delete(&executed_statement),
				&tx,
				&[&live_statement],
				&sender,
			)
			.await
			.unwrap();
		});

		// THEN:
		let notification = receiver.try_recv().expect("There should be a notification");
		// TODO(SUR-349): Delete value should be the object that was just deleted
		let expected_value = Value::Object(Object::default());
		assert!(
			notification.fuzzy_eq(&Notification::new(
				Uuid::default(),
				Action::Delete,
				expected_value
			)),
			"{:?}",
			notification
		);
		assert!(receiver.try_recv().is_err());
		tx.lock().await.cancel().await.unwrap();
	}

	// Live queries will have authentication info associated with them
	// This is a way to fake that
	fn a_live_query_statement() -> LiveStatement {
		let mut stm = LiveStatement::new(Fields::all());
		let mut session: BTreeMap<String, Value> = BTreeMap::new();
		session.insert(OBJ_PATH_ACCESS.to_string(), Value::Strand(Strand::from("access")));
		session.insert(OBJ_PATH_AUTH.to_string(), Value::Strand(Strand::from("auth")));
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
