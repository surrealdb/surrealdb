use crate::cf::TableMutation;
use crate::kvs::ds;
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
