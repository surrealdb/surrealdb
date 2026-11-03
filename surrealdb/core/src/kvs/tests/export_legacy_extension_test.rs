//! An export of a database that holds an unnamed silo extension imports back.
//!
//! Earlier releases stored `DEFINE MODULE silo::org::pkg<v>` with no name,
//! and the catalog keeps loading such a row. An export renders it as
//! `DEFINE EXTENSION silo::org::pkg<v> …`, the only form that names it. An
//! ordinary query refuses that form, so every new extension gets an `ext::`
//! name; an import accepts it, so a dump of the database restores.
//!
//! A dump a 3.x binary wrote imports too: it spells the source `AS` and has
//! no `UNSIGNED`. Each definition without `UNSIGNED` is stored as a legacy
//! row, as a 3.x catalog row reads; outside an import, such a definition is
//! refused.
//!
//! The row cannot be written in SurrealQL by this version, so it is stored
//! directly under the name the catalog derives for it.

#![allow(clippy::unwrap_used)]

use std::borrow::Cow;
use std::sync::Arc;

use surrealdb_kvs::TransactionType::Write;

use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{ModuleExecutable, SiloExecutable, StoredModuleDefinition, StoredPermission};
use crate::dbs::Session;
use crate::dbs::capabilities::{Capabilities, ExperimentalTarget, Targets};
use crate::key::schema::ModuleKey;
use crate::kvs::Datastore;

/// The storage name of the legacy row, which keys it in the catalog.
const LEGACY: &str = "silo::acme::widgets::<1.2.3>";
/// The name SurrealQL addresses the legacy row by.
const LEGACY_NAME: &str = "silo::acme::widgets<1.2.3>";

async fn ds() -> Arc<Datastore> {
	let caps = Capabilities::all()
		.with_experimental(Targets::Some([ExperimentalTarget::Surrealism].into()));
	Datastore::builder()
		.without_maintenance_tasks()
		.with_capabilities(caps)
		.build_with_path("memory")
		.await
		.unwrap()
}

fn session() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

async fn export_text(ds: &Datastore, ses: &Session) -> String {
	let (tx, rx) = crate::channel::bounded::<Vec<u8>>(16);
	let task = ds.export_with_config(ses, tx, Default::default()).await.unwrap();
	let collector = tokio::spawn(async move {
		let mut out = Vec::new();
		while let Ok(chunk) = rx.recv().await {
			out.extend_from_slice(&chunk);
		}
		out
	});
	task.await.unwrap();
	String::from_utf8(collector.await.unwrap()).unwrap()
}

/// The extensions `INFO FOR DB` lists, keyed by the name SurrealQL addresses
/// each by.
async fn extensions(ds: &Datastore, ses: &Session) -> String {
	let mut res = ds.execute("(INFO FOR DB).extensions", ses, None).await.unwrap();
	format!("{:?}", res.remove(0).result.unwrap())
}

/// A datastore holding one unnamed silo definition, as an earlier release
/// stored it.
async fn with_legacy_row() -> (Arc<Datastore>, Session) {
	let ds = ds().await;
	let ses = session();
	for result in
		ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE test;", &ses, None).await.unwrap()
	{
		result.result.unwrap();
	}
	let txn = ds.transaction(Write).await.unwrap();
	let db = txn.expect_db_by_name("test", "test").await.unwrap();
	let row = StoredModuleDefinition {
		name: None,
		comment: Some("legacy silo row".to_string()),
		permissions: StoredPermission::Full,
		executable: ModuleExecutable::Silo(SiloExecutable {
			organisation: "acme".to_string(),
			package: "widgets".to_string(),
			major: 1,
			minor: 2,
			patch: 3,
		}),
		unsigned: true,
		// A row a 3.x release wrote records no digest, signer or capabilities.
		digest: None,
		signer: None,
		capabilities: None,
		bindings: Vec::new(),
		auth_limit: Default::default(),
	};
	txn.set_key(&ModuleKey::new(db.namespace_id, db.database_id, Cow::Borrowed(LEGACY)), &row)
		.await
		.unwrap();
	txn.commit().await.unwrap();
	(ds, ses)
}

#[tokio::test]
async fn an_unnamed_silo_extension_survives_export_and_import() {
	let (source, ses) = with_legacy_row().await;
	let before = extensions(&source, &ses).await;
	assert!(before.contains(LEGACY_NAME), "the legacy row is listed: {before}");

	let dump = export_text(&source, &ses).await;
	assert!(
		dump.contains(&format!("DEFINE EXTENSION {LEGACY_NAME} UNSIGNED")),
		"the dump renders the row in the unnamed form:\n{dump}"
	);

	let target = ds().await;
	for result in target.import(&dump, &ses).await.unwrap() {
		result.result.unwrap();
	}
	assert_eq!(extensions(&target, &ses).await, before);
}

#[tokio::test]
async fn an_ordinary_query_refuses_the_unnamed_silo_form() {
	let ds = ds().await;
	let ses = session();
	let err = ds
		.execute(&format!("DEFINE EXTENSION {LEGACY_NAME} UNSIGNED"), &ses, None)
		.await
		.expect_err("a new extension needs a name");
	assert!(err.to_string().contains("An extension needs a name"), "{err}");
}

/// A dump as a 3.x binary writes it: the `MODULE` keyword, `AS` for `FROM`,
/// the version without `::`, and no `UNSIGNED`.
const DUMP_3X: &str = r#"OPTION IMPORT;
DEFINE MODULE silo::acme::w<1.0.0> PERMISSIONS FULL;
DEFINE MODULE mod::x AS f"b:/x.surli" PERMISSIONS FULL;"#;

#[tokio::test]
async fn a_3x_dump_imports_as_legacy_rows() {
	let ds = ds().await;
	let ses = session();
	for result in
		ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE test;", &ses, None).await.unwrap()
	{
		result.result.unwrap();
	}
	for result in ds.import(DUMP_3X, &ses).await.unwrap() {
		result.result.unwrap();
	}
	let listed = extensions(&ds, &ses).await;
	for expected in [
		"DEFINE EXTENSION silo::acme::w<1.0.0> PERMISSIONS FULL",
		r#"DEFINE EXTENSION ext::x FROM f\"b:/x.surli\" PERMISSIONS FULL"#,
	] {
		assert!(listed.contains(expected), "{expected} in {listed}");
	}
	assert!(!listed.contains("UNSIGNED"), "a legacy row is not unsigned: {listed}");
}

#[tokio::test]
async fn a_definition_outside_an_import_needs_unsigned() {
	let ds = ds().await;
	let ses = session();
	let mut res = ds
		.execute(
			r#"DEFINE NAMESPACE test; DEFINE DATABASE test; DEFINE EXTENSION ext::x FROM f"b:/x.surli";"#,
			&ses,
			None,
		)
		.await
		.unwrap();
	let err = res.pop().unwrap().result.expect_err("a definition needs UNSIGNED");
	assert!(err.to_string().contains("DEFINE EXTENSION needs SIGNER"), "{err}");
}

/// An imported definition with `DIGEST` and `UNSIGNED` over a file is loaded
/// eagerly once it is written, and the load reads the bucket it names. The
/// archive is not there yet, which an import accepts for a definition with
/// `DIGEST`.
#[tokio::test]
async fn an_imported_file_extension_is_loaded_unsealed() {
	let ds = ds().await;
	let ses = session();
	for result in
		ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE test;", &ses, None).await.unwrap()
	{
		result.result.unwrap();
	}
	let digest = format!("sha256:{}", "5d".repeat(32));
	let dump = format!(
		"OPTION IMPORT;\nDEFINE EXTENSION ext::x FROM f\"b:/x.surli\" DIGEST '{digest}' UNSIGNED PERMISSIONS FULL;"
	);
	for result in ds.import(&dump, &ses).await.unwrap() {
		result.result.unwrap();
	}
	assert!(extensions(&ds, &ses).await.contains("ext::x"));
}

/// An imported definition without `DIGEST`, or without both `SIGNER` and
/// `UNSIGNED`, is a legacy row like a 3.x catalog row: a call refuses it before
/// any archive is fetched, and says to reinstall it.
#[tokio::test]
async fn an_imported_legacy_row_is_refused_when_called() {
	let ds = ds().await;
	let ses = session();
	for result in
		ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE test;", &ses, None).await.unwrap()
	{
		result.result.unwrap();
	}
	let digest = format!("sha256:{}", "5d".repeat(32));
	let dump = format!(
		"{DUMP_3X}\nDEFINE EXTENSION ext::y FROM f\"b:/y.surli\" DIGEST '{digest}' CAPABILITIES {{}} PERMISSIONS FULL;"
	);
	for result in ds.import(&dump, &ses).await.unwrap() {
		result.result.unwrap();
	}
	let listed = extensions(&ds, &ses).await;
	assert!(!listed.contains("DIGEST"), "an imported legacy row records no DIGEST: {listed}");
	// Without the `surrealism` feature every extension call is refused for the
	// missing feature before the definition is read.
	#[cfg(feature = "surrealism")]
	for call in ["silo::acme::w<1.0.0>::f()", "ext::x::f()", "ext::y::f()"] {
		let mut res = ds.execute(&format!("RETURN {call}"), &ses, None).await.unwrap();
		let err = res.remove(0).result.expect_err("a legacy row does not run");
		assert!(err.to_string().contains(crate::catalog::REINSTALL_WARNING), "{call}: {err}");
	}
	let structure = ds
		.execute("(INFO FOR DB STRUCTURE).extensions", &ses, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap()
		.into_array()
		.unwrap();
	assert_eq!(structure.len(), 3, "{structure:?}");
	for extension in structure {
		let text = format!("{extension:?}");
		assert!(text.contains(crate::catalog::REINSTALL_WARNING), "{text}");
	}
}
