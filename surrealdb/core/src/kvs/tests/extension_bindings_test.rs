//! An extension definition's `BINDINGS`, end to end through SurrealQL.
//!
//! The runner here prepares nothing and runs no guest: its one export,
//! `read(name)`, answers with what the invocation's bindings give for `name`.
//! Everything between the statement and that answer is the server's own: the
//! check of `BINDINGS` against the archive's `[bindings]` at `DEFINE`, the
//! definer's auth limit recorded with the definition, and the evaluation of a
//! binding with that limit, permissions off, no write, and none of the caller's
//! parameters in scope.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use surrealism_runtime::abi::Abi;
use surrealism_runtime::config::SurrealismConfig as PackageConfig;
use surrealism_runtime::exports::{ExportsManifest, FunctionExport};
use surrealism_runtime::package::SurrealismPackage;

use crate::dbs::Session;
use crate::dbs::capabilities::{Capabilities, ExperimentalTarget, Targets};
use crate::iam::{Level, Role};
use crate::kvs::Datastore;
use crate::surrealism::runner::{
	BoxFut, ExtensionRunner, Invocation, PreparedExtension, RunnerCapabilities, VerifiedSource,
};
use crate::types::{PublicValue, PublicVariables};

const PACKAGE: &str = r#"
[package]
organisation = "test"
name = "bound"
version = "1.0.0"

[bindings]
secret = { type = "string", description = "A value the caller may not read", sensitive = true }
greeting = "option<string>"
"#;

/// [`PACKAGE`], stamped as built for `abi`.
fn package(abi: Abi) -> PackageConfig {
	let mut config =
		surrealism_runtime::package_config::PackageConfig::parse(PACKAGE).unwrap().into_config();
	config.abi = abi.version();
	config
}

fn exports() -> ExportsManifest {
	ExportsManifest {
		functions: vec![FunctionExport {
			name: Some("read".to_owned()),
			args: vec![("name".to_owned(), surrealdb_types::Kind::String)],
			returns: surrealdb_types::Kind::Any,
			args_text: None,
			returns_text: None,
			writeable: false,
			declares_transaction: false,
			comment: None,
		}],
	}
}

/// Answers `read(name)` with the invocation's binding for `name`.
struct ReadsBindings {
	package: PackageConfig,
	exports: ExportsManifest,
}

impl PreparedExtension for ReadsBindings {
	fn package(&self) -> &PackageConfig {
		&self.package
	}

	fn exports(&self) -> &ExportsManifest {
		&self.exports
	}

	fn invoke<'a>(&'a self, call: Invocation<'a>) -> BoxFut<'a, anyhow::Result<Vec<PublicValue>>> {
		Box::pin(async move {
			let bindings = call.bindings.expect("every invocation is lent its bindings");
			let mut out = Vec::new();
			for row in call.rows {
				let Some(PublicValue::String(name)) = row.into_iter().next() else {
					anyhow::bail!("read takes a name");
				};
				let value = bindings.get(name).await.map_err(|e| anyhow::anyhow!("{e}"))?;
				out.push(value.unwrap_or(PublicValue::None));
			}
			Ok(out)
		})
	}
}

/// Prepares every archive as [`PACKAGE`] built for its ABI.
struct BindingsRunner(Abi);

impl ExtensionRunner for BindingsRunner {
	fn name(&self) -> &'static str {
		"bindings"
	}

	fn check(&self, _: &RunnerCapabilities) -> anyhow::Result<()> {
		Ok(())
	}

	fn prepare<'a>(
		&'a self,
		_: &'a VerifiedSource,
	) -> BoxFut<'a, anyhow::Result<Arc<dyn PreparedExtension>>> {
		Box::pin(async move {
			Ok(Arc::new(ReadsBindings {
				package: package(self.0),
				exports: exports(),
			}) as Arc<dyn PreparedExtension>)
		})
	}
}

/// A `.surli` holding [`PACKAGE`] built for `abi`. The runner never reads its
/// component.
fn archive(abi: Abi) -> Vec<u8> {
	let dir = tempfile::tempdir().unwrap();
	let path = dir.path().join("bound.surli");
	SurrealismPackage {
		config: package(abi),
		wasm: b"\0asm".to_vec(),
		exports: exports(),
		fs: None,
		logo: None,
		manifest: None,
		signature: None,
	}
	.pack(path.clone(), None)
	.unwrap();
	std::fs::read(path).unwrap()
}

fn owner() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

fn record_user() -> Session {
	Session::for_record(
		"test",
		"test",
		"user",
		PublicValue::RecordId(surrealdb_types::RecordId::new("user", "tobie")),
	)
}

fn db_owner() -> Session {
	Session::for_level(Level::Database("test".into(), "test".into()), Role::Owner)
		.with_ns("test")
		.with_db("test")
}

/// A datastore with the archive at `f"b:/bound.surli"`, a table only a
/// system user can read, and a record access method.
async fn ds() -> Arc<Datastore> {
	ds_for(Abi::LATEST).await
}

/// [`ds`], with the archive built for `abi`.
async fn ds_for(abi: Abi) -> Arc<Datastore> {
	let caps = Capabilities::all()
		.with_experimental(Targets::Some([ExperimentalTarget::Surrealism].into()));
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_capabilities(caps)
		.with_extension_runner(Arc::new(BindingsRunner(abi)))
		.build_with_path("memory")
		.await
		.unwrap();
	let mut vars = PublicVariables::new();
	vars.insert("archive", surrealdb_types::Bytes::from(archive(abi)));
	let setup = "
		DEFINE NAMESPACE test; DEFINE DATABASE test;
		DEFINE BUCKET b BACKEND 'memory' PERMISSIONS FULL;
		f\"b:/bound.surli\".put($archive);
		DEFINE TABLE vault SCHEMALESS PERMISSIONS NONE;
		CREATE vault:key SET value = 'shh';
		DEFINE ACCESS user ON DATABASE TYPE RECORD;
	";
	for (i, res) in ds.execute(setup, &owner(), Some(vars)).await.unwrap().into_iter().enumerate() {
		res.result.unwrap_or_else(|e| panic!("setup statement {i}: {e}"));
	}
	ds
}

async fn run(ds: &Datastore, ses: &Session, sql: &str) -> Result<PublicValue, String> {
	let mut res = ds.execute(sql, ses, None).await.unwrap();
	res.pop().unwrap().result.map_err(|e| e.to_string())
}

async fn define(ds: &Datastore, ses: &Session, name: &str, bindings: &str) -> Result<(), String> {
	run(
		ds,
		ses,
		&format!(
			"DEFINE EXTENSION ext::{name} FROM f\"b:/bound.surli\" UNSIGNED BINDINGS {bindings} PERMISSIONS FULL;"
		),
	)
	.await
	.map(|_| ())
}

/// A binding reads with permissions off: a record user who cannot read
/// `vault` gets the value an owner's definition binds from it.
#[tokio::test]
async fn a_binding_reads_with_the_definers_rights() {
	let ds = ds().await;
	define(&ds, &owner(), "bound", "{ secret: (SELECT VALUE value FROM ONLY vault:key) }")
		.await
		.unwrap();

	assert_eq!(
		run(&ds, &record_user(), "SELECT * FROM vault").await.unwrap(),
		PublicValue::Array(Default::default()),
		"the caller itself cannot read the table"
	);
	assert_eq!(
		run(&ds, &record_user(), "RETURN ext::bound::read('secret')").await.unwrap(),
		PublicValue::String("shh".into())
	);
}

/// The caller's parameters are not in a binding's scope, so a caller cannot
/// steer an expression that runs with permissions off.
#[tokio::test]
async fn a_binding_does_not_see_the_callers_parameters() {
	let ds = ds().await;
	define(&ds, &owner(), "bound", "{ secret: 'shh', greeting: $greeting }").await.unwrap();
	assert_eq!(
		run(&ds, &record_user(), "LET $greeting = 'injected'; RETURN ext::bound::read('greeting')")
			.await
			.unwrap(),
		PublicValue::None
	);
}

/// An `option<…>` name the definition leaves out reads as `NONE`; a name the
/// package does not declare is refused.
#[tokio::test]
async fn unbound_and_undeclared_names() {
	let ds = ds().await;
	define(&ds, &owner(), "bound", "{ secret: 'shh' }").await.unwrap();
	assert_eq!(
		run(&ds, &owner(), "RETURN ext::bound::read('greeting')").await.unwrap(),
		PublicValue::None
	);
	let err = run(&ds, &owner(), "RETURN ext::bound::read('nope')").await.unwrap_err();
	assert!(err.contains("`nope` is not declared under [bindings]"), "{err}");
}

/// `DEFINE` refuses `BINDINGS` that do not answer the archive's
/// `[bindings]`: a required name left out, or a name it does not declare.
#[tokio::test]
async fn define_checks_bindings_against_the_package() {
	let ds = ds().await;
	let err = define(&ds, &owner(), "missing", "{ greeting: 'hi' }").await.unwrap_err();
	assert!(err.contains("requires the binding `secret` of kind `string`"), "{err}");
	let err = define(&ds, &owner(), "extra", "{ secret: 'shh', other: 1 }").await.unwrap_err();
	assert!(err.contains("`BINDINGS` gives `other`"), "{err}");
}

/// An archive built for an ABI without bindings takes no `BINDINGS`, and its
/// `[bindings]` table asks nothing of a definition.
#[tokio::test]
async fn an_abi_without_bindings_takes_no_bindings() {
	let ds = ds_for(Abi::V4).await;
	let err = define(&ds, &owner(), "bound", "{ secret: 'shh' }").await.unwrap_err();
	assert!(err.contains("built for ABI 4, which cannot read bindings"), "{err}");
	define(&ds, &owner(), "bound", "{}").await.unwrap();
}

/// A value that is not of the declared kind is refused without the value in
/// the error: it was read with permissions off.
#[tokio::test]
async fn a_value_of_the_wrong_kind_is_refused_without_the_value() {
	let ds = ds().await;
	define(
		&ds,
		&owner(),
		"bound",
		"{ secret: 'shh', greeting: (SELECT VALUE value FROM ONLY vault:key) + 1 }",
	)
	.await
	.unwrap();
	let err = run(&ds, &record_user(), "RETURN ext::bound::read('greeting')").await.unwrap_err();
	assert!(!err.contains("shh"), "the error carries the value: {err}");
	define(&ds, &owner(), "number", "{ secret: 42 }").await.unwrap();
	let err = run(&ds, &record_user(), "RETURN ext::number::read('secret')").await.unwrap_err();
	assert!(err.contains("is not a value of kind `string`"), "{err}");
	assert!(!err.contains("42"), "the error carries the value: {err}");
}

/// A binding may not write, write to an external system or run code whose
/// effects cannot be known: every caller would trigger it with permissions
/// off. `DEFINE` refuses each one.
#[tokio::test]
async fn define_refuses_a_binding_with_effects() {
	let ds = ds().await;
	for (bindings, what) in [
		("{ secret: type::string((CREATE ONLY leak:1).id) }", "writes data"),
		("{ secret: type::string(http::post('http://127.0.0.1:1', {})) }", "external system"),
		("{ secret: type::string(ext::other::send()) }", "effects cannot be known"),
	] {
		let err = define(&ds, &owner(), "bound", bindings).await.unwrap_err();
		assert!(err.contains("The BINDINGS clause must not have effects"), "{bindings}: {err}");
		assert!(err.contains(what), "{bindings}: {err}");
	}
	let err = run(&ds, &owner(), "SELECT * FROM leak").await.unwrap_err();
	assert!(err.contains("does not exist"), "the write was made: {err}");
}

/// A stored function that was pure when the binding was defined, and is
/// redefined to call an extension, is refused when the binding is read: the
/// frame refuses the extension call itself, wherever it is reached from.
#[tokio::test]
async fn a_binding_refuses_an_extension_reached_through_a_redefined_function() {
	let ds = ds().await;
	define(&ds, &owner(), "other", "{ secret: 'from other' }").await.unwrap();
	run(&ds, &owner(), "DEFINE FUNCTION fn::get() { RETURN 'pure' } PERMISSIONS FULL;")
		.await
		.unwrap();
	define(&ds, &owner(), "bound", "{ secret: fn::get() }").await.unwrap();
	assert_eq!(
		run(&ds, &owner(), "RETURN ext::bound::read('secret')").await.unwrap(),
		PublicValue::String("pure".into())
	);
	run(
		&ds,
		&owner(),
		"DEFINE FUNCTION OVERWRITE fn::get() { RETURN ext::other::read('secret') } PERMISSIONS FULL;",
	)
	.await
	.unwrap();
	// Outside a binding the call runs, so the refusal below is the frame's.
	assert_eq!(
		run(&ds, &owner(), "RETURN fn::get()").await.unwrap(),
		PublicValue::String("from other".into())
	);
	let err = run(&ds, &owner(), "RETURN ext::bound::read('secret')").await.unwrap_err();
	assert!(err.contains("The binding `secret` of extension"), "{err}");
	assert!(err.contains("failed to evaluate"), "{err}");
}

/// The definer's level is recorded with the definition and caps the caller's:
/// a root caller of a database owner's definition evaluates its bindings at
/// that database, so a binding that reads the namespace fails, while the same
/// binding in a root's definition succeeds.
#[tokio::test]
async fn a_binding_is_capped_by_the_definers_level() {
	let ds = ds().await;
	let ns_info = "{ secret: type::string((INFO FOR NS).databases) }";
	define(&ds, &owner(), "by_root", ns_info).await.unwrap();
	define(&ds, &db_owner(), "by_db_owner", ns_info).await.unwrap();
	run(&ds, &owner(), "RETURN ext::by_root::read('secret')").await.unwrap();
	let err = run(&ds, &owner(), "RETURN ext::by_db_owner::read('secret')").await.unwrap_err();
	assert!(err.contains("failed to evaluate"), "{err}");
}

/// `INFO FOR DB` shows each binding's expression, never its value, and the
/// definition renders its clause.
#[tokio::test]
async fn info_shows_the_expressions() {
	let ds = ds().await;
	define(&ds, &owner(), "bound", "{ secret: (SELECT VALUE value FROM ONLY vault:key) }")
		.await
		.unwrap();
	let info = format!("{:?}", run(&ds, &owner(), "(INFO FOR DB).extensions").await.unwrap());
	assert!(
		info.contains("BINDINGS { secret: (SELECT VALUE `value` FROM ONLY vault:key) }"),
		"{info}"
	);
	let structure =
		format!("{:?}", run(&ds, &owner(), "(INFO FOR DB STRUCTURE).extensions").await.unwrap());
	assert!(structure.contains("bindings"), "{structure}");
	assert!(!structure.contains("shh"), "{structure}");
}
