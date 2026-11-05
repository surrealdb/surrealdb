//! Holds the catalog's stored-grammar numbering to its committed lock.
//!
//! The committed `catalog-schema.lock` must equal the lock rendered from the derived schemas, and
//! must keep every rule of `surrealdb_catalog::stored::lock` against itself as the previous
//! version and against the lock named by `CATALOG_SCHEMA_BASE`, which CI sets to the base
//! branch's lock. `CATALOG_SCHEMA=OVERWRITE` rewrites the committed lock after the rules have
//! passed; it cannot bypass one.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use surrealdb_catalog::stored::lock::{LockViolation, all_schemas, check, check_alone, render};

const REGENERATE: &str =
	"CATALOG_SCHEMA=OVERWRITE cargo test --locked -p surrealdb-catalog --test catalog_schema_lock";

fn lock_path() -> PathBuf {
	Path::new(env!("CARGO_MANIFEST_DIR")).join("catalog-schema.lock")
}

fn read(path: &Path) -> Option<String> {
	match fs::read_to_string(path) {
		Ok(text) => Some(text),
		Err(error) if error.kind() == ErrorKind::NotFound => None,
		Err(error) => panic!("cannot read {}: {error}", path.display()),
	}
}

fn report(what: &str, violations: &[LockViolation]) -> String {
	let mut out = format!("{what} breaks the schema lock rules:\n");
	for violation in violations {
		out.push_str(&format!("  {violation}\n"));
	}
	out
}

#[test]
fn the_catalog_schema_lock_holds() {
	let rendered = render(&all_schemas());
	let path = lock_path();
	let committed = read(&path);

	if let Err(violations) = check_alone(&rendered) {
		panic!("{}", report("the rendered schema", &violations));
	}
	if let Some(committed) = &committed
		&& let Err(violations) = check(committed, &rendered)
	{
		panic!("{}", report("the rendered schema against the committed lock", &violations));
	}
	// A base that is named but unreadable fails: an unset variable is the only way to skip.
	if let Some(base) = std::env::var_os("CATALOG_SCHEMA_BASE").filter(|base| !base.is_empty()) {
		let base_path = PathBuf::from(base);
		let Some(base) = read(&base_path) else {
			panic!("CATALOG_SCHEMA_BASE names {}, which does not exist", base_path.display());
		};
		if let Err(violations) = check(&base, &rendered) {
			panic!("{}", report("the rendered schema against the base branch's lock", &violations));
		}
	}

	if std::env::var("CATALOG_SCHEMA").is_ok_and(|mode| mode == "OVERWRITE") {
		if committed.as_deref() != Some(rendered.as_str()) {
			fs::write(&path, &rendered)
				.unwrap_or_else(|error| panic!("cannot write {}: {error}", path.display()));
		}
		return;
	}
	match committed {
		Some(committed) => assert!(
			committed == rendered,
			"{} is out of date with the catalog's schemas. Review the change to the numbering, then \
			 regenerate it with\n  {REGENERATE}",
			path.display()
		),
		None => panic!("{} does not exist. Generate it with\n  {REGENERATE}", path.display()),
	}
}
