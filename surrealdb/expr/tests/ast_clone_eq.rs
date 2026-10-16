//! `Clone` and `PartialEq` over parsed `Expr` values: a clone prints and
//! compares as its source, and equality agrees with the `Debug` rendering.

use std::fs;
use std::path::Path;

use surrealdb_expr::expr::Expr;
use surrealdb_expr::parser::{TopLevelExpr, parse};

/// File names (not paths) excluded from the corpus because an inner type's
/// equality and `Debug` rendering disagree for them.
const SKIP: &[&str] = &[];

fn collect(dir: &Path, out: &mut Vec<String>) {
	let Ok(entries) = fs::read_dir(dir) else {
		return;
	};
	let mut entries: Vec<_> = entries.flatten().map(|e| e.path()).collect();
	entries.sort();
	for path in entries {
		if path.is_dir() {
			collect(&path, out);
		} else if path.extension().is_some_and(|e| e == "surql") {
			let name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
			if SKIP.contains(&name) {
				continue;
			}
			if let Ok(text) = fs::read_to_string(&path) {
				out.push(text);
			}
		}
	}
}

fn corpus() -> Vec<Expr> {
	let root = Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../language-tests/tests"));
	let mut files = Vec::new();
	collect(root, &mut files);
	let mut exprs = Vec::new();
	for text in files {
		let Ok(ast) = parse(&text) else {
			continue;
		};
		for top in ast.expressions {
			if let TopLevelExpr::Expr(e) = top {
				exprs.push(e);
			}
		}
	}
	exprs
}

fn float_sensitive(dbg: &str) -> bool {
	dbg.contains("NaN") || dbg.contains("-0.0")
}

fn check(exprs: &[Expr]) {
	let dbg: Vec<String> = exprs.iter().map(|e| format!("{e:?}")).collect();
	for (i, e) in exprs.iter().enumerate() {
		let c = e.clone();
		assert_eq!(dbg[i], format!("{c:?}"), "clone changed the rendering");
		assert!(*e == c, "an expression differs from its clone: {}", dbg[i]);
		for step in [1usize, 7, 31] {
			let j = i + step;
			if j >= exprs.len() || float_sensitive(&dbg[i]) || float_sensitive(&dbg[j]) {
				continue;
			}
			assert_eq!(
				exprs[i] == exprs[j],
				dbg[i] == dbg[j],
				"equality disagrees with rendering:\n{}\n{}",
				dbg[i],
				dbg[j]
			);
		}
	}
}

#[test]
fn clone_and_eq_over_corpus() {
	let exprs = corpus();
	assert!(exprs.len() > 1000, "corpus too small: {}", exprs.len());
	check(&exprs);
}

#[test]
fn clone_and_eq_over_statement_kinds() {
	let exprs = corpus();
	let define: Vec<Expr> =
		exprs.iter().filter(|e| matches!(e, Expr::Define(_))).cloned().collect();
	let alter: Vec<Expr> = exprs.iter().filter(|e| matches!(e, Expr::Alter(_))).cloned().collect();
	let remove: Vec<Expr> =
		exprs.iter().filter(|e| matches!(e, Expr::Remove(_))).cloned().collect();
	assert!(!define.is_empty() && !alter.is_empty() && !remove.is_empty());
	check(&define);
	check(&alter);
	check(&remove);
}
