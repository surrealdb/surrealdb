use std::hint::black_box;
use std::path::Path;

use criterion::{Criterion, criterion_group, criterion_main};

fn parse(s: &str) {
	let x = surrealdb_parser::Parser::enter_parse::<ast::Query>(black_box(s), Default::default())
		.expect("");
	black_box(x);
}

fn walk_dir<F: FnMut(&Path)>(path: &Path, f: &mut F) {
	for r in std::fs::read_dir(path).expect("") {
		let r = r.expect("");
		let ft = r.file_type().expect("");
		let path = r.path();
		if ft.is_file() {
			f(&path)
		}
		if ft.is_dir() {
			walk_dir(&path, f);
		}
	}
}

fn gather_lang_tests() -> String {
	// Some tests are not yet compatible with this parser.
	let ignore = [
		"language/control_flow/transaction/cancel_behaviour.surql",
		"language/control_flow/transaction/commit_behaviour.surql",
		"language/statements/define/field/permissions_full_2.0.surql",
		"language/statements/remove/config/api.surql",
		"language/statements/remove/config/default.surql",
		"language/statements/remove/config/graphql.surql",
		"language/statements/remove/config/not_exists.surql",
		"language/statements/select/fetch/objects.surql",
		"language/statements/alter/alter_param.surql",
		"language/statements/alter/alter_user.surql",
		"language/statements/alter/alter_function.surql",
		"language/statements/alter/alter_event.surql",
		"language/statements/alter/alter_config.surql",
		"language/statements/alter/alter_bucket.surql",
		"language/statements/alter/alter_api.surql",
		"language/statements/alter/alter_analyzer.surql",
		"language/statements/alter/alter_access.surql",
		"language/graph/edge_clauses.surql",
		"reproductions/7169_from_only_in_graph_lookup.surql",
	];

	let mut res = String::new();
	walk_dir(Path::new("../../language-tests/tests"), &mut |path| {
		if path.extension().and_then(|x| x.to_str()) != Some("surql") {
			return;
		}

		for i in ignore {
			if path.ends_with(i) {
				return;
			}
		}

		let source = std::fs::read_to_string(path).expect("");
		// Skip all tests which expect a parsing error
		if source.contains("parsing-error") {
			return;
		}
		// Skip all tests which might contain to-be-fixed errors.
		if source.contains("wip = true") {
			return;
		}
		res.push_str(";\n");
		res.push_str(&source);
	});
	res
}

fn criterion_benchmark(c: &mut Criterion) {
	let lang_test = gather_lang_tests();

	c.bench_function("parse_all_tests", |b| b.iter(|| parse(&lang_test)));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
