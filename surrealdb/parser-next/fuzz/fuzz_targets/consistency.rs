#![no_main]

//! Parses every input as a query and checks what must hold whatever the
//! input: no panic; an accepted query, printed as SurrealQL, parses back, and
//! reprinting only shortens the text until it settles; and a statement stream fed the input in two pieces yields
//! the statements a whole parse yields. Seed it from the language-test corpus
//! and use `surrealql.dict`.

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use surrealdb_expr::parser::{
	ParserConfig, ParserSettings, StatementStream, parse, query_with_settings,
};
use surrealdb_types::ToSql;

/// Replaces the random 128-character keys minted for access methods defined
/// without one, which differ on every parse.
fn mask_keys(sql: &str) -> String {
	let mut out = String::with_capacity(sql.len());
	let mut rest = sql;
	while let Some(i) = rest.find('\'') {
		let after = &rest[i + 1..];
		let key = after.len() > 128
			&& after.as_bytes()[128] == b'\''
			&& after.as_bytes()[..128].iter().all(|b| b.is_ascii_alphanumeric());
		if key {
			out.push_str(&rest[..i]);
			out.push_str("'random'");
			rest = &after[129..];
		} else {
			out.push_str(&rest[..i + 1]);
			rest = after;
		}
	}
	out.push_str(rest);
	out
}

fuzz_target!(|data: &str| {
	let Ok(ast) = parse(data) else {
		return;
	};
	let printed: Vec<String> = ast.expressions.iter().map(|e| mask_keys(&e.to_sql())).collect();
	// Printing normalises (`+ 1` prints as `+1`, which reads back as the
	// number), so each round may only shorten the text until it settles.
	for text in &printed {
		// Printing may add parentheses, so printed text is read back under
		// the stored-text budgets, as stored definitions are.
		let print = |t: &str| -> Vec<String> {
			let ast = query_with_settings(t, ParserSettings::STORED_TEXT)
				.unwrap_or_else(|e| panic!("printed text rejected: {t}\n{e}"));
			ast.expressions.iter().map(|e| mask_keys(&e.to_sql())).collect()
		};
		let mut current = text.clone();
		loop {
			let next = print(&current);
			assert_eq!(next.len(), 1, "printed text is one statement: {current}");
			if next[0] == current {
				break;
			}
			assert!(next[0].len() < current.len(), "printed text grows: {current}");
			current = next[0].clone();
		}
	}

	let mut split = data.len() / 2;
	while !data.is_char_boundary(split) {
		split -= 1;
	}
	// The settings `parse` uses: every optional grammar enabled.
	let settings = ParserSettings::all_features(&ParserConfig::default());
	let mut stream = StatementStream::new_with_settings(settings);
	let mut buf = BytesMut::from(&data.as_bytes()[..split]);
	let mut streamed = Vec::new();
	while let Ok(Some(s)) = stream.parse_partial(&mut buf) {
		streamed.push(mask_keys(&s.to_sql()));
	}
	buf.extend_from_slice(&data.as_bytes()[split..]);
	loop {
		match stream.parse_complete(&mut buf) {
			Ok(Some(s)) => streamed.push(mask_keys(&s.to_sql())),
			Ok(None) => break,
			Err(e) => panic!("the stream rejects what a whole parse accepts: {e}"),
		}
	}
	assert_eq!(streamed, printed, "streamed statements differ from a whole parse");
});
