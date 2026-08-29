use super::format;

/// Formats `input`, asserting it is valid SurrealQL.
fn fmt(input: &str) -> String {
	format(input.as_bytes()).expect("input should be valid SurrealQL")
}

#[test]
fn reformats_basic_statement() {
	assert_eq!(fmt("select   *   from t;"), "SELECT * FROM t;\n");
}

#[test]
fn collapses_blank_line_runs_and_reformats_each_statement() {
	assert_eq!(
		fmt("select 1 from a;\n\n\nupdate b set x=1;"),
		"SELECT 1 FROM a;\n\nUPDATE b SET x = 1;\n",
	);
}

#[test]
fn normalizes_line_comment_markers_to_hash() {
	assert_eq!(fmt("-- hello\nselect 1 from a;"), "# hello\nSELECT 1 FROM a;\n");
	assert_eq!(fmt("// hi\nselect 1 from a;"), "# hi\nSELECT 1 FROM a;\n");
	assert_eq!(fmt("# yo\nselect 1 from a;"), "# yo\nSELECT 1 FROM a;\n");
}

#[test]
fn preserves_multi_line_comments() {
	assert_eq!(fmt("/* multi\nline */\nselect 1 from a;"), "/* multi\nline */\nSELECT 1 FROM a;\n",);
}

#[test]
fn keeps_statement_with_inline_comment_verbatim() {
	// The AST holds no comment nodes, so a statement carrying a comment must be
	// emitted unchanged rather than reflowed - the comment must never be lost.
	let out = fmt("select /* c */ 1 from t;");
	assert_eq!(out, "select /* c */ 1 from t;\n");
	assert!(out.contains("/* c */"));
}

#[test]
fn does_not_glue_a_trailing_comment_onto_the_statement() {
	assert_eq!(fmt("select 1 from t; -- note"), "SELECT 1 FROM t; # note\n");
}

#[test]
fn terminates_a_statement_missing_its_semicolon() {
	assert_eq!(fmt("select 1 from t"), "SELECT 1 FROM t;\n");
}

#[test]
fn normalizes_crlf_line_endings() {
	assert_eq!(
		fmt("select 1 from a;\r\nselect 2 from a;\r\n"),
		"SELECT 1 FROM a;\nSELECT 2 FROM a;\n",
	);
}

#[test]
fn empty_or_whitespace_only_input_yields_empty_output() {
	assert_eq!(fmt(""), "");
	assert_eq!(fmt("   \n\n  "), "");
}

#[test]
fn output_ends_with_exactly_one_newline() {
	let out = fmt("select 1 from a;\n\n\n");
	assert!(out.ends_with(";\n"), "got {out:?}");
	assert!(!out.ends_with(";\n\n"), "got {out:?}");
}

#[test]
fn is_idempotent() {
	let inputs = [
		"select   *   from t;",
		"-- c\nselect 1 from a;\n\nupdate b set x=1;",
		"select /* c */ 1 from t;",
		"select 1 from t; -- note",
		"/* block */\nselect 1 from a;",
	];
	for input in inputs {
		let once = fmt(input);
		let twice = format(once.as_bytes()).expect("formatted output should re-format");
		assert_eq!(once, twice, "not idempotent for {input:?}");
	}
}

#[test]
fn invalid_input_is_an_error() {
	// `SELECT` without a `FROM` clause does not parse.
	assert!(format(b"select 1;").is_err());
}
