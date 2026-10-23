//! The `MATERIALIZE INTO` clause of a `REFERENCE` field.

use surrealdb_expr::expr::Expr;
use surrealdb_expr::expr::statements::alter::AlterKind;
use surrealdb_expr::expr::statements::{AlterStatement, DefineFieldStatement, DefineStatement};
use surrealdb_expr::parser::{self, ParserConfig, ParserSettings};
use surrealdb_types::ToSql;

/// The whole of `text` as one expression: nothing after it is left unread.
fn parse(text: &str) -> Result<Expr, parser::ParseError> {
	parser::expr_with_settings(text, ParserSettings::all_features(&ParserConfig::default()), true)
}

fn define(text: &str) -> Box<DefineFieldStatement> {
	let expr = parse(text).expect(text);
	let Expr::Define(define) = expr else {
		panic!("not a definition: {expr:?}");
	};
	let DefineStatement::Field(field) = *define else {
		panic!("not a field definition: {define:?}");
	};
	field
}

#[test]
fn the_clause_is_read_next_to_the_reference() {
	let field = define(
		"DEFINE FIELD author ON post TYPE record<author> REFERENCE ON DELETE CASCADE \
		 MATERIALIZE INTO author.posts WHERE archived != true",
	);
	let reference = field.reference.expect("a reference");
	let materialize = reference.materialize.expect("a clause");
	assert_eq!(materialize.table.as_str(), "author");
	assert_eq!(materialize.field.to_sql(), "posts");
	assert_eq!(materialize.filter.expect("a condition").to_sql(), "archived != true");
}

#[test]
fn the_clause_may_come_before_the_reference_and_has_no_condition_by_default() {
	let field = define("DEFINE FIELD a ON b MATERIALIZE INTO c.d REFERENCE");
	let materialize = field.reference.expect("a reference").materialize.expect("a clause");
	assert_eq!((materialize.table.as_str(), materialize.filter), ("c", None));
}

#[test]
fn the_clause_renders_as_it_was_written() {
	for text in [
		"DEFINE FIELD author ON post TYPE record<author> REFERENCE ON DELETE IGNORE MATERIALIZE INTO author.posts PERMISSIONS FULL",
		"DEFINE FIELD author ON post REFERENCE ON DELETE CASCADE MATERIALIZE INTO author.posts WHERE archived != true PERMISSIONS FULL",
		"DEFINE FIELD author ON post REFERENCE ON DELETE UNSET MATERIALIZE INTO `a b`.`c d` PERMISSIONS FULL",
	] {
		let rendered = parse(text).expect(text).to_sql();
		assert_eq!(rendered, text);
		assert_eq!(parse(&rendered).expect("a rendering reads back").to_sql(), rendered);
	}
}

#[test]
fn the_clause_needs_a_reference() {
	for text in [
		"DEFINE FIELD a ON b TYPE record<c> MATERIALIZE INTO c.d",
		"ALTER FIELD a ON b MATERIALIZE INTO c.d",
	] {
		let error = parse(text).expect_err(text).to_string();
		assert!(error.contains("MATERIALIZE requires a REFERENCE clause"), "{text}: {error}");
	}
}

#[test]
fn the_clause_is_malformed_without_its_parts() {
	for text in [
		"DEFINE FIELD a ON b REFERENCE MATERIALIZE c.d",
		"DEFINE FIELD a ON b REFERENCE MATERIALIZE INTO c",
		"DEFINE FIELD a ON b REFERENCE MATERIALIZE INTO c.d MATERIALIZE INTO e.f",
		"DEFINE FIELD a ON b REFERENCE MATERIALISE INTO c.d",
	] {
		assert!(parse(text).is_err(), "{text}");
	}
}

#[test]
fn materialize_is_no_reserved_word() {
	let field = define("DEFINE FIELD materialize ON materialize TYPE int");
	assert_eq!(field.what.to_sql(), "materialize");
	let field =
		define("DEFINE FIELD materialize ON t REFERENCE MATERIALIZE INTO materialize.materialize");
	let materialize = field.reference.expect("a reference").materialize.expect("a clause");
	assert_eq!(materialize.table.as_str(), "materialize");
	assert_eq!(materialize.field.to_sql(), "materialize");
}

#[test]
fn an_alteration_carries_the_clause_with_the_reference() {
	let Expr::Alter(alter) =
		parse("ALTER FIELD a ON b REFERENCE ON DELETE CASCADE MATERIALIZE INTO c.d WHERE e")
			.expect("an alteration")
	else {
		panic!("not an alteration");
	};
	let AlterStatement::Field(field) = *alter else {
		panic!("not a field alteration");
	};
	let AlterKind::Set(reference) = field.reference else {
		panic!("no reference set");
	};
	assert_eq!(reference.materialize.expect("a clause").table.as_str(), "c");
}
