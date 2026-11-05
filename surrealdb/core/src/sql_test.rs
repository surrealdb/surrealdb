//! Runtime expression round trips and kind rendering through the native parser.

use rstest::rstest;
use surrealdb_expr::parser::kind_for_definition;
use surrealdb_types::ToSql;

use crate::expr::Kind;
use crate::expr::kind::GeometryKind;
use crate::syn;

/// Statement counts per execution unit for a parsed query.
fn unit_sizes(query: &str) -> Vec<usize> {
	syn::parse(query)
		.unwrap()
		.into_execution_units()
		.iter()
		.map(surrealdb_expr::parser::Query::num_statements)
		.collect()
}

#[test]
fn splits_plain_statements_individually() {
	assert_eq!(unit_sizes("RETURN 1; RETURN 2; RETURN 3"), vec![1, 1, 1]);
}

#[test]
fn keeps_transaction_block_as_one_unit() {
	// BEGIN; RETURN 1; RETURN 2; COMMIT -> one unit of four statements.
	assert_eq!(unit_sizes("BEGIN; RETURN 1; RETURN 2; COMMIT"), vec![4]);
}

#[test]
fn mixes_blocks_and_plain_statements() {
	assert_eq!(unit_sizes("RETURN 0; BEGIN; RETURN 1; COMMIT; RETURN 2"), vec![1, 3, 1]);
}

#[test]
fn unterminated_block_keeps_remainder() {
	// A BEGIN with no COMMIT/CANCEL carries the rest of the query.
	assert_eq!(unit_sizes("RETURN 0; BEGIN; RETURN 1; RETURN 2"), vec![1, 3]);
}

#[test]
fn cancel_closes_a_block() {
	assert_eq!(unit_sizes("BEGIN; RETURN 1; CANCEL"), vec![3]);
}

#[test]
fn ifelse_format_pretty() {
	let query = syn::parse("IF 1 { 1 } ELSE IF 2 { 2 }").unwrap();
	assert_eq!(query.to_sql(), "IF 1 { 1 } ELSE IF 2 { 2 };");
	// Single-statement blocks stay inline even in pretty mode
	assert_eq!(query.to_sql_pretty(), "IF 1 { 1 } ELSE IF 2 { 2 };");
}

#[test]
fn set_check_type() {
	let query = syn::parse("LET $param = 5").unwrap();
	assert_eq!(query.to_sql(), "LET $param = 5;");

	let query = syn::parse("LET $param: number = 5").unwrap();
	assert_eq!(query.to_sql(), "LET $param: number = 5;");
}

#[rstest]
#[case::any(Kind::Any)]
#[case::none(Kind::None)]
#[case::null(Kind::Null)]
#[case::bool(Kind::Bool)]
#[case::bytes(Kind::Bytes)]
#[case::datetime(Kind::Datetime)]
#[case::decimal(Kind::Decimal)]
#[case::duration(Kind::Duration)]
#[case::float(Kind::Float)]
#[case::int(Kind::Int)]
#[case::number(Kind::Number)]
#[case::object(Kind::Object)]
#[case::string(Kind::String)]
#[case::uuid(Kind::Uuid)]
#[case::regex(Kind::Regex)]
#[case::range(Kind::Range)]
#[case::table(Kind::Table(vec!["users".into()]))]
#[case::record(Kind::Record(vec!["users".into()]))]
#[case::geometry(Kind::Geometry(vec![GeometryKind::Point]))]
#[case::set(Kind::Set(Box::new(Kind::String), None))]
#[case::array(Kind::Array(Box::new(Kind::String), None))]
#[case::either(Kind::Either(vec![Kind::String, Kind::Int]))]
#[case::file(Kind::File(vec!["bucket".to_string()]))]
#[case::named(Kind::Named("mood".to_string()))]
#[case::named_escaped(Kind::Named("a type".to_string()))]
#[case::enum_inline(Kind::Enum(vec!["a".to_string(), "it's".to_string(), "x y".to_string()]))]
#[case::named_option(Kind::Either(vec![Kind::None, Kind::Named("m".to_string())]))]
#[case::array_of_enum(Kind::Array(Box::new(Kind::Enum(vec!["a".to_string()])), None))]
fn kind_canonical_text_round_trips(#[case] kind: Kind) {
	assert_eq!(kind, kind_for_definition(&kind.to_sql()).unwrap());
	assert_eq!(kind, kind_for_definition(&kind.to_sql_pretty()).unwrap());
}

/// Parses `query` with every experimental capability on, so statements behind a
/// stability gate are reachable.
fn parse_experimental(query: &str) -> surrealdb_expr::parser::Query {
	use crate::dbs::Capabilities;
	use crate::dbs::capabilities::Targets;
	syn::parse_with_capabilities(
		query,
		&Capabilities::all().with_experimental(Targets::All),
		&crate::syn::ParserConfig::default(),
	)
	.unwrap()
}

/// A definition's name must survive being rendered and read back.
///
/// These names are always static, so each is rendered by an escaper rather than
/// by the expression printer. A name needing quotes that is emitted bare
/// produces text that either fails to parse or parses to a different name, so
/// the round-trip is the property worth pinning rather than any single expected
/// spelling.
fn assert_name_round_trips(query: &str) {
	let once = parse_experimental(query);
	let rendered = once.to_sql();
	let twice = parse_experimental(&rendered);
	assert_eq!(
		rendered,
		twice.to_sql(),
		"rendering {query:?} produced {rendered:?}, which reads back as something else"
	);
	assert_eq!(once, twice, "{query:?} did not survive a render/reparse round trip");
}

#[rstest]
#[case::function_plain("DEFINE FUNCTION fn::greet() {}")]
#[case::function_reserved("DEFINE FUNCTION fn::`select`() {}")]
#[case::function_spaced("DEFINE FUNCTION fn::`my fn`() {}")]
#[case::param_plain("DEFINE PARAM $limit VALUE 1")]
#[case::type_union("DEFINE TYPE mood AS 'a' | 'b'")]
#[case::type_escaped(
	"DEFINE TYPE IF NOT EXISTS `my mood` AS 'it\\'s' | 'b' COMMENT 'c' PERMISSIONS NONE"
)]
#[case::type_alter_add(
	"ALTER TYPE mood ADD VALUE IF NOT EXISTS 'c' BEFORE 'a' ADD VALUE 'd' AFTER 'b'"
)]
#[case::type_alter_rename("ALTER TYPE IF EXISTS mood RENAME VALUE 'a' TO 'z'")]
#[case::type_remove("REMOVE TYPE IF EXISTS mood")]
#[case::type_clauses(
	"DEFINE TYPE OVERWRITE `my type` AS mood | none CONSTRAINT `not sad` ASSERT $value != 'sad' ASSERT string::len($value) > 0 DEFAULT 'ok' SQL TYPE POSTGRESQL 'mood'"
)]
#[case::type_object("DEFINE TYPE address AS { street: none | null | string, `select`: int }")]
#[case::type_remove_escaped("REMOVE TYPE `my type`")]
#[case::param_reserved("DEFINE PARAM $`select` VALUE 1")]
#[case::param_spaced("DEFINE PARAM $`my param` VALUE 1")]
#[case::param_digit_leading("DEFINE PARAM $`1st` VALUE 1")]
fn a_definition_name_survives_render_and_reparse(#[case] query: &str) {
	assert_name_round_trips(query);
}

/// A type needs its body, takes a constraint name only before an `ASSERT`, a built-in name is
/// not a type's, and `enum<name>` is not a kind.
#[rstest]
#[case::without_body("DEFINE TYPE t")]
#[case::without_kind("DEFINE TYPE t AS")]
#[case::constraint_without_assert("DEFINE TYPE t AS int CONSTRAINT c")]
#[case::other_dialect("DEFINE TYPE t AS int SQL TYPE MYSQL 'int'")]
#[case::builtin_name("DEFINE TYPE string AS int")]
#[case::enum_of_a_name("DEFINE FIELD f ON t TYPE enum<mood>")]
#[case::enum_of_a_number("DEFINE FIELD f ON t TYPE enum<1, 2>")]
fn an_incomplete_type_is_refused(#[case] query: &str) {
	use crate::dbs::Capabilities;
	use crate::dbs::capabilities::Targets;
	assert!(
		syn::parse_with_capabilities(
			query,
			&Capabilities::all().with_experimental(Targets::All),
			&crate::syn::ParserConfig::default(),
		)
		.is_err(),
		"{query:?} parsed"
	);
}

/// `DEFINE MODULE` is behind the `surrealism` cargo feature; without it the
/// parser rejects the statement outright.
#[cfg(feature = "surrealism")]
#[rstest]
#[case::plain("DEFINE MODULE mod::helpers FROM f\"test:/demo.surli\" UNSIGNED")]
#[case::reserved("DEFINE MODULE mod::`select` FROM f\"test:/demo.surli\" UNSIGNED")]
#[case::spaced("DEFINE MODULE mod::`my module` FROM f\"test:/demo.surli\" UNSIGNED")]
#[case::digit_leading("DEFINE MODULE mod::`1st` FROM f\"test:/demo.surli\" UNSIGNED")]
#[case::backtick("DEFINE MODULE mod::`odd\\`name` FROM f\"test:/demo.surli\" UNSIGNED")]
#[case::silo("DEFINE MODULE mod::helpers FROM silo::acme::pkg::<1.0.0> UNSIGNED")]
fn a_module_name_survives_render_and_reparse(#[case] query: &str) {
	assert_name_round_trips(query);
}
