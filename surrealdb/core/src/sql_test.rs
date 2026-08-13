//! Tests for the `surrealdb-sql` AST that need layers sitting above it.
//!
//! `surrealdb-sql` is the bottom of the language stack, so it cannot reach the
//! parser or the expression layer even as a dev-dependency without inverting
//! the crate graph. Cases that assert round-tripping through either of those
//! live here instead.

use rstest::rstest;
use surrealdb_types::ToSql;

use crate::sql::Kind;
use crate::sql::kind::GeometryKind;
use crate::syn;

/// Statement counts per execution unit for a parsed query.
fn unit_sizes(query: &str) -> Vec<usize> {
	syn::parse(query)
		.unwrap()
		.into_execution_units()
		.iter()
		.map(crate::sql::Ast::num_statements)
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
fn test_kind_conversions_expr(#[case] sql_kind: Kind) {
	let expr_kind: crate::expr::Kind = sql_kind.clone().into();
	let back_to_sql: Kind = expr_kind.into();
	assert_eq!(sql_kind, back_to_sql);
}

/// Parses `query` with every experimental capability on, so statements behind a
/// stability gate are reachable.
fn parse_experimental(query: &str) -> crate::sql::Ast {
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
#[case::param_reserved("DEFINE PARAM $`select` VALUE 1")]
#[case::param_spaced("DEFINE PARAM $`my param` VALUE 1")]
#[case::param_digit_leading("DEFINE PARAM $`1st` VALUE 1")]
fn a_definition_name_survives_render_and_reparse(#[case] query: &str) {
	assert_name_round_trips(query);
}

/// `DEFINE MODULE` is behind the `surrealism` cargo feature; without it the
/// parser rejects the statement outright.
#[cfg(feature = "surrealism")]
#[rstest]
#[case::plain("DEFINE MODULE mod::helpers AS f\"test:/demo.surli\" UNSIGNED")]
#[case::reserved("DEFINE MODULE mod::`select` AS f\"test:/demo.surli\" UNSIGNED")]
#[case::spaced("DEFINE MODULE mod::`my module` AS f\"test:/demo.surli\" UNSIGNED")]
#[case::digit_leading("DEFINE MODULE mod::`1st` AS f\"test:/demo.surli\" UNSIGNED")]
#[case::backtick("DEFINE MODULE mod::`odd\\`name` AS f\"test:/demo.surli\" UNSIGNED")]
#[case::silo("DEFINE MODULE mod::helpers AS silo::acme::pkg::<1.0.0> UNSIGNED")]
fn a_module_name_survives_render_and_reparse(#[case] query: &str) {
	assert_name_round_trips(query);
}
