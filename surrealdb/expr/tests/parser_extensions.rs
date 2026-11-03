//! The extension statements and calls. Every spelling of a definition lowers
//! to the same tree, which renders as `EXTENSION` and `ext::`. A call keeps the
//! prefix it is written with, `ext::` or `mod::`, in how it renders and in its
//! implicit field name; both prefixes call the same extension.

use surrealdb_expr::parser::{self, deprecated_extension_spellings};
use surrealdb_parser_next::ast::ext::{MOD_PREFIX, MODULE_KEYWORD, SILO_NAME};
use surrealdb_types::ToSql;

fn rendered(src: &str) -> String {
	parser::parse(src).unwrap_or_else(|e| panic!("{src}: {e:?}")).to_sql()
}

#[test]
fn every_spelling_renders_as_extension() {
	let file = r#"f"b:/m.wasm" UNSIGNED"#;
	let canonical = rendered(&format!("DEFINE EXTENSION ext::a FROM {file}"));
	assert!(canonical.starts_with("DEFINE EXTENSION ext::a FROM"), "{canonical}");
	for spelling in [
		format!("DEFINE MODULE ext::a FROM {file}"),
		format!("DEFINE EXTENSION mod::a FROM {file}"),
		format!("DEFINE MODULE mod::a FROM {file}"),
	] {
		assert_eq!(rendered(&spelling), canonical, "{spelling}");
	}
	let silo = rendered("DEFINE MODULE mod::p FROM silo::o::p::<1.2.3> UNSIGNED");
	assert!(silo.starts_with("DEFINE EXTENSION ext::p FROM silo::o::p<1.2.3>"), "{silo}");

	assert_eq!(
		rendered("ALTER MODULE mod::a COMMENT 'c'"),
		rendered("ALTER EXTENSION ext::a COMMENT 'c'")
	);
	assert_eq!(rendered("REMOVE MODULE mod::a"), "REMOVE EXTENSION ext::a;");
	// An unnamed silo definition an earlier release wrote is still addressed
	// by its coordinates.
	assert_eq!(
		rendered("REMOVE EXTENSION silo::o::p::<1.2.3>"),
		"REMOVE EXTENSION silo::o::p<1.2.3>;"
	);
}

#[test]
fn calls_keep_their_prefix() {
	assert_eq!(rendered("RETURN ext::a::f(1)"), "RETURN ext::a::f(1);");
	assert_eq!(rendered("RETURN mod::a::f(1)"), "RETURN mod::a::f(1);");
	assert_eq!(rendered("RETURN ext::a(1)"), "RETURN ext::a(1);");
	assert_eq!(rendered("RETURN mod::a(1)"), "RETURN mod::a(1);");
}

/// The key a projection of a call answers under when it has no alias. A query
/// written against `mod::` keeps reading the key it was written to produce.
#[test]
fn a_call_is_projected_under_its_written_prefix() {
	use surrealdb_expr::expr::{Expr, Function};

	for (call, key) in [
		("ext::demo::f(v)", "ext::demo::f"),
		("mod::demo::f(v)", "mod::demo::f"),
		("ext::demo(v)", "ext::demo"),
		("mod::demo(v)", "mod::demo"),
	] {
		let Expr::FunctionCall(c) = parser::expr(call).unwrap_or_else(|e| panic!("{e:?}")) else {
			panic!("{call} is not a call");
		};
		assert!(matches!(c.receiver, Function::Module(..)), "{call}");
		assert_eq!(c.receiver.to_idiom().to_sql(), format!("`{key}`"), "{call}");
	}
}

#[test]
fn an_analyzer_function_renders_as_ext() {
	let analyzer = rendered("DEFINE ANALYZER x FUNCTION mod::d::lower TOKENIZERS class");
	assert_eq!(analyzer, rendered("DEFINE ANALYZER x FUNCTION ext::d::lower TOKENIZERS class"),);
	assert!(analyzer.contains("FUNCTION ext::d::lower"), "{analyzer}");
}

fn import(src: &str) -> String {
	let settings = parser::ParserSettings {
		surrealism_enabled: true,
		import: true,
		..Default::default()
	};
	parser::query_with_settings(src, settings).unwrap_or_else(|e| panic!("{src}: {e:?}")).to_sql()
}

/// An import reads the forms an export or a 3.x dump writes: an unnamed silo
/// definition, `AS` in place of `FROM`, and a definition without `UNSIGNED`.
#[test]
fn an_import_reads_the_legacy_definition_forms() {
	assert_eq!(
		import("DEFINE EXTENSION silo::o::p<1.2.3> UNSIGNED"),
		"DEFINE EXTENSION silo::o::p<1.2.3> UNSIGNED CAPABILITIES {} PERMISSIONS FULL;"
	);
	assert_eq!(
		import("DEFINE MODULE silo::acme::w<1.0.0> PERMISSIONS FULL"),
		"DEFINE EXTENSION silo::acme::w<1.0.0> CAPABILITIES {} PERMISSIONS FULL;"
	);
	assert_eq!(
		import(r#"DEFINE MODULE mod::x AS f"b:/x.surli" PERMISSIONS FULL"#),
		r#"DEFINE EXTENSION ext::x FROM f"b:/x.surli" CAPABILITIES {} PERMISSIONS FULL;"#
	);
	assert!(parser::parse(r#"DEFINE MODULE mod::x AS f"b:/x.surli""#).is_err());
}

/// `UNSIGNED` and `SIGNER` are optional in the grammar; the executor refuses
/// a definition with neither outside an import. A parsed definition always
/// records `CAPABILITIES`.
#[test]
fn unsigned_is_optional_in_the_grammar() {
	assert_eq!(
		rendered(r#"DEFINE EXTENSION ext::x FROM f"b:/x.surli""#),
		r#"DEFINE EXTENSION ext::x FROM f"b:/x.surli" CAPABILITIES {} PERMISSIONS FULL;"#
	);
}

/// A silo version is written `<v>` or `::<v>` after the package, and renders
/// as `<v>`.
#[test]
fn a_silo_version_is_written_either_way() {
	for (a, b) in [
		(
			"DEFINE EXTENSION ext::p FROM silo::o::p<1.2.3> UNSIGNED",
			"DEFINE EXTENSION ext::p FROM silo::o::p::<1.2.3> UNSIGNED",
		),
		("RETURN silo::o::p<1.2.3>(1)", "RETURN silo::o::p::<1.2.3>(1)"),
		("RETURN silo::o::p<1.2.3>::f(1)", "RETURN silo::o::p::<1.2.3>::f(1)"),
		("REMOVE EXTENSION silo::o::p<1.2.3>", "REMOVE EXTENSION silo::o::p::<1.2.3>"),
		(
			"ALTER EXTENSION silo::o::p<1.2.3> COMMENT 'c'",
			"ALTER EXTENSION silo::o::p::<1.2.3> COMMENT 'c'",
		),
	] {
		let canonical = rendered(a);
		assert_eq!(canonical, rendered(b), "{a}");
		assert!(canonical.contains("silo::o::p<1.2.3>"), "{canonical}");
	}
}

#[test]
fn unnamed_definitions_are_refused() {
	for src in [
		"DEFINE EXTENSION silo::o::p::<1.2.3> UNSIGNED",
		"DEFINE MODULE silo::o::p<1.2.3> UNSIGNED",
		r#"DEFINE EXTENSION f"b:/m.wasm" UNSIGNED"#,
	] {
		assert!(parser::parse(src).is_err(), "{src}");
	}
}

#[test]
fn each_deprecated_spelling_has_one_warning() {
	assert_eq!(deprecated_extension_spellings(0).count(), 0);
	for bit in [MODULE_KEYWORD, MOD_PREFIX, SILO_NAME] {
		assert_eq!(deprecated_extension_spellings(bit).count(), 1, "{bit}");
	}
	let all: Vec<_> =
		deprecated_extension_spellings(MODULE_KEYWORD | MOD_PREFIX | SILO_NAME).collect();
	assert_eq!(all.len(), 3);
	assert!(all[0].contains("`MODULE`") && all[1].contains("`mod::`") && all[2].contains("silo::"));
}
