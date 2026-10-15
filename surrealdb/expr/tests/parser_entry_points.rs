//! The single-value entry points: durations, datetimes, table and function
//! names.

use surrealdb_expr::parser;
use surrealdb_expr::parser::{
	ParserConfig, ParserSettings, datetime, duration, function_with_settings, table,
};

#[test]
fn durations() {
	assert_eq!(duration("1h30m").expect("valid").to_string(), "1h30m");
	assert_eq!(duration(" 2w ").expect("valid").to_string(), "2w");
	for bad in ["1h x", "1x", "99999999999999999999y", "'1h'", ""] {
		assert!(duration(bad).is_err(), "{bad:?}");
	}
}

#[test]
fn datetimes() {
	assert!(datetime("2024-01-02T03:04:05Z").is_ok());
	for bad in ["2024-13-01", "2024-01-02T03:04:05+25:00", "x"] {
		let err = datetime(bad).expect_err(bad).to_string();
		assert!(err.contains("datetime") || err.contains("Datetime"), "{bad:?}: {err}");
	}
}

#[test]
fn table_names() {
	assert_eq!(table("person").expect("valid").as_str(), "person");
	assert_eq!(table("`a b`").expect("valid").as_str(), "a b");
	// A keyword names a table, and anything after the name is ignored.
	assert_eq!(table("select").expect("valid").as_str(), "select");
	assert_eq!(table("person rest").expect("valid").as_str(), "person");
	assert!(table("1").is_err());
	assert!(table("").is_err());
}

#[test]
fn function_names() {
	let settings = ParserSettings::all_features(&ParserConfig::default());
	let name = |f: &str| function_with_settings(f, settings).map(|f| format!("{f:?}"));
	assert_eq!(name("string::len").expect("valid"), r#"Normal("string::len")"#);
	assert_eq!(name("fn::a::b").expect("valid"), r#"Custom("a::b")"#);
	assert_eq!(name("fn::`x y`").expect("valid"), r#"Custom("x y")"#);
	assert_eq!(name("ext::m::f").expect("valid"), r#"Module("m", Some("f"), Ext)"#);
	assert_eq!(name("mod::m::f").expect("valid"), r#"Module("m", Some("f"), Mod)"#);
	assert!(name("silo::o::p::<1.2.3>::f").is_ok());
	assert!(name("ml::model<1.2.3>").is_ok());
	assert_eq!(name("count rest").expect("valid"), r#"Normal("count")"#);
	assert!(name("fn").is_err());
	assert!(name("1").is_err());
	// Surrealism modules need the grammar enabled.
	assert!(function_with_settings("mod::m::f", ParserSettings::default()).is_err());
}

/// UTF-16 surrogate pairs in `\u` escapes are JSON; SurrealQL strings reject
/// them.
#[test]
fn surrogate_pairs_are_json_only() {
	let text = r#""\uD83D\uDE00""#;
	assert!(parser::value(text).is_err());
	assert!(parser::expr(text).is_err());
	let json = ParserSettings {
		json_string_escapes: true,
		import: false,
		..Default::default()
	};
	assert!(parser::json_with_settings(text, json).is_ok());
}

/// Duration digits past `u64::MAX` are an error, however many there are.
#[test]
fn overlong_duration_digits_are_an_error() {
	let text = format!("{}y", "8".repeat(90));
	assert!(duration(&text).is_err());
	assert!(parser::parse(&format!("RETURN {text};")).is_err());
}

/// A silo package and a file source lower as what they are, whichever
/// statements precede them.
#[test]
fn module_executables_lower_by_kind() {
	let src = include_str!("../../parser-next/tests/corpus/module_executables.surql");
	let _ = parser::parse(src);
	let ast = parser::parse(
		"DEFINE MODULE mod::test FROM f\"bucket:/module.wasm\" UNSIGNED;\n\
		 DEFINE MODULE mod::pkg FROM silo::acme::pkg::<1.0.0> UNSIGNED;",
	)
	.expect("both forms parse");
	assert_eq!(ast.expressions.len(), 2);
}

/// An escaped path segment is one segment, `::` and all.
#[test]
fn escaped_path_segments_may_hold_path_separators() {
	use surrealdb_types::ToSql;
	for text in ["REMOVE FUNCTION fn::`a::b`", "RETURN fn::`a::b`::c()"] {
		let once = parser::parse(text).expect("valid").to_sql();
		let twice = parser::parse(&once).expect("printed").to_sql();
		assert_eq!(once, twice, "{text}");
	}
	let name = function_with_settings("fn::`a::b`", ParserSettings::default())
		.map(|f| format!("{f:?}"))
		.expect("valid");
	assert_eq!(name, r#"Custom("a::b")"#);
}

/// A character that starts no token is rejected whole, wherever it is read.
#[test]
fn stray_non_ascii_characters_are_rejected() {
	for text in
		["ALTER SYSTEM DROP\u{0BAA}", "SELECT \u{0BAA} FROM t", "a.\u{0BAA}", "RETURN 1\u{0BAA}"]
	{
		assert!(parser::parse(text).is_err(), "{text}");
	}
}

mod id_components {
	use surrealdb_expr::expr::statements::alter::{AlterFieldStatement, AlterKind};
	use surrealdb_expr::expr::statements::{AlterStatement, DefineFieldStatement, DefineStatement};
	use surrealdb_expr::expr::{Expr, KeyNames, KeyNamesError, Kind, KindLiteral};
	use surrealdb_expr::parser::{self, TopLevelExpr};
	use surrealdb_strand::Strand;
	use surrealdb_types::ToSql;

	fn only_expr(text: &str) -> Expr {
		let mut ast = parser::parse(text).unwrap_or_else(|e| panic!("{text}: {e}"));
		assert_eq!(ast.expressions.len(), 1, "{text}");
		match ast.expressions.remove(0) {
			TopLevelExpr::Expr(e) => e,
			_ => panic!("{text}: not an expression statement"),
		}
	}

	fn define_field(text: &str) -> DefineFieldStatement {
		match only_expr(text) {
			Expr::Define(d) => match *d {
				DefineStatement::Field(f) => *f,
				_ => panic!("{text}: not DEFINE FIELD"),
			},
			_ => panic!("{text}: not DEFINE"),
		}
	}

	fn alter_field(text: &str) -> AlterFieldStatement {
		match only_expr(text) {
			Expr::Alter(a) => match *a {
				AlterStatement::Field(f) => *f,
				_ => panic!("{text}: not ALTER FIELD"),
			},
			_ => panic!("{text}: not ALTER"),
		}
	}

	fn names(s: &DefineFieldStatement) -> Vec<&str> {
		s.key_names.as_ref().expect("the id is named").iter().collect()
	}

	fn tuple(kinds: Vec<Kind>) -> Kind {
		Kind::Literal(KindLiteral::Array(kinds))
	}

	fn strands(list: &[&str]) -> Vec<Strand> {
		list.iter().map(Strand::new).collect()
	}

	/// The names of a keyed `TYPE` ride beside the plain tuple kind, and the
	/// statement prints them back.
	#[test]
	fn define_field_id_keeps_its_component_names() {
		let s = define_field("DEFINE FIELD id ON reading TYPE [city: string, time: datetime]");
		assert_eq!(names(&s), ["city", "time"]);
		assert_eq!(s.field_kind, Some(tuple(vec![Kind::String, Kind::Datetime])));
		assert_eq!(
			s.to_sql(),
			"DEFINE FIELD id ON reading TYPE [city: string, time: datetime] PERMISSIONS FULL"
		);
		assert_eq!(define_field(&s.to_sql()), s);
		for kind in ["OVERWRITE", "IF NOT EXISTS"] {
			let s = define_field(&format!("DEFINE FIELD {kind} id ON reading TYPE [a: int,]"));
			assert_eq!(names(&s), ["a"], "{kind}");
		}
		// An escaped `id` is the field `id`.
		assert_eq!(names(&define_field("DEFINE FIELD `id` ON t TYPE [a: int]")), ["a"]);
	}

	/// Names that are keywords or need escaping print escaped and read back as the
	/// same names.
	#[test]
	fn component_names_round_trip_through_their_text() {
		let text = "DEFINE FIELD id ON reading TYPE \
			[value: int, `my city`: string, `NaN`: bool, `1a`: uuid, string: duration]";
		let s = define_field(text);
		assert_eq!(names(&s), ["value", "my city", "NaN", "1a", "string"]);
		let printed = s.to_sql();
		assert!(
			printed.contains(
				"TYPE [`value`: int, `my city`: string, `NaN`: bool, `1a`: uuid, string: duration]"
			),
			"{printed}"
		);
		assert_eq!(define_field(&printed), s);
		let pretty = s.to_sql_pretty();
		assert_eq!(define_field(&pretty), s, "{pretty}");
	}

	/// Each `TYPE` clause replaces the previous one along with its names.
	#[test]
	fn a_later_unnamed_type_clause_clears_the_names() {
		let s = define_field("DEFINE FIELD id ON reading TYPE [a: string] TYPE [string]");
		assert!(s.key_names.is_none());
		assert_eq!(s.field_kind, Some(tuple(vec![Kind::String])));
		let s = define_field("DEFINE FIELD id ON reading TYPE [string] TYPE [a: string]");
		assert_eq!(names(&s), ["a"]);
		// Without a `TYPE` or with an unnamed one, nothing is named.
		assert!(define_field("DEFINE FIELD id ON reading").key_names.is_none());
		assert!(define_field("DEFINE FIELD id ON reading TYPE [string, int]").key_names.is_none());
		assert!(define_field("DEFINE FIELD id ON reading TYPE []").key_names.is_none());
	}

	/// Only the bare field `id` may name components; every other field reads the
	/// text as an ordinary tuple kind and fails expecting a kind name.
	#[test]
	fn other_fields_do_not_take_component_names() {
		for text in [
			"DEFINE FIELD value ON reading TYPE [a: string]",
			"DEFINE FIELD id.x ON reading TYPE [a: string]",
			"DEFINE FIELD $f ON reading TYPE [a: string]",
			"DEFINE FIELD (id) ON reading TYPE [a: string]",
			"ALTER FIELD value ON reading TYPE [a: string]",
		] {
			let err = parser::parse(text).expect_err(text).to_string();
			assert!(err.contains("expected a kind name"), "{text}: {err}");
		}
	}

	/// A named tuple is the whole `TYPE`, never part of a larger kind.
	#[test]
	fn component_names_must_name_the_whole_type() {
		for text in [
			"DEFINE FIELD id ON t TYPE [a: string] | none",
			"DEFINE FIELD id ON t TYPE option<[a: string]>",
			"DEFINE FIELD id ON t TYPE array<[a: string]>",
			"DEFINE FIELD id ON t TYPE [[a: string]]",
			"DEFINE FIELD id ON t TYPE [a: string, b: [c: int]]",
			"ALTER FIELD id ON t TYPE [a: string] | none",
			"RETURN <[a: string]> [1]",
		] {
			assert!(parser::parse(text).is_err(), "{text}");
		}
		let err = parser::parse("DEFINE FIELD id ON t TYPE [a: string] | none")
			.expect_err("a named alternative")
			.to_string();
		assert!(
			err.contains("component names are only allowed when they name the whole TYPE"),
			"{err}"
		);
	}

	#[test]
	fn component_names_are_checked_when_the_statement_is_lowered() {
		let err = |text: &str| parser::parse(text).expect_err(text).to_string();
		assert!(err("DEFINE FIELD id ON t TYPE [a: int, a: int]").contains("is named twice"));
		assert!(err("DEFINE FIELD id ON t TYPE [a: int, `a`: int]").contains("is named twice"));
		for reserved in ["id", "in", "out"] {
			let text = format!("DEFINE FIELD id ON t TYPE [{reserved}: int]");
			assert!(err(&text).contains("is reserved"), "{text}");
		}
		assert!(err("DEFINE FIELD id ON t TYPE [a: int, int]").contains("a component name"));
		assert!(err("DEFINE FIELD id ON t TYPE [a: int b: int]").contains("`,` or `]`"));
		assert!(err("DEFINE FIELD id ON t TYPE [a: int,, b: int]").contains("a component name"));
	}

	/// An `ALTER FIELD` carries the names as a tri-state beside the kind.
	#[test]
	fn alter_field_id_carries_names_as_a_tri_state() {
		let named = alter_field("ALTER FIELD id ON reading TYPE [city: string, time: datetime]");
		assert_eq!(named.kind, AlterKind::Set(tuple(vec![Kind::String, Kind::Datetime])));
		assert!(matches!(&named.key_names, AlterKind::Set(n) if n.len() == 2));
		assert_eq!(alter_field(&named.to_sql()), named);

		let unnamed = alter_field("ALTER FIELD id ON reading TYPE [string, datetime]");
		assert!(matches!(unnamed.kind, AlterKind::Set(_)));
		assert_eq!(unnamed.key_names, AlterKind::Drop);

		let dropped = alter_field("ALTER FIELD id ON reading DROP TYPE");
		assert_eq!(dropped.kind, AlterKind::Drop);
		assert_eq!(dropped.key_names, AlterKind::Drop);

		let untouched = alter_field("ALTER FIELD id ON reading READONLY");
		assert_eq!(untouched.kind, AlterKind::None);
		assert_eq!(untouched.key_names, AlterKind::None);

		// Clauses replay in order.
		let replayed = alter_field("ALTER FIELD id ON reading TYPE [a: string] TYPE [string]");
		assert_eq!(replayed.key_names, AlterKind::Drop);
		let replayed = alter_field("ALTER FIELD id ON reading DROP TYPE TYPE [a: string]");
		assert!(matches!(&replayed.key_names, AlterKind::Set(n) if n.len() == 1));
		let replayed = alter_field("ALTER FIELD id ON reading TYPE [a: string] DROP TYPE");
		assert_eq!(replayed.kind, AlterKind::Drop);
		assert_eq!(replayed.key_names, AlterKind::Drop);
	}

	/// The carriers refuse names that do not match the tuple, and keep their
	/// previous state when they do.
	#[test]
	fn set_type_refuses_names_that_do_not_match_the_tuple() {
		let names = KeyNames::new(strands(&["a", "b"]), 2).expect("two names");
		let two = tuple(vec![Kind::String, Kind::Datetime]);

		let mut s = DefineFieldStatement::default();
		let arity = |kinds| {
			Err(KeyNamesError::Arity {
				names: 2,
				kinds,
			})
		};
		assert_eq!(s.set_type(Some(tuple(vec![Kind::String])), Some(names.clone())), arity(1));
		assert_eq!(s.set_type(Some(Kind::String), Some(names.clone())), arity(0));
		assert_eq!(s.set_type(None, Some(names.clone())), arity(0));
		assert!(s.field_kind.is_none() && s.key_names.is_none());
		assert_eq!(s.set_type(Some(two.clone()), Some(names.clone())), Ok(()));
		assert_eq!(s.field_kind, Some(two.clone()));
		assert_eq!(s.key_names.as_ref(), Some(&names));
		assert_eq!(s.set_type(Some(two.clone()), None), Ok(()));
		assert!(s.key_names.is_none());

		let mut a = AlterFieldStatement::default();
		assert_eq!(
			a.set_type(AlterKind::Set(tuple(vec![Kind::String])), AlterKind::Set(names.clone())),
			arity(1)
		);
		assert_eq!(a.set_type(AlterKind::Drop, AlterKind::Set(names.clone())), arity(0));
		assert_eq!(a.kind, AlterKind::None);
		assert_eq!(a.key_names, AlterKind::None);
		assert_eq!(a.set_type(AlterKind::Set(two), AlterKind::Set(names.clone())), Ok(()));
		assert_eq!(a.key_names, AlterKind::Set(names));
	}

	/// The stored text of an `id` field reads back with its names; the plain kind
	/// entry points never read names.
	#[test]
	fn id_kind_for_definition_reads_component_names() {
		let (kind, names) =
			parser::id_kind_for_definition("[city: string, time: datetime]").expect("keyed");
		assert_eq!(kind, tuple(vec![Kind::String, Kind::Datetime]));
		let names = names.expect("named");
		assert_eq!(names.iter().collect::<Vec<_>>(), ["city", "time"]);

		// Unnamed text reads as a plain kind, without names.
		for text in ["[string, datetime]", "[]", "array<int>", "string"] {
			let (_, names) = parser::id_kind_for_definition(text).expect(text);
			assert!(names.is_none(), "{text}");
		}

		// Escaped, keyword, NaN, digit-leading and spaced names print and read back.
		for text in [
			"[`my city`: string, `1a`: int]",
			"[`NaN`: string, `Infinity`: string]",
			"[`value`: int, `select`: int, option: int]",
			"[`id2`: int, `a\\`b`: int]",
		] {
			let (kind, names) = parser::id_kind_for_definition(text).expect(text);
			let names = names.expect(text);
			let Kind::Literal(KindLiteral::Array(kinds)) = &kind else {
				panic!("{text}: not a tuple");
			};
			let printed = names.fmt_keyed(kinds).to_string();
			let (again, again_names) = parser::id_kind_for_definition(&printed).expect(&printed);
			assert_eq!(again, kind, "{printed}");
			assert_eq!(again_names.as_ref(), Some(&names), "{printed}");
		}

		// The names must be the whole kind, and all of the text is read.
		for text in
			["[a: string] | none", "option<[a: string]>", "[[a: string]]", "[a: string] trailing"]
		{
			assert!(parser::id_kind_for_definition(text).is_err(), "{text}");
		}
	}

	/// Stored text is read whatever its names, so that one bad name cannot make the
	/// whole field list of a table unreadable: the type is the plain tuple, without
	/// names. The same text typed into a statement is refused.
	#[test]
	fn id_kind_for_definition_drops_names_that_are_not_valid() {
		for (text, kinds) in [
			("[a: string, a: int]", vec![Kind::String, Kind::Int]),
			("[a: string, `a`: int]", vec![Kind::String, Kind::Int]),
			("[id: string]", vec![Kind::String]),
			("[in: string, b: int]", vec![Kind::String, Kind::Int]),
			("[x: int, out: int]", vec![Kind::Int, Kind::Int]),
		] {
			let (kind, names) = parser::id_kind_for_definition(text).expect(text);
			assert_eq!(kind, tuple(kinds), "{text}");
			assert!(names.is_none(), "{text}");

			let fresh =
				parser::id_kind_with_settings(text, parser::ParserSettings::default(), true);
			assert!(fresh.is_err(), "{text} is refused when it is not stored text");
		}
		// The text still has to be a kind: only the names are forgiven.
		assert!(parser::id_kind_for_definition("[a: string, a:]").is_err());
	}

	#[test]
	fn plain_kind_entry_points_refuse_component_names() {
		assert!(parser::kind_for_definition("[city: string]").is_err());
		assert!(parser::kind("[city: string, time: datetime]").is_err());
		assert!(parser::kind_for_definition("[string, datetime]").is_ok());
	}
}
