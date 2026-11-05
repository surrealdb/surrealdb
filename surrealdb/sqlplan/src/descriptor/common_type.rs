//! The type a result is described by when its values may differ in type: `CASE`, `COALESCE`, `IF`,
//! `GREATEST` and the like produce a value of one of several alternatives, and a client reads
//! every value of a column as the column's announced type.

use super::SqlType;
use crate::ir::{BoundExpr, BoundLiteral};

/// Whether `expr` is the literal `NULL`, which has no value of its own and so takes the type of
/// whatever it is an alternative to.
pub(super) fn is_null_literal(expr: &BoundExpr) -> bool {
	matches!(expr, BoundExpr::Literal(BoundLiteral::Null))
}

/// The number types in the order PostgreSQL converts them implicitly: a value of one converts to
/// any later one, so an expression over numbers of several types has the latest of them.
const NUMBER_TYPES: [SqlType; 6] = [
	SqlType::Int2,
	SqlType::Int4,
	SqlType::Int8,
	SqlType::Numeric,
	SqlType::Float4,
	SqlType::Float8,
];

impl SqlType {
	/// The type every value of any of `alternatives` has.
	///
	/// - Alternatives of one type are described by that type.
	/// - Alternatives that are all numbers but not of one type are described by the latest of their
	///   types in [`NUMBER_TYPES`]: `Int2`, `Int4`, `Int8`, `Numeric`, `Float4`, `Float8`. A client
	///   can read each of them as that type, and it is the one PostgreSQL gives such an expression.
	/// - Alternatives that are `cidr` and `inet` are described by `inet`, which a `cidr` converts
	///   to implicitly.
	/// - Any other mixture, an alternative of unknown type, or no alternative at all is `Unknown`,
	///   which a client reads as text. A text column holds a value of any kind.
	///
	/// The result does not depend on the order of the alternatives.
	pub(crate) fn common(alternatives: impl IntoIterator<Item = SqlType>) -> SqlType {
		let mut alternatives = alternatives.into_iter();
		let Some(first) = alternatives.next() else {
			return Self::Unknown;
		};
		alternatives.fold(first, Self::widen)
	}

	/// The type every value of `self` and of `other` has.
	fn widen(self, other: SqlType) -> SqlType {
		if self == other {
			return self;
		}
		// A `cidr` converts to `inet` implicitly, so the two networks have `inet` in common.
		if matches!((self, other), (SqlType::Cidr, SqlType::Inet) | (SqlType::Inet, SqlType::Cidr))
		{
			return SqlType::Inet;
		}
		// A `UInt8` takes its place among the numbers as `Numeric`, the type that holds both it and
		// every signed integer, so a mixture with one of them is `Numeric`.
		let as_number = |ty: SqlType| {
			if ty == SqlType::UInt8 {
				SqlType::Numeric
			} else {
				ty
			}
		};
		let position = |ty: SqlType| NUMBER_TYPES.iter().position(|number| *number == ty);
		match (position(as_number(self)), position(as_number(other))) {
			(Some(own), Some(theirs)) if own >= theirs => as_number(self),
			(Some(_), Some(_)) => as_number(other),
			(None, _) | (_, None) => Self::Unknown,
		}
	}
}

#[cfg(test)]
mod tests {
	use SqlType::*;

	use super::*;
	use crate::catalog::MemoryCatalog;
	use crate::dialect::{DialectPolicy, SqlDialect};
	use crate::functions::FunctionRegistry;
	use crate::ir::BoundSelect;

	const SAMPLE: [SqlType; 9] = [Unknown, Bool, Int2, Int4, Int8, Float4, Float8, Numeric, Text];

	#[test]
	fn a_u64_mixed_with_signed_integers_is_described_by_numeric() {
		assert_eq!(SqlType::common([UInt8, UInt8]), UInt8);
		assert_eq!(SqlType::common([UInt8, Int8]), Numeric);
		assert_eq!(SqlType::common([Int4, UInt8]), Numeric);
		assert_eq!(SqlType::common([Numeric, UInt8]), Numeric);
		assert_eq!(SqlType::common([UInt8, Float8]), Float8);
		assert_eq!(SqlType::common([UInt8, Text]), Unknown);
	}

	#[test]
	fn alternatives_of_one_type_are_described_by_it() {
		for ty in SAMPLE {
			assert_eq!(SqlType::common([ty]), ty);
			assert_eq!(SqlType::common([ty, ty, ty]), ty);
		}
		assert_eq!(SqlType::common([Array(&Int8), Array(&Int8)]), Array(&Int8));
	}

	#[test]
	fn numbers_of_different_types_are_described_by_the_type_postgresql_gives() {
		// The type PostgreSQL 17 gives `CASE`, `COALESCE`, `GREATEST` and `LEAST` over two numbers.
		let order = [Int2, Int4, Int8, Numeric, Float4, Float8];
		let expected = [
			[Int2, Int4, Int8, Numeric, Float4, Float8],
			[Int4, Int4, Int8, Numeric, Float4, Float8],
			[Int8, Int8, Int8, Numeric, Float4, Float8],
			[Numeric, Numeric, Numeric, Numeric, Float4, Float8],
			[Float4, Float4, Float4, Float4, Float4, Float8],
			[Float8, Float8, Float8, Float8, Float8, Float8],
		];
		for (row, a) in order.into_iter().enumerate() {
			for (column, b) in order.into_iter().enumerate() {
				assert_eq!(SqlType::common([a, b]), expected[row][column], "{a:?} {b:?}");
			}
		}
		assert_eq!(SqlType::common([Int2, Int2, Int4]), Int4);
		assert_eq!(SqlType::common([Int2, Float4, Int8]), Float4);
	}

	#[test]
	fn anything_else_is_described_as_unknown() {
		assert_eq!(SqlType::common([Text, Int8]), Unknown);
		assert_eq!(SqlType::common([Bool, Int8]), Unknown);
		assert_eq!(SqlType::common([Int8, Unknown]), Unknown);
		assert_eq!(SqlType::common([Unknown, Float8, Float8]), Unknown);
		assert_eq!(SqlType::common([Text, Varchar]), Unknown);
		assert_eq!(SqlType::common([Array(&Int8), Array(&Text)]), Unknown);
		assert_eq!(SqlType::common([]), Unknown);
	}

	#[test]
	fn the_order_of_the_alternatives_does_not_matter() {
		for a in SAMPLE {
			for b in SAMPLE {
				assert_eq!(SqlType::common([a, b]), SqlType::common([b, a]), "{a:?} {b:?}");
				for c in SAMPLE {
					let expected = SqlType::common([a, b, c]);
					for permutation in [[a, c, b], [b, a, c], [b, c, a], [c, a, b], [c, b, a]] {
						assert_eq!(SqlType::common(permutation), expected, "{a:?} {b:?} {c:?}");
					}
				}
			}
		}
	}

	#[test]
	fn a_function_is_described_by_the_type_its_results_have() {
		let infer = |name, args: &[SqlType]| FunctionRegistry::infer_return_type_any(name, args);
		assert_eq!(infer("floor", &[Float8]), Float8);
		assert_eq!(infer("floor", &[Float4]), Float8);
		assert_eq!(infer("floor", &[Int8]), Int8);
		assert_eq!(infer("floor", &[Numeric]), Numeric);
		assert_eq!(infer("ceil", &[Float8]), Float8);
		assert_eq!(infer("ceiling", &[Numeric]), Numeric);
		assert_eq!(infer("greatest", &[Int8, Float8]), Float8);
		assert_eq!(infer("least", &[Int8, Numeric]), Numeric);
		assert_eq!(infer("least", &[Int8, Text]), Unknown);
		assert_eq!(infer("coalesce", &[Int8, Float8]), Float8);
		assert_eq!(infer("coalesce", &[Float8, Int8, Numeric]), Float8);
		assert_eq!(infer("coalesce", &[Float4, Int8]), Float4);
		assert_eq!(infer("ifnull", &[Int8, Int8]), Int8);
		assert_eq!(infer("ifnull", &[Text, Int8]), Unknown);
		assert_eq!(infer("if", &[Bool, Int8, Float8]), Float8);
		assert_eq!(infer("if", &[Int8, Text, Text]), Text);
		// The type of what the first argument is compared with does not matter for the result.
		assert_eq!(infer("nullif", &[Text, Int8]), Text);
	}

	#[test]
	fn a_null_literal_has_every_type() {
		let infer = |name, args: &[Option<SqlType>]| {
			FunctionRegistry::infer_return_type_of_arguments(name, args)
		};
		assert_eq!(infer("coalesce", &[None, Some(Float8)]), Float8);
		assert_eq!(infer("coalesce", &[Some(Int8), None, Some(Float8)]), Float8);
		assert_eq!(infer("coalesce", &[None, None]), Unknown);
		assert_eq!(infer("if", &[Some(Bool), Some(Int8), None]), Int8);
		assert_eq!(infer("if", &[None, Some(Int8), Some(Float8)]), Float8);
		assert_eq!(infer("if", &[Some(Bool), None, None]), Unknown);
		assert_eq!(infer("greatest", &[None, Some(Int8)]), Int8);
		assert_eq!(infer("floor", &[None]), Unknown);
	}

	fn described(dialect: SqlDialect, select: &str, catalog: &MemoryCatalog) -> Vec<SqlType> {
		let (bound, policy): (BoundSelect, DialectPolicy) = match dialect {
			SqlDialect::Postgres => {
				let statements = surrealdb_pgsql::parse_statements(select).expect("parse");
				let surrealdb_pgsql::ast::stmt::Statement::Select(stmt) = &statements[0] else {
					panic!("expected SELECT");
				};
				let policy = DialectPolicy::postgres();
				(
					crate::binder::pgsql::bind_select(stmt, catalog, &policy, None).expect("bind"),
					policy,
				)
			}
			SqlDialect::MySql => {
				let statements = surrealdb_mysql::parse_statements(select).expect("parse");
				let surrealdb_mysql::ast::stmt::Statement::Select(stmt) = &statements[0] else {
					panic!("expected SELECT");
				};
				let policy = DialectPolicy::mysql();
				(
					crate::binder::mysql::bind_select(stmt, catalog, &policy, None).expect("bind"),
					policy,
				)
			}
		};
		bound.describe(catalog, &policy).columns.iter().map(|column| column.ty).collect()
	}

	fn types(dialect: SqlDialect, select: &str) -> Vec<SqlType> {
		described(dialect, select, &MemoryCatalog::new())
	}

	#[test]
	fn an_integer_column_and_a_constant_add_up_to_an_integer_in_postgresql_only() {
		let catalog = MemoryCatalog::new()
			.with_column_type("t", "i", Int4)
			.with_column_type("t", "s", Int2)
			.with_column_type("t", "b", Int8)
			.with_column_type("t", "r", Float4);
		let sql = "SELECT i + 1, s + 1, s + s, i + b, i * 2 + 1, 2 * 3, i + (-1), -(i + 1), r + r, r + 1, i + CASE WHEN true THEN 1 ELSE 2 END FROM t";
		assert_eq!(
			described(SqlDialect::Postgres, sql, &catalog),
			[Int4, Int4, Int2, Int8, Int4, Int4, Int4, Int4, Float4, Float8, Int4]
		);
		let mysql = described(SqlDialect::MySql, "SELECT i + 1 FROM t", &catalog);
		assert_eq!(mysql, [Int8]);
	}

	#[test]
	fn a_column_of_a_lateral_relation_has_the_type_of_its_expression() {
		let types = types(
			SqlDialect::Postgres,
			"SELECT l.x, l.y FROM pg_class c CROSS JOIN LATERAL (SELECT 1::int4 AS x, 'a'::text AS y) l",
		);
		assert_eq!(types, [Int4, Text]);
	}

	#[test]
	fn a_choice_among_narrow_operands_is_a_narrow_operand_in_postgresql() {
		let catalog = MemoryCatalog::new()
			.with_column_type("t", "i", Int4)
			.with_column_type("t", "s", Int2)
			.with_column_type("t", "b", Int8);
		let sql = "SELECT i + COALESCE(1, 2), i + NULLIF(1, 2), i + GREATEST(1, NULL, 2), \
			i + LEAST(s, 3), s + COALESCE(s, s), i + COALESCE(b, 1) FROM t";
		assert_eq!(
			described(SqlDialect::Postgres, sql, &catalog),
			[Int4, Int4, Int4, Int4, Int2, Int8]
		);
	}

	#[test]
	fn a_mysql_sum_of_a_float_column_is_a_double() {
		let catalog = MemoryCatalog::new().with_column_type("t", "f", Float4);
		assert_eq!(described(SqlDialect::MySql, "SELECT SUM(f) FROM t", &catalog), [Float8]);
		assert_eq!(
			described(SqlDialect::MySql, "SELECT SUM(f) OVER (PARTITION BY f) FROM t", &catalog),
			[Float8]
		);
		assert_eq!(described(SqlDialect::Postgres, "SELECT SUM(f) FROM t", &catalog), [Float4]);
	}

	#[test]
	fn a_sum_of_narrow_integers_is_a_bigint_in_both_dialects() {
		let catalog =
			MemoryCatalog::new().with_column_type("t", "i", Int4).with_column_type("t", "s", Int2);
		for dialect in [SqlDialect::Postgres, SqlDialect::MySql] {
			let sql = "SELECT SUM(i), SUM(s), SUM(i) OVER (PARTITION BY s) FROM t";
			assert_eq!(described(dialect, sql, &catalog), [Int8, Int8, Int8], "{dialect:?}");
		}
	}

	#[test]
	fn a_case_is_described_by_the_type_all_its_branches_have() {
		for dialect in [SqlDialect::Postgres, SqlDialect::MySql] {
			assert_eq!(
				types(
					dialect,
					"SELECT CASE WHEN true THEN 1 ELSE 2.5 END, CASE WHEN true THEN 1 ELSE 2 END"
				),
				[
					Numeric,
					if dialect == SqlDialect::Postgres {
						Int4
					} else {
						Int8
					}
				],
				"{dialect:?}"
			);
			assert_eq!(
				types(
					dialect,
					"SELECT CASE WHEN true THEN 1 ELSE 'x' END, CASE WHEN true THEN 'x' END"
				),
				[Unknown, Text],
				"{dialect:?}"
			);
			assert_eq!(
				types(dialect, "SELECT CASE 1 WHEN 1 THEN 1.5 WHEN 2 THEN 2 ELSE 3 END"),
				[Numeric],
				"{dialect:?}"
			);
		}
	}

	#[test]
	fn a_null_branch_does_not_change_the_type_of_the_others() {
		for dialect in [SqlDialect::Postgres, SqlDialect::MySql] {
			assert_eq!(
				types(
					dialect,
					"SELECT CASE WHEN true THEN 1 ELSE NULL END, CASE WHEN true THEN NULL ELSE 1.5 END, \
					 CASE WHEN true THEN NULL ELSE NULL END, coalesce(NULL, 2.5)"
				),
				[Int8, Numeric, Unknown, Numeric],
				"{dialect:?}"
			);
		}
		assert_eq!(
			types(SqlDialect::MySql, "SELECT IF(true, 1, NULL), IF(true, NULL, 2.5)"),
			[Int8, Numeric]
		);
	}

	#[test]
	fn a_function_call_is_described_by_the_type_its_results_have() {
		for dialect in [SqlDialect::Postgres, SqlDialect::MySql] {
			assert_eq!(
				types(
					dialect,
					"SELECT floor(1.5), ceil(1.5), floor(1), greatest(1, 2.5), least(2, 1.5), \
					 coalesce(1, 2.5), greatest(1, 2)"
				),
				[Numeric, Numeric, Int8, Numeric, Numeric, Numeric, Int8],
				"{dialect:?}"
			);
		}
		assert_eq!(
			types(SqlDialect::MySql, "SELECT ifnull(1, 2.5), IF(true, 1, 2.5), IF(true, 1, 'x')"),
			[Numeric, Numeric, Unknown]
		);
	}

	#[test]
	fn the_columns_of_a_table_are_described_by_their_declared_types() {
		let catalog = MemoryCatalog::new()
			.with_column_type("t", "i", Int4)
			.with_column_type("t", "n", Numeric)
			.with_column_type("t", "f", Float8)
			.with_column_type("t", "s", Text);
		for (select, expected) in [
			("SELECT CASE WHEN i > 1 THEN i ELSE n END FROM t", Numeric),
			("SELECT CASE WHEN i > 1 THEN n ELSE f END FROM t", Float8),
			("SELECT CASE WHEN i > 1 THEN i ELSE s END FROM t", Unknown),
			("SELECT coalesce(i, n) FROM t", Numeric),
			("SELECT greatest(i, f) FROM t", Float8),
			("SELECT floor(n) FROM t", Numeric),
			("SELECT floor(f) FROM t", Float8),
			("SELECT ceil(i) FROM t", Int4),
			("SELECT SUM(CASE WHEN i > 1 THEN i ELSE NULL END) FROM t", Int8),
		] {
			assert_eq!(described(SqlDialect::Postgres, select, &catalog), [expected], "{select}");
		}
	}

	#[test]
	fn a_case_nested_within_the_parser_limit_is_described() {
		for dialect in [SqlDialect::Postgres, SqlDialect::MySql] {
			for depth in [10usize, 50, 100] {
				let mut case = "2.5".to_string();
				for _ in 0..depth {
					case = format!("CASE WHEN true THEN 1 ELSE {case} END");
				}
				assert_eq!(
					types(dialect, &format!("SELECT {case}")),
					[Numeric],
					"{dialect:?} {depth}"
				);
			}
		}
	}
}
