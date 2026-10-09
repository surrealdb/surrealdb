//! Allocation budgets for warmed stored-expression parsing.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct CountingAllocator;
thread_local! {
	static COUNT: Cell<Option<usize>> = const { Cell::new(None) };
}

fn count() {
	let _ = COUNT.try_with(|c| {
		if let Some(n) = c.get() {
			c.set(Some(n + 1));
		}
	});
}

unsafe impl GlobalAlloc for CountingAllocator {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		count();
		unsafe { System.alloc(layout) }
	}
	unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
		count();
		unsafe { System.alloc_zeroed(layout) }
	}
	unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
		count();
		unsafe { System.realloc(ptr, layout, size) }
	}
	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		unsafe { System.dealloc(ptr, layout) }
	}
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn allocations<T>(f: impl FnOnce() -> T) -> (T, usize) {
	COUNT.with(|c| c.set(Some(0)));
	let value = f();
	let count = COUNT.with(|c| c.replace(None).expect("count enabled"));
	(value, count)
}

#[test]
fn stored_expression_skips_the_surface_tree() {
	use surrealdb_expr::parser;
	for (source, budget) in [
		("$value.name != NONE AND $value.age > 18", 12),
		("{ LET $names = [1, 2, 3]; RETURN $names[WHERE $this > 1] }", 11),
		("(SELECT name, age FROM person WHERE age > 18 ORDER BY age LIMIT 10)", 15),
	] {
		// The arena cache must be warm before charging allocations.
		parser::expr_for_definition(source).unwrap();
		let (direct, direct_count) = allocations(|| parser::expr_for_definition(source).unwrap());
		assert!(!direct.to_stored_sql().is_empty());
		assert!(direct_count <= budget, "allocation budget {budget} exceeded: {direct_count}");
		eprintln!("{source}: {direct_count} allocations");
	}
}

#[test]
fn stored_logical_lowering_allocates_only_the_logical_operators() {
	use surrealdb_expr::expr::logical::lower_expr;
	use surrealdb_expr::parser;
	for (source, budget) in [
		("$value != NONE AND $value > 18", 8),
		("($a + 1) * ($b - 2)", 8),
		("{ LET $x = [1, 2, 3]; RETURN $x }", 7),
	] {
		parser::logical_expr_for_definition(source).unwrap();
		let (direct, count) = allocations(|| parser::logical_expr_for_definition(source).unwrap());
		let (through_runtime, runtime_count) =
			allocations(|| lower_expr(parser::expr_for_definition(source).unwrap()).unwrap());
		assert_eq!(direct, through_runtime);
		assert!(count < runtime_count, "direct: {count}; runtime: {runtime_count}");
		assert!(count <= budget, "allocation budget {budget} exceeded: {count}");
		eprintln!("logical {source}: {count} allocations; runtime: {runtime_count}");
	}
}

#[test]
fn canonical_printing_borrows_the_runtime_tree() {
	use surrealdb_expr::expr::CoverStmts;
	use surrealdb_expr::parser;
	use surrealdb_types::ToSql;
	for (source, budget) in [
		("$value.name != NONE AND $value.age > 18", 5),
		("{ LET $names = [1, 2, 3]; RETURN $names[WHERE $this > 1] }", 8),
		("(SELECT name, age FROM person WHERE age > 18 ORDER BY age LIMIT 10)", 7),
	] {
		let expr = parser::expr_for_definition(source).unwrap();
		let (direct, direct_count) = allocations(|| expr.to_stored_sql());
		assert_eq!(direct, CoverStmts(&expr).to_sql());
		assert!(direct_count <= budget, "print budget {budget} exceeded: {direct_count}");
		eprintln!("printing {source}: {direct_count} allocations");
	}
}

#[test]
fn request_compilation_skips_the_runtime_expression_tree() {
	use surrealdb_expr::expr::logical::lower;
	use surrealdb_expr::parser::{
		self, ExecutionStatement, ParserConfig, ParserSettings, TopLevelExpr,
	};
	let settings = ParserSettings::all_features(&ParserConfig::default());
	for (source, budget) in [
		("SELECT name, age FROM person WHERE age > 18 AND active ORDER BY age LIMIT 10", 23),
		("LET $x = [1 + 2, 3 + 4, 5 + 6]; RETURN $x", 16),
		("RETURN (($a + 1) * ($b - 2)) + ($c * 3)", 18),
	] {
		parser::execution_query_with_settings(source, settings).unwrap();
		let (direct, count) =
			allocations(|| parser::execution_query_with_settings(source, settings).unwrap());
		let (native, native_count) = allocations(|| {
			parser::query_with_settings(source, settings)
				.unwrap()
				.expressions
				.into_iter()
				.map(|statement| {
					let TopLevelExpr::Expr(expr) = statement else {
						panic!("engine statement expected")
					};
					lower(expr).unwrap()
				})
				.collect::<Vec<_>>()
		});
		let direct = direct
			.into_iter()
			.map(|statement| {
				let ExecutionStatement::Logical(statement) = statement else {
					panic!("direct compilation expected")
				};
				statement.plan
			})
			.collect::<Vec<_>>();
		assert_eq!(direct, native);
		assert!(count < native_count, "direct: {count}; runtime: {native_count}");
		assert!(count <= budget, "request allocation budget {budget} exceeded: {count}");
		eprintln!("request {source}: {count} allocations; runtime: {native_count}");
	}
}
