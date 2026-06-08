//! Computed field dependency extraction.
//!
//! Extracts same-table field dependencies from computed field expressions using the
//! expression visitor pattern. This is used for:
//! - Cycle detection at DEFINE FIELD time
//! - Topological ordering of computed field evaluation
//! - Selective computation (only compute fields needed by the query)

use std::collections::{HashMap, HashSet, VecDeque};

use crate::expr::function::{Function, FunctionCall};
use crate::expr::visit::{Visit, Visitor};
use crate::expr::{Expr, Idiom, Literal, Part};

/// Tracing target for permission-analysis warnings emitted from this module.
const TARGET: &str = "surrealdb::core::perms";

/// Dependency metadata for a computed field.
///
/// Tracks which same-table fields a computed expression references, and whether
/// the analysis was able to fully determine all dependencies.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct ComputedDeps {
	/// Known same-table field names this computed field depends on.
	pub fields: Vec<String>,
	/// Whether static analysis could fully determine all dependencies.
	///
	/// When `false`, the expression contains opaque constructs (subqueries, params,
	/// graph traversals, etc.) that could access arbitrary fields at runtime.
	/// If such a field is needed by a query, ALL computed fields must be evaluated.
	pub is_complete: bool,
}

/// Extract computed field dependencies from an expression.
///
/// Walks the expression tree using the visitor pattern to collect all same-table
/// field references. Returns a `ComputedDeps` indicating which fields are referenced
/// and whether the analysis is complete.
pub(crate) fn extract_computed_deps(expr: &Expr) -> ComputedDeps {
	let mut extractor = FieldDependencyExtractor {
		deps: HashSet::new(),
		is_complete: true,
	};
	// Errors from our visitor are `Infallible`, so this cannot fail.
	let _ = extractor.visit_expr(expr);
	ComputedDeps {
		fields: {
			let mut fields: Vec<String> = extractor.deps.into_iter().collect();
			fields.sort();
			fields
		},
		is_complete: extractor.is_complete,
	}
}

/// Extract a static field name from an idiom part, if it represents one.
///
/// Matches `Part::Field("name")` and `Part::Value(Literal::String("name"))` —
/// the latter covers bracket access like `["name"]`.
fn field_name_from_part(part: &Part) -> Option<String> {
	match part {
		Part::Field(name) => Some(name.as_str().to_owned()),
		Part::Value(Expr::Literal(Literal::String(name))) => Some(name.as_str().to_owned()),
		_ => None,
	}
}

/// Visitor that walks an expression tree extracting field dependencies.
struct FieldDependencyExtractor {
	/// Collected same-table field dependencies (root field names only).
	deps: HashSet<String>,
	/// Whether all dependencies could be statically determined.
	is_complete: bool,
}

impl Visitor for FieldDependencyExtractor {
	type Error = std::convert::Infallible;

	fn visit_idiom(&mut self, idiom: &Idiom) -> Result<(), Self::Error> {
		// Try to extract a root field name from the idiom, skipping the
		// `$this`/`$self` prefix when present. Returns the number of
		// leading parts that were consumed (0, 1, or 2).
		let consumed = match idiom.0.as_slice() {
			// Direct field access: `field_name` or `field.nested.path`
			[Part::Field(name), ..] => {
				self.deps.insert(name.as_str().to_owned());
				1
			}
			// `$this.field` / `$self["field"]` — equivalent to bare field.
			[Part::Start(Expr::Param(p)), second, ..] if matches!(p.as_str(), "this" | "self") => {
				match field_name_from_part(second) {
					Some(name) => {
						self.deps.insert(name);
						2
					}
					// $this.* / $this[0] / etc — can't determine field statically
					None => 0,
				}
			}
			_ => 0,
		};
		// Walk the remaining parts. When consumed == 0 (unrecognised
		// pattern), all parts are walked, which correctly marks
		// Part::Start and Part::Lookup as incomplete.
		for p in idiom.0.iter().skip(consumed) {
			self.visit_part(p)?;
		}
		Ok(())
	}

	/// Walk an `Expr`.
	///
	/// The match is **exhaustive (no `_` arm) on purpose**: this analysis is
	/// security-relevant (computed-field permissions rely on its result), and
	/// the default in a security-relevant visitor must be fail-closed. Adding a
	/// new `Expr` variant should be a build error here so a human triages it
	/// into "transparent" or "opaque" rather than silently defaulting to
	/// transparent (the old fail-open behaviour).
	fn visit_expr(&mut self, expr: &Expr) -> Result<(), Self::Error> {
		match expr {
			// === Transparent: walk children, no flag flip ===
			// These constructs do not read fields beyond what their children
			// read. Default traversal is sufficient; any opaque construct
			// inside (Select, Param, Closure, …) will flip the flag itself.
			Expr::Literal(_)
			| Expr::Idiom(_)
			| Expr::Table(_)
			| Expr::Mock(_)
			| Expr::Constant(_)
			| Expr::Break
			| Expr::Continue
			| Expr::Prefix { .. }
			| Expr::Postfix { .. }
			| Expr::Binary { .. }
			| Expr::Block(_)
			| Expr::IfElse(_)
			| Expr::Foreach(_)
			| Expr::Let(_)
			| Expr::Return(_)
			| Expr::Throw(_)
			| Expr::Explain { .. }
			| Expr::Sleep(_)
			| Expr::Info(_) => {
				expr.visit(self)?;
			}

			// === Opaque (flip + walk) ===
			// Subqueries can access arbitrary fields/tables at runtime.
			Expr::Select(_)
			| Expr::Create(_)
			| Expr::Update(_)
			| Expr::Upsert(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_)
			// Closures capture variables and can read fields via captured
			// `$this` indirectly. Walk body for any obvious deps but assume
			// incompleteness.
			| Expr::Closure(_)
			// DDL inside an expression should never appear in a permission
			// clause, but if it does it is by definition opaque to static
			// dependency analysis.
			| Expr::Define(_)
			| Expr::Remove(_)
			| Expr::Rebuild(_)
			| Expr::Alter(_) => {
				self.is_complete = false;
				expr.visit(self)?;
			}

			// === Opaque (flip, do not walk) ===
			// Parameters are resolved at runtime and may bind to anything.
			Expr::Param(_) => {
				self.is_complete = false;
			}

			// === Projection-aware function calls ===
			Expr::FunctionCall(call) => {
				self.visit_function_call_with_projection(call)?;
			}
		}
		Ok(())
	}

	/// Walk a `Part`. Exhaustive match, same fail-closed reasoning as `visit_expr`.
	fn visit_part(&mut self, part: &Part) -> Result<(), Self::Error> {
		match part {
			// Transparent — no field reads outside what walking yields.
			Part::All
			| Part::Flatten
			| Part::Last
			| Part::First
			| Part::Field(_)
			| Part::Optional
			| Part::Doc
			| Part::RepeatRecurse => {}

			// Walk children — any embedded expression contributes deps.
			Part::Where(_) | Part::Value(_) | Part::Destructure(_) | Part::Recurse(_, _, _) => {
				part.visit(self)?;
			}

			// Opaque (flip + walk).
			// Lookup: graph traversals can access other tables.
			// Start: `(subexpr).field` — the start expr is itself opaque to
			//   idiom-root extraction.
			// Method: method dispatch is dynamic on the receiver's runtime
			//   type; the method body is not in the AST.
			Part::Lookup(_) | Part::Start(_) | Part::Method(_, _) => {
				self.is_complete = false;
				part.visit(self)?;
			}
		}
		Ok(())
	}
}

impl FieldDependencyExtractor {
	/// Handle a function call, applying projection-function-aware analysis
	/// before falling through to default argument traversal.
	///
	/// Projection functions (`type::field("x")`, `type::fields(["x", "y"])`)
	/// read fields of the current document by name, bypassing the idiom-walk
	/// path. The match below mirrors the runtime classification in
	/// `exec::function::builtin::type` — kept inline here (rather than
	/// dispatched through the executor's `FunctionRegistry`) so this module
	/// stays on the AST layer with no dependency on `exec`. The runtime
	/// registry asserts these are the only projection functions; adding a new
	/// one requires updating both sites.
	///
	/// Arguments are always walked after the projection analysis, so that
	/// nested subqueries / params / etc. inside arg expressions still
	/// contribute their own deps and may flip the flag.
	fn visit_function_call_with_projection(
		&mut self,
		call: &FunctionCall,
	) -> Result<(), std::convert::Infallible> {
		if let Function::Normal(name) = &call.receiver {
			match name.as_str() {
				"type::field" => self.analyse_type_field(&call.arguments),
				"type::fields" => self.analyse_type_fields(&call.arguments),
				_ => {}
			}
		}
		call.visit(self)?;
		Ok(())
	}

	/// `type::field("static_name")` — single literal-string arg whose parsed
	/// idiom starts with a static `Part::Field` adds the root to deps. Any
	/// dynamic shape flips `is_complete = false`.
	fn analyse_type_field(&mut self, args: &[Expr]) {
		match args {
			[Expr::Literal(Literal::String(s))] => match parse_idiom_root(s.as_str()) {
				Some(root) => {
					self.deps.insert(root);
				}
				None => {
					self.is_complete = false;
				}
			},
			_ => {
				self.is_complete = false;
			}
		}
	}

	/// `type::fields(["a", "b.c"])` — single literal-array arg with all
	/// literal-string elements adds each root to deps. Any dynamic element or
	/// non-array arg flips `is_complete = false`.
	fn analyse_type_fields(&mut self, args: &[Expr]) {
		let [Expr::Literal(Literal::Array(items))] = args else {
			self.is_complete = false;
			return;
		};
		let mut pending = Vec::with_capacity(items.len());
		for item in items {
			let Expr::Literal(Literal::String(s)) = item else {
				self.is_complete = false;
				return;
			};
			let Some(root) = parse_idiom_root(s.as_str()) else {
				self.is_complete = false;
				return;
			};
			pending.push(root);
		}
		self.deps.extend(pending);
	}
}

/// Parse a string as a SurrealQL idiom and return the root field name if the
/// parsed idiom starts with a `Part::Field`. Returns `None` for parse errors
/// and for idioms whose first part is not a static field reference
/// (e.g. `$this.x`, `[0]`, function-call roots).
fn parse_idiom_root(s: &str) -> Option<String> {
	let idi: Idiom = crate::syn::idiom(s).ok()?.into();
	match idi.0.first()? {
		Part::Field(name) => Some(name.as_str().to_owned()),
		_ => None,
	}
}

/// Emit a tracing warning that a field-permission expression's deps could not
/// be statically determined, forcing the all-computed-fields fallback for every
/// row of `table`.
///
/// Does NOT include the permission expression source — that may contain
/// schema-author-supplied literal secrets (see `SECURITY_GUIDE.md` §4 and
/// CLAUDE.md "Never log sensitive user data or credentials"). Operators can
/// inspect the expression themselves via `INFO FOR TABLE <table>` under their
/// own auth.
///
/// `table` and `field` must be schema-identifier strings — never strings built
/// from user record data.
///
/// Routing: plain `tracing::warn!`. The OTLP audit-log pipeline at
/// `server/src/telemetry/audit_logs.rs` is attached to the tracing subscriber,
/// so this lands in the compliance trail when OTLP is configured (precedent:
/// `core/src/kvs/slowlog.rs:151`).
pub(crate) fn warn_incomplete_perm_deps(table: &str, field: &str) {
	tracing::warn!(
		target: TARGET,
		table = %table,
		field = %field,
		"Field-permission expression has opaque dependencies; \
		 all computed fields on the named table will be evaluated for every \
		 row. Inspect with `INFO FOR TABLE`."
	);
}

/// Topologically sort computed field indices by their dependencies.
///
/// Given a list of `(field_name, deps)` pairs representing the computed fields,
/// returns the indices into the original slice in a valid evaluation order
/// (dependencies before dependents).
///
/// Uses Kahn's algorithm (BFS-based). Fields whose dependencies are not in the
/// computed field set (i.e. stored fields) are treated as having no in-edges.
///
/// If a cycle exists (should be caught at DEFINE time), fields in the cycle are
/// appended at the end to avoid losing them silently.
pub fn topological_sort_computed_fields(fields: &[(String, Vec<String>)]) -> Vec<usize> {
	if fields.is_empty() {
		return Vec::new();
	}

	let name_to_idx: HashMap<&str, usize> =
		fields.iter().enumerate().map(|(i, (name, _))| (name.as_str(), i)).collect();

	// Compute in-degrees: only count edges from other computed fields
	let mut in_degree = vec![0usize; fields.len()];
	let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); fields.len()];

	for (idx, (_, deps)) in fields.iter().enumerate() {
		for dep in deps {
			if let Some(&dep_idx) = name_to_idx.get(dep.as_str()) {
				in_degree[idx] += 1;
				dependents[dep_idx].push(idx);
			}
			// Dependencies on non-computed (stored) fields don't contribute in-degree
		}
	}

	// BFS from nodes with in-degree 0
	let mut queue: VecDeque<usize> = VecDeque::new();
	for (idx, &deg) in in_degree.iter().enumerate() {
		if deg == 0 {
			queue.push_back(idx);
		}
	}

	let mut result = Vec::with_capacity(fields.len());
	while let Some(idx) = queue.pop_front() {
		result.push(idx);
		for &dependent in &dependents[idx] {
			in_degree[dependent] -= 1;
			if in_degree[dependent] == 0 {
				queue.push_back(dependent);
			}
		}
	}

	// If there are remaining nodes (cycle), append them to avoid silently dropping fields.
	// Cycles should be caught at DEFINE time, but this is a safety net.
	if result.len() < fields.len() {
		for idx in 0..fields.len() {
			if !result.contains(&idx) {
				result.push(idx);
			}
		}
	}

	result
}

/// Compute the transitive closure of needed fields through the computed field
/// dependency graph.
///
/// Given:
/// - `needed`: the set of field names directly needed by the query
/// - `computed_deps`: a map from computed field name -> its `ComputedDeps`
///
/// Returns `Some(set)` with the full set of computed field names that must be
/// evaluated, or `None` if ALL computed fields must be evaluated (because a
/// needed field has `is_complete = false` or has no stored deps).
pub fn resolve_required_computed_fields(
	needed: &HashSet<String>,
	computed_deps: &HashMap<String, ComputedDeps>,
) -> Option<HashSet<String>> {
	let mut required: HashSet<String> = HashSet::new();
	let mut worklist: Vec<String> = needed.iter().cloned().collect();

	while let Some(field) = worklist.pop() {
		if !required.insert(field.clone()) {
			continue; // Already processed
		}
		if let Some(deps) = computed_deps.get(&field) {
			if !deps.is_complete {
				// This field has opaque deps -- must compute ALL fields
				return None;
			}
			for dep in &deps.fields {
				if !required.contains(dep) {
					worklist.push(dep.clone());
				}
			}
		}
		// If field is not in computed_deps, it's a stored field -- no further deps
	}

	Some(required)
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::expr::operator::BinaryOperator;
	use crate::expr::{Literal, Part};

	/// Wrapper to keep test bodies short.
	fn extract(expr: &Expr) -> ComputedDeps {
		extract_computed_deps(expr)
	}

	/// Helper: build `Expr::Idiom` for a simple field name.
	fn field_expr(name: &str) -> Expr {
		Expr::Idiom(Idiom(vec![Part::Field(name.into())]))
	}

	/// Helper: build a literal integer expression.
	fn int_expr(n: i64) -> Expr {
		Expr::Literal(Literal::Integer(n))
	}

	/// Helper: build a literal string expression.
	fn str_lit(s: &str) -> Expr {
		Expr::Literal(Literal::String(Strand::new(s)))
	}

	/// Helper: build a function call expression.
	fn fn_call(name: &str, args: Vec<Expr>) -> Expr {
		Expr::FunctionCall(Box::new(crate::expr::function::FunctionCall {
			receiver: crate::expr::function::Function::Normal(name.to_owned()),
			arguments: args,
		}))
	}

	#[test]
	fn simple_field_reference() {
		// Expression: `b`
		let expr = field_expr("b");
		let deps = extract(&expr);
		assert_eq!(deps.fields, vec!["b"]);
		assert!(deps.is_complete);
	}

	#[test]
	fn binary_expression_two_fields() {
		// Expression: `b + c`
		let expr = Expr::Binary {
			left: Box::new(field_expr("b")),
			op: BinaryOperator::Add,
			right: Box::new(field_expr("c")),
		};
		let deps = extract(&expr);
		assert_eq!(deps.fields, vec!["b", "c"]);
		assert!(deps.is_complete);
	}

	#[test]
	fn field_plus_literal() {
		// Expression: `d + 1`
		let expr = Expr::Binary {
			left: Box::new(field_expr("d")),
			op: BinaryOperator::Add,
			right: Box::new(int_expr(1)),
		};
		let deps = extract(&expr);
		assert_eq!(deps.fields, vec!["d"]);
		assert!(deps.is_complete);
	}

	#[test]
	fn nested_field_access() {
		// Expression: `user.name.first` -- root dep is `user`
		let expr = Expr::Idiom(Idiom(vec![
			Part::Field(Strand::new_static("user")),
			Part::Field(Strand::new_static("name")),
			Part::Field(Strand::new_static("first")),
		]));
		let deps = extract(&expr);
		assert_eq!(deps.fields, vec!["user"]);
		assert!(deps.is_complete);
	}

	#[test]
	fn param_marks_incomplete() {
		// Expression: `$param`
		let expr = Expr::Param(crate::expr::Param::from("param".to_string()));
		let deps = extract(&expr);
		assert!(deps.fields.is_empty());
		assert!(!deps.is_complete);
	}

	#[test]
	fn no_deps_literal_only() {
		// Expression: `55 * 1000`
		let expr = Expr::Binary {
			left: Box::new(int_expr(55)),
			op: BinaryOperator::Multiply,
			right: Box::new(int_expr(1000)),
		};
		let deps = extract(&expr);
		assert!(deps.fields.is_empty());
		assert!(deps.is_complete);
	}

	#[test]
	fn deduplicates_deps() {
		// Expression: `a + a` -- should only list `a` once
		let expr = Expr::Binary {
			left: Box::new(field_expr("a")),
			op: BinaryOperator::Add,
			right: Box::new(field_expr("a")),
		};
		let deps = extract(&expr);
		assert_eq!(deps.fields, vec!["a"]);
		assert!(deps.is_complete);
	}

	#[test]
	fn self_reference() {
		// Expression: `a * 2` -- dep is `a` (self-reference detected by cycle detection, not here)
		let expr = Expr::Binary {
			left: Box::new(field_expr("a")),
			op: BinaryOperator::Multiply,
			right: Box::new(int_expr(2)),
		};
		let deps = extract(&expr);
		assert_eq!(deps.fields, vec!["a"]);
		assert!(deps.is_complete);
	}

	// ===== $this / $self field access tests =====

	/// Helper: build `Expr::Idiom` for `$this.field_name`.
	fn this_field_expr(name: &str) -> Expr {
		Expr::Idiom(Idiom(vec![
			Part::Start(Expr::Param(crate::expr::Param::from("this".to_string()))),
			Part::Field(name.into()),
		]))
	}

	/// Helper: build `Expr::Idiom` for `$self.field_name`.
	fn self_field_expr(name: &str) -> Expr {
		Expr::Idiom(Idiom(vec![
			Part::Start(Expr::Param(crate::expr::Param::from("self".to_string()))),
			Part::Field(name.into()),
		]))
	}

	#[test]
	fn this_dot_field() {
		let deps = extract(&this_field_expr("a"));
		assert_eq!(deps.fields, vec!["a"]);
		assert!(deps.is_complete);
	}

	#[test]
	fn self_dot_field() {
		let deps = extract(&self_field_expr("a"));
		assert_eq!(deps.fields, vec!["a"]);
		assert!(deps.is_complete);
	}

	#[test]
	fn this_dot_nested() {
		// $this.a.b.c -- root dep is `a`
		let expr = Expr::Idiom(Idiom(vec![
			Part::Start(Expr::Param(crate::expr::Param::from("this".to_string()))),
			Part::Field(Strand::new_static("a")),
			Part::Field(Strand::new_static("b")),
			Part::Field(Strand::new_static("c")),
		]));
		let deps = extract(&expr);
		assert_eq!(deps.fields, vec!["a"]);
		assert!(deps.is_complete);
	}

	#[test]
	fn this_dot_field_in_binary() {
		// $this.a + $this.b
		let expr = Expr::Binary {
			left: Box::new(this_field_expr("a")),
			op: BinaryOperator::Add,
			right: Box::new(this_field_expr("b")),
		};
		let deps = extract(&expr);
		assert_eq!(deps.fields, vec!["a", "b"]);
		assert!(deps.is_complete);
	}

	#[test]
	fn mixed_bare_and_this() {
		// a + $this.b + $self.c  →  (a + $this.b) + $self.c
		let expr = Expr::Binary {
			left: Box::new(Expr::Binary {
				left: Box::new(field_expr("a")),
				op: BinaryOperator::Add,
				right: Box::new(this_field_expr("b")),
			}),
			op: BinaryOperator::Add,
			right: Box::new(self_field_expr("c")),
		};
		let deps = extract(&expr);
		assert_eq!(deps.fields, vec!["a", "b", "c"]);
		assert!(deps.is_complete);
	}

	#[test]
	fn this_in_function_args() {
		// math::sum([$this.a, b, $self.c])
		use crate::expr::function::{Function, FunctionCall};
		let expr = Expr::FunctionCall(Box::new(FunctionCall {
			receiver: Function::Normal("math::sum".to_string()),
			arguments: vec![Expr::Literal(Literal::Array(vec![
				this_field_expr("a"),
				field_expr("b"),
				self_field_expr("c"),
			]))],
		}));
		let deps = extract(&expr);
		assert_eq!(deps.fields, vec!["a", "b", "c"]);
		assert!(deps.is_complete);
	}

	#[test]
	fn this_in_nested_parens() {
		// (a + ($this.b + $self.c))
		let expr = Expr::Binary {
			left: Box::new(field_expr("a")),
			op: BinaryOperator::Add,
			right: Box::new(Expr::Binary {
				left: Box::new(this_field_expr("b")),
				op: BinaryOperator::Add,
				right: Box::new(self_field_expr("c")),
			}),
		};
		let deps = extract(&expr);
		assert_eq!(deps.fields, vec!["a", "b", "c"]);
		assert!(deps.is_complete);
	}

	#[test]
	fn this_alone_marks_incomplete() {
		// $this (bare param, not an idiom)
		let expr = Expr::Param(crate::expr::Param::from("this".to_string()));
		let deps = extract(&expr);
		assert!(deps.fields.is_empty());
		assert!(!deps.is_complete);
	}

	#[test]
	fn this_dot_wildcard_marks_incomplete() {
		// $this.* -- can access any field
		let expr = Expr::Idiom(Idiom(vec![
			Part::Start(Expr::Param(crate::expr::Param::from("this".to_string()))),
			Part::All,
		]));
		let deps = extract(&expr);
		assert!(deps.fields.is_empty());
		assert!(!deps.is_complete);
	}

	#[test]
	fn this_bracket_string_field() {
		// $this["a"] -- bracket string access, equivalent to $this.a
		let expr = Expr::Idiom(Idiom(vec![
			Part::Start(Expr::Param(crate::expr::Param::from("this".to_string()))),
			Part::Value(Expr::Literal(Literal::String(Strand::new_static("a")))),
		]));
		let deps = extract(&expr);
		assert_eq!(deps.fields, vec!["a"]);
		assert!(deps.is_complete);
	}

	#[test]
	fn self_bracket_string_field() {
		// $self["c"] -- bracket string access, equivalent to $self.c
		let expr = Expr::Idiom(Idiom(vec![
			Part::Start(Expr::Param(crate::expr::Param::from("self".to_string()))),
			Part::Value(Expr::Literal(Literal::String(Strand::new_static("c")))),
		]));
		let deps = extract(&expr);
		assert_eq!(deps.fields, vec!["c"]);
		assert!(deps.is_complete);
	}

	#[test]
	fn mixed_dot_and_bracket() {
		// (a + ($this.b + $self["c"]))
		let expr = Expr::Binary {
			left: Box::new(field_expr("a")),
			op: BinaryOperator::Add,
			right: Box::new(Expr::Binary {
				left: Box::new(this_field_expr("b")),
				op: BinaryOperator::Add,
				right: Box::new(Expr::Idiom(Idiom(vec![
					Part::Start(Expr::Param(crate::expr::Param::from("self".to_string()))),
					Part::Value(Expr::Literal(Literal::String(Strand::new_static("c")))),
				]))),
			}),
		};
		let deps = extract(&expr);
		assert_eq!(deps.fields, vec!["a", "b", "c"]);
		assert!(deps.is_complete);
	}

	#[test]
	fn other_param_dot_field_marks_incomplete() {
		// $foo.a -- unknown param, not $this/$self
		let expr = Expr::Idiom(Idiom(vec![
			Part::Start(Expr::Param(crate::expr::Param::from("foo".to_string()))),
			Part::Field(Strand::new_static("a")),
		]));
		let deps = extract(&expr);
		assert!(!deps.is_complete);
	}

	// ===== Opaque-marking tests (one per match arm) =====

	/// Helper: parse a SurrealQL expression string into an `expr::Expr`.
	/// Using the parser keeps the tests resilient to small AST struct
	/// shape changes — they exercise the same surface a user would write.
	fn parse(s: &str) -> Expr {
		crate::syn::expr(s).expect("test expression must parse").into()
	}

	#[test]
	fn select_subquery_marks_incomplete() {
		// `(SELECT * FROM t) + b` -- known dep `b` extracted, but the subquery
		// makes the analysis incomplete.
		let expr = parse("(SELECT * FROM t) + b");
		let deps = extract(&expr);
		assert!(!deps.is_complete);
		assert!(deps.fields.contains(&"b".to_string()));
	}

	#[test]
	fn create_subquery_marks_incomplete() {
		let expr = parse("(CREATE t SET x = 1).y");
		let deps = extract(&expr);
		assert!(!deps.is_complete);
	}

	#[test]
	fn update_subquery_marks_incomplete() {
		let expr = parse("(UPDATE t SET x = 1).y");
		let deps = extract(&expr);
		assert!(!deps.is_complete);
	}

	#[test]
	fn upsert_subquery_marks_incomplete() {
		let expr = parse("(UPSERT t SET x = 1).y");
		let deps = extract(&expr);
		assert!(!deps.is_complete);
	}

	#[test]
	fn delete_subquery_marks_incomplete() {
		let expr = parse("(DELETE t).y");
		let deps = extract(&expr);
		assert!(!deps.is_complete);
	}

	#[test]
	fn relate_subquery_marks_incomplete() {
		let expr = parse("(RELATE a:1 -> rel -> b:1).y");
		let deps = extract(&expr);
		assert!(!deps.is_complete);
	}

	#[test]
	fn insert_subquery_marks_incomplete() {
		let expr = parse("(INSERT INTO t { x: 1 }).y");
		let deps = extract(&expr);
		assert!(!deps.is_complete);
	}

	#[test]
	fn closure_marks_incomplete() {
		// `|$x| $x + a` -- closure flips incompleteness.
		let expr = parse("|$x: any| $x + a");
		let deps = extract(&expr);
		assert!(!deps.is_complete);
	}

	#[test]
	fn graph_lookup_marks_incomplete() {
		// `->friends->person` graph traversal -- contains a Part::Lookup which
		// is opaque to static analysis.
		let expr = parse("->friends->person");
		let deps = extract(&expr);
		assert!(!deps.is_complete);
	}

	#[test]
	fn start_with_arbitrary_expr_marks_incomplete() {
		// `(a + b).field` -- the idiom's first part is a `Part::Start` over
		// an arbitrary expression, which is opaque to root-name extraction.
		let expr = parse("(a + b).field");
		let deps = extract(&expr);
		assert!(!deps.is_complete);
	}

	// ===== Composition: opaque inside other constructs =====

	#[test]
	fn subquery_inside_function_args() {
		// Subquery inside fn args must still flip incomplete via
		// default-traversal recursion. `a` is still recovered.
		let expr = parse("array::len([a, (SELECT * FROM t)])");
		let deps = extract(&expr);
		assert!(!deps.is_complete);
		assert!(deps.fields.contains(&"a".to_string()));
	}

	#[test]
	fn subquery_inside_ifelse() {
		// Both branches walked. `a` and `b` extracted from cond/else;
		// subquery in the then-branch flips incomplete.
		let expr = parse("IF a > 0 { (SELECT * FROM t) } ELSE { b }");
		let deps = extract(&expr);
		assert!(!deps.is_complete);
		assert!(deps.fields.contains(&"a".to_string()));
		assert!(deps.fields.contains(&"b".to_string()));
	}

	// ===== Control-flow happy path (must walk children) =====

	#[test]
	fn ifelse_extracts_branch_deps() {
		let expr = parse("IF a { b } ELSE { c }");
		let deps = extract(&expr);
		assert!(deps.is_complete);
		assert!(deps.fields.contains(&"a".to_string()));
		assert!(deps.fields.contains(&"b".to_string()));
		assert!(deps.fields.contains(&"c".to_string()));
	}

	// ===== type::field / type::fields projection tests =====

	#[test]
	fn type_field_literal_extracts_root() {
		let expr = fn_call("type::field", vec![str_lit("admin_only")]);
		let deps = extract(&expr);
		assert!(deps.is_complete);
		assert_eq!(deps.fields, vec!["admin_only"]);
	}

	#[test]
	fn type_field_dotted_extracts_root() {
		let expr = fn_call("type::field", vec![str_lit("user.name")]);
		let deps = extract(&expr);
		assert!(deps.is_complete);
		assert_eq!(deps.fields, vec!["user"]);
	}

	#[test]
	fn type_field_dollar_this_marks_incomplete() {
		// `type::field("$this.x")` -- parsed idiom starts with Start, not Field.
		let expr = fn_call("type::field", vec![str_lit("$this.x")]);
		let deps = extract(&expr);
		assert!(!deps.is_complete);
	}

	#[test]
	fn type_field_param_arg_marks_incomplete() {
		// `type::field($name)` -- dynamic arg.
		let expr =
			fn_call("type::field", vec![Expr::Param(crate::expr::Param::from("name".to_string()))]);
		let deps = extract(&expr);
		assert!(!deps.is_complete);
	}

	#[test]
	fn type_field_concat_arg_marks_incomplete() {
		// `type::field("a" + b)` -- the arg is a Binary, not a single string
		// literal, so the projection analyser returns Incomplete.
		let expr = fn_call(
			"type::field",
			vec![Expr::Binary {
				left: Box::new(str_lit("a")),
				op: BinaryOperator::Add,
				right: Box::new(field_expr("b")),
			}],
		);
		let deps = extract(&expr);
		assert!(!deps.is_complete);
		// The Binary's inner field `b` is still discovered via arg-walking.
		assert!(deps.fields.contains(&"b".to_string()));
	}

	#[test]
	fn type_fields_array_of_literals_extracts_roots() {
		let expr = fn_call(
			"type::fields",
			vec![Expr::Literal(Literal::Array(vec![str_lit("a"), str_lit("b.c")]))],
		);
		let deps = extract(&expr);
		assert!(deps.is_complete);
		assert_eq!(deps.fields, vec!["a", "b"]);
	}

	#[test]
	fn type_fields_with_dynamic_element_marks_incomplete() {
		// `type::fields([a])` -- the array contains an idiom, not a string
		// literal.
		let expr =
			fn_call("type::fields", vec![Expr::Literal(Literal::Array(vec![field_expr("a")]))]);
		let deps = extract(&expr);
		assert!(!deps.is_complete);
	}

	#[test]
	fn type_fields_non_array_marks_incomplete() {
		// `type::fields($names)`
		let expr = fn_call(
			"type::fields",
			vec![Expr::Param(crate::expr::Param::from("names".to_string()))],
		);
		let deps = extract(&expr);
		assert!(!deps.is_complete);
	}

	#[test]
	fn type_field_combined_with_other_field() {
		// `type::field("a") + b` -- both contribute to deps; analysis stays
		// complete because both halves are statically resolvable.
		let expr = Expr::Binary {
			left: Box::new(fn_call("type::field", vec![str_lit("a")])),
			op: BinaryOperator::Add,
			right: Box::new(field_expr("b")),
		};
		let deps = extract(&expr);
		assert!(deps.is_complete);
		assert_eq!(deps.fields, vec!["a", "b"]);
	}

	// ===== Topological sort tests =====

	#[test]
	fn topo_sort_empty() {
		let result = topological_sort_computed_fields(&[]);
		assert!(result.is_empty());
	}

	#[test]
	fn topo_sort_no_deps() {
		// b has no deps, a has no deps
		let fields = vec![("b".to_string(), vec![]), ("a".to_string(), vec![])];
		let order = topological_sort_computed_fields(&fields);
		assert_eq!(order.len(), 2);
		// Both should appear (order doesn't matter since no deps)
		assert!(order.contains(&0));
		assert!(order.contains(&1));
	}

	#[test]
	fn topo_sort_linear_chain() {
		// a depends on b, b depends on c: evaluation order must be c, b, a
		let fields = vec![
			("a".to_string(), vec!["b".to_string()]),
			("b".to_string(), vec!["c".to_string()]),
			("c".to_string(), vec![]),
		];
		let order = topological_sort_computed_fields(&fields);
		assert_eq!(order.len(), 3);
		// c (idx 2) must come before b (idx 1), which must come before a (idx 0)
		let pos_a = order.iter().position(|&x| x == 0).unwrap();
		let pos_b = order.iter().position(|&x| x == 1).unwrap();
		let pos_c = order.iter().position(|&x| x == 2).unwrap();
		assert!(pos_c < pos_b);
		assert!(pos_b < pos_a);
	}

	#[test]
	fn topo_sort_diamond() {
		// a depends on b and c, b depends on d, c depends on d
		let fields = vec![
			("a".to_string(), vec!["b".to_string(), "c".to_string()]),
			("b".to_string(), vec!["d".to_string()]),
			("c".to_string(), vec!["d".to_string()]),
			("d".to_string(), vec![]),
		];
		let order = topological_sort_computed_fields(&fields);
		assert_eq!(order.len(), 4);
		let pos_a = order.iter().position(|&x| x == 0).unwrap();
		let pos_b = order.iter().position(|&x| x == 1).unwrap();
		let pos_c = order.iter().position(|&x| x == 2).unwrap();
		let pos_d = order.iter().position(|&x| x == 3).unwrap();
		assert!(pos_d < pos_b);
		assert!(pos_d < pos_c);
		assert!(pos_b < pos_a);
		assert!(pos_c < pos_a);
	}

	#[test]
	fn topo_sort_dep_on_stored_field() {
		// a depends on "stored" which is not a computed field
		let fields = vec![("a".to_string(), vec!["stored".to_string()])];
		let order = topological_sort_computed_fields(&fields);
		// "stored" is not in the computed field list, so a has in-degree 0
		assert_eq!(order, vec![0]);
	}

	// ===== Transitive closure tests =====

	#[test]
	fn closure_simple() {
		let mut computed = HashMap::new();
		computed.insert(
			"a".to_string(),
			ComputedDeps {
				fields: vec!["b".to_string(), "c".to_string()],
				is_complete: true,
			},
		);
		computed.insert(
			"b".to_string(),
			ComputedDeps {
				fields: vec![],
				is_complete: true,
			},
		);
		computed.insert(
			"c".to_string(),
			ComputedDeps {
				fields: vec!["d".to_string()],
				is_complete: true,
			},
		);

		let needed: HashSet<String> = ["a".to_string()].into_iter().collect();
		let required = resolve_required_computed_fields(&needed, &computed).unwrap();
		// a needs b and c, c needs d (stored), so required computed = {a, b, c, d}
		assert!(required.contains("a"));
		assert!(required.contains("b"));
		assert!(required.contains("c"));
		assert!(required.contains("d")); // d is included even though it's stored
	}

	#[test]
	fn closure_incomplete_forces_all() {
		let mut computed = HashMap::new();
		computed.insert(
			"a".to_string(),
			ComputedDeps {
				fields: vec!["b".to_string()],
				is_complete: false,
			},
		);
		computed.insert(
			"b".to_string(),
			ComputedDeps {
				fields: vec![],
				is_complete: true,
			},
		);

		let needed: HashSet<String> = ["a".to_string()].into_iter().collect();
		let result = resolve_required_computed_fields(&needed, &computed);
		// a has is_complete=false, so we must compute ALL
		assert!(result.is_none());
	}

	#[test]
	fn closure_only_needed() {
		let mut computed = HashMap::new();
		computed.insert(
			"a".to_string(),
			ComputedDeps {
				fields: vec!["b".to_string(), "c".to_string()],
				is_complete: true,
			},
		);
		computed.insert(
			"b".to_string(),
			ComputedDeps {
				fields: vec![],
				is_complete: true,
			},
		);
		computed.insert(
			"c".to_string(),
			ComputedDeps {
				fields: vec![],
				is_complete: true,
			},
		);
		computed.insert(
			"x".to_string(),
			ComputedDeps {
				fields: vec!["y".to_string()],
				is_complete: true,
			},
		);

		let needed: HashSet<String> = ["b".to_string()].into_iter().collect();
		let required = resolve_required_computed_fields(&needed, &computed).unwrap();
		// Only b is needed, no transitive deps
		assert!(required.contains("b"));
		assert!(!required.contains("a"));
		assert!(!required.contains("x"));
	}
}
