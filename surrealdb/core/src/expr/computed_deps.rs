//! Computed field dependency extraction.
//!
//! Extracts same-table field dependencies from computed field expressions using the
//! expression visitor pattern. This is used for:
//! - Cycle detection at DEFINE FIELD time
//! - Topological ordering of computed field evaluation
//! - Selective computation (only compute fields needed by the query)

use std::collections::{HashMap, HashSet, VecDeque};

use crate::expr::visit::{Visit, Visitor};
use crate::expr::{Expr, Idiom, Part};

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
pub fn extract_computed_deps(expr: &Expr) -> ComputedDeps {
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
		// Extract the root field name from the idiom.
		// For `b.nested.path`, only `b` is a same-table dependency.
		if let Some(Part::Field(name)) = idiom.0.first() {
			self.deps.insert(name.clone());
		}
		// Walk nested parts for any embedded expressions (WHERE clauses, methods, etc.)
		for p in idiom.0.iter() {
			self.visit_part(p)?;
		}
		Ok(())
	}

	fn visit_expr(&mut self, expr: &Expr) -> Result<(), Self::Error> {
		match expr {
			// Subqueries can access arbitrary fields at runtime.
			Expr::Select(_)
			| Expr::Create(_)
			| Expr::Update(_)
			| Expr::Upsert(_)
			| Expr::Delete(_)
			| Expr::Relate(_)
			| Expr::Insert(_) => {
				self.is_complete = false;
				// Still walk the expression to extract any known deps.
				expr.visit(self)?;
			}
			// Parameters are resolved at runtime -- could reference anything.
			Expr::Param(_) => {
				self.is_complete = false;
			}
			// Closures can capture external state.
			Expr::Closure(_) => {
				self.is_complete = false;
				// Walk the body to extract known deps.
				expr.visit(self)?;
			}
			// All other expressions: use default visitor traversal.
			_ => {
				expr.visit(self)?;
			}
		}
		Ok(())
	}

	fn visit_part(&mut self, part: &Part) -> Result<(), Self::Error> {
		match part {
			// Graph traversals can access other tables.
			Part::Lookup(_) => {
				self.is_complete = false;
				// Walk the lookup for any embedded expressions.
				part.visit(self)?;
			}
			// Start expressions (e.g., `(subexpr).field`) are opaque.
			Part::Start(_) => {
				self.is_complete = false;
				part.visit(self)?;
			}
			// All other parts: use default visitor traversal.
			_ => {
				part.visit(self)?;
			}
		}
		Ok(())
	}
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
	use super::*;
	use crate::expr::operator::BinaryOperator;
	use crate::expr::{Literal, Part};

	/// Helper: build `Expr::Idiom` for a simple field name.
	fn field_expr(name: &str) -> Expr {
		Expr::Idiom(Idiom(vec![Part::Field(name.to_string())]))
	}

	/// Helper: build a literal integer expression.
	fn int_expr(n: i64) -> Expr {
		Expr::Literal(Literal::Integer(n))
	}

	#[test]
	fn simple_field_reference() {
		// Expression: `b`
		let expr = field_expr("b");
		let deps = extract_computed_deps(&expr);
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
		let deps = extract_computed_deps(&expr);
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
		let deps = extract_computed_deps(&expr);
		assert_eq!(deps.fields, vec!["d"]);
		assert!(deps.is_complete);
	}

	#[test]
	fn nested_field_access() {
		// Expression: `user.name.first` -- root dep is `user`
		let expr = Expr::Idiom(Idiom(vec![
			Part::Field("user".to_string()),
			Part::Field("name".to_string()),
			Part::Field("first".to_string()),
		]));
		let deps = extract_computed_deps(&expr);
		assert_eq!(deps.fields, vec!["user"]);
		assert!(deps.is_complete);
	}

	#[test]
	fn param_marks_incomplete() {
		// Expression: `$param`
		let expr = Expr::Param(crate::expr::Param::from("param".to_string()));
		let deps = extract_computed_deps(&expr);
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
		let deps = extract_computed_deps(&expr);
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
		let deps = extract_computed_deps(&expr);
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
		let deps = extract_computed_deps(&expr);
		assert_eq!(deps.fields, vec!["a"]);
		assert!(deps.is_complete);
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
