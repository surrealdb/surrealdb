//! The clauses of a `SELECT` pipeline as `collect_pipeline` hands them to the VM's mapping: `SPLIT`
//! targets and the selection they apply to.

#[cfg(test)]
mod tests {

	use crate::exec::planner::fold::collect_pipeline;
	use crate::expr::logical::{LogicalExpr, LogicalFields, lower_select};
	use crate::syn;

	/// The `SPLIT` targets and selection of a `SELECT`, as the pipeline sees
	/// them.
	fn split_and_fields(sql: &str) -> (Vec<LogicalExpr>, Option<LogicalFields>) {
		let ast = syn::parse(sql).expect("parse");
		let mut exprs = ast.expressions;
		assert_eq!(exprs.len(), 1, "expected one statement in {sql:?}");
		let surrealdb_expr::parser::TopLevelExpr::Expr(crate::expr::Expr::Select(select)) =
			exprs.remove(0)
		else {
			panic!("`{sql}` is not a SELECT");
		};
		let (clauses, _) = collect_pipeline(lower_select(*select).expect("lowers"));
		let split = clauses.split.expect("the statement has a SPLIT");
		(split, clauses.project.map(|project| project.fields))
	}

	/// Every `SPLIT` shape reaches the pipeline with its targets and its
	/// selection intact, whatever the projection does to the split field.
	/// The pipeline projects before it splits, so no shape is turned away
	/// for naming a field only the projected row has.
	#[test]
	fn every_split_shape_carries_its_targets_and_selection() {
		for sql in [
			// The field is carried through under its own name.
			"SELECT id, own FROM t SPLIT own",
			"SELECT * FROM t SPLIT own",
			"SELECT id, own, other FROM t SPLIT own, other",
			// The name exists only after the projection.
			"SELECT id, own AS x FROM t SPLIT x",
			"SELECT id, [1, 2, 3] AS x FROM t SPLIT x",
			"SELECT id, link.tags AS x FROM t SPLIT x",
			// The target is a path the projection builds.
			"SELECT id, link.tags FROM t SPLIT link.tags",
			// The name holds a different value on either side.
			"SELECT id, other AS own FROM t SPLIT own",
			"SELECT id, array::distinct(own) AS own FROM t SPLIT own",
			// Another field is owed the whole of the split value.
			"SELECT id, own AS a, own FROM t SPLIT own",
		] {
			let (targets, fields) = split_and_fields(sql);
			assert!(!targets.is_empty(), "`{sql}` carries its SPLIT targets");
			assert!(fields.is_some(), "`{sql}` carries its selection");
		}
	}
}
