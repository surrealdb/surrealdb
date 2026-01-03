use std::fmt::Write;

use crate::exec::{OperatorPlan, PhysicalExpr, PlannedStatement};

/// Format an execution plan as a text tree for display
pub(crate) fn format_planned_statement(stmt: &PlannedStatement) -> String {
	let mut output = String::new();
	format_planned_statement_impl(stmt, &mut output, "", true);
	output
}

fn format_planned_statement_impl(
	stmt: &PlannedStatement,
	output: &mut String,
	prefix: &str,
	is_last: bool,
) {
	let connector = if prefix.is_empty() {
		""
	} else if is_last {
		"└── "
	} else {
		"├── "
	};

	match stmt {
		PlannedStatement::Query(plan) => {
			write!(output, "{}{}", prefix, connector).unwrap();
			format_execution_plan(plan.as_ref(), output, prefix, is_last);
		}
		PlannedStatement::SessionCommand(cmd) => {
			writeln!(output, "{}{}{:?}", prefix, connector, cmd).unwrap();
		}
		PlannedStatement::Let {
			name,
			value,
		} => {
			writeln!(output, "{}{}Let [name: ${}]", prefix, connector, name).unwrap();

			match value {
				crate::exec::LetValue::Scalar(expr) => {
					write!(output, "{}=> ", prefix).unwrap();
					format_physical_expr(expr.as_ref(), output);
				}
				crate::exec::LetValue::Query(plan) => {
					write!(output, "{}└────> ", prefix).unwrap();
					let next_prefix = format!("{}       ", prefix);
					format_execution_plan(plan.as_ref(), output, &next_prefix, true);
				}
			}
		}
		PlannedStatement::Scalar(expr) => {
			write!(output, "{}{}Scalar => ", prefix, connector).unwrap();
			format_physical_expr(expr.as_ref(), output);
		}
		PlannedStatement::Explain {
			format: _,
			statement,
		} => {
			// For nested EXPLAIN (unlikely but handle it)
			format_planned_statement_impl(statement, output, prefix, is_last);
		}
	}
}

/// Format an execution plan node as a text tree
fn format_execution_plan(
	plan: &dyn OperatorPlan,
	output: &mut String,
	prefix: &str,
	_is_last: bool,
) {
	// Get operator name and properties
	let name = plan.name();
	let properties = plan.attrs();

	// Show context level
	let context = plan.required_context();
	write!(output, "{} [ctx: {}]", name, context.short_name()).unwrap();

	// Show properties if any
	if !properties.is_empty() {
		write!(output, " [").unwrap();
		for (i, (key, value)) in properties.iter().enumerate() {
			if i > 0 {
				write!(output, ", ").unwrap();
			}
			write!(output, "{key}: {value}").unwrap();
		}
		write!(output, "]").unwrap();
	}

	writeln!(output).unwrap();

	// Format children
	let children = plan.children();
	if !children.is_empty() {
		for (i, child) in children.iter().enumerate() {
			let is_last_child = i == children.len() - 1;
			// Use proper tree connector with arrow
			let child_connector = if is_last_child {
				"└────> "
			} else {
				"├────> "
			};
			write!(output, "{}{}", prefix, child_connector).unwrap();
			// Calculate next prefix: align under the operator name, with continuation bar if not
			// last
			let next_prefix = if is_last_child {
				format!("{}       ", prefix)
			} else {
				format!("{}│      ", prefix)
			};
			format_execution_plan(child.as_ref(), output, &next_prefix, is_last_child);
		}
	}
}

/// Format a physical expression
fn format_physical_expr(expr: &dyn PhysicalExpr, output: &mut String) {
	let name = expr.name();
	writeln!(output, "{name}").unwrap();
}
