use anyhow::Result;

use super::{Expr, LogicalPlan, TopLevelExpr};
use crate::expr::statements::UseStatement;
use crate::expr::{Cond, Fetch, Fetchs};

mod rules;
mod utils;

pub(crate) use rules::{ConstantFolding, StaticLiteralFolding};

/// Tracks whether a transformation occurred during optimisation.
///
/// Inspired by DataFusion's Transformed type, this wrapper indicates
/// whether an expression or plan was modified during an optimisation pass.
#[derive(Debug, Clone, PartialEq)]
pub struct Transformed<T> {
	pub data: T,
	pub transformed: bool,
}

impl<T> Transformed<T> {
	/// Create a new Transformed indicating the data was changed
	pub fn yes(data: T) -> Self {
		Self {
			data,
			transformed: true,
		}
	}

	/// Create a new Transformed indicating the data was not changed
	pub fn no(data: T) -> Self {
		Self {
			data,
			transformed: false,
		}
	}

	/// Map the data using a function, preserving the transformed flag
	pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Transformed<U> {
		Transformed {
			data: f(self.data),
			transformed: self.transformed,
		}
	}

	/// Transform the data with a function that returns a Transformed,
	/// combining the transformed flags with OR
	#[allow(dead_code)]
	pub fn and_then<U, F: FnOnce(T) -> Result<Transformed<U>>>(
		self,
		f: F,
	) -> Result<Transformed<U>> {
		let result = f(self.data)?;
		Ok(Transformed {
			data: result.data,
			transformed: self.transformed || result.transformed,
		})
	}
}

/// Trait for optimisation rules that can transform expressions and plans.
///
/// Following DataFusion's OptimiserRule pattern, each rule implements
/// a specific optimisation strategy.
pub(crate) trait OptimiserRule: Send + Sync {
	/// Returns the name of this optimisation rule
	#[allow(dead_code)]
	fn name(&self) -> &str;

	/// Optimise an expression, returning the potentially transformed expression
	fn optimise_expr(&self, expr: Expr) -> Result<Transformed<Expr>>;

	/// Optimise a top-level expression in the plan
	fn optimise_top_level_expr(&self, expr: TopLevelExpr) -> Result<Transformed<TopLevelExpr>> {
		match expr {
			TopLevelExpr::Begin | TopLevelExpr::Cancel | TopLevelExpr::Commit => {
				Ok(Transformed::no(expr))
			}
			TopLevelExpr::Access(_) => Ok(Transformed::no(expr)),
			TopLevelExpr::Kill(mut kill) => {
				let result = self.optimise_expr(kill.id)?;
				kill.id = result.data;
				Ok(Transformed {
					data: TopLevelExpr::Kill(kill),
					transformed: result.transformed,
				})
			}
			TopLevelExpr::Live(mut live) => {
				let mut transformed = false;

				// Optimise the what expression
				let what_result = self.optimise_expr(live.what)?;
				transformed = transformed || what_result.transformed;
				live.what = what_result.data;

				// Optimise the cond expression if present
				if let Some(cond) = live.cond {
					let cond_result = self.optimise_expr(cond.0)?;
					transformed = transformed || cond_result.transformed;
					live.cond = Some(Cond(cond_result.data));
				}

				// Optimise the fetch expressions if present
				if let Some(fetchs) = live.fetch {
					let mut new_fetchs = Vec::with_capacity(fetchs.0.len());
					for fetch in fetchs.0 {
						let result = self.optimise_expr(fetch.0)?;
						transformed = transformed || result.transformed;
						new_fetchs.push(Fetch(result.data));
					}
					live.fetch = Some(Fetchs(new_fetchs));
				}

				Ok(Transformed {
					data: TopLevelExpr::Live(live),
					transformed,
				})
			}
			TopLevelExpr::Option(option) => Ok(Transformed::no(TopLevelExpr::Option(option))),
			TopLevelExpr::Use(use_stmt) => match use_stmt {
				UseStatement::Ns(ns) => {
					let result = self.optimise_expr(ns)?;
					Ok(Transformed {
						data: TopLevelExpr::Use(UseStatement::Ns(result.data)),
						transformed: result.transformed,
					})
				}
				UseStatement::Db(db) => {
					let result = self.optimise_expr(db)?;
					Ok(Transformed {
						data: TopLevelExpr::Use(UseStatement::Db(result.data)),
						transformed: result.transformed,
					})
				}
				UseStatement::NsDb(ns, db) => {
					let ns_result = self.optimise_expr(ns)?;
					let db_result = self.optimise_expr(db)?;
					Ok(Transformed {
						data: TopLevelExpr::Use(UseStatement::NsDb(ns_result.data, db_result.data)),
						transformed: ns_result.transformed || db_result.transformed,
					})
				}
				UseStatement::Default => {
					Ok(Transformed::no(TopLevelExpr::Use(UseStatement::Default)))
				}
			},
			TopLevelExpr::Show(show) => Ok(Transformed::no(TopLevelExpr::Show(show))),
			TopLevelExpr::Expr(e) => {
				let result = self.optimise_expr(e)?;
				Ok(result.map(TopLevelExpr::Expr))
			}
		}
	}
}

/// The main optimiser that applies a sequence of optimisation rules.
///
/// The optimiser runs multiple passes over the logical plan, applying
/// each rule in sequence until either no changes are made or the maximum
/// number of passes is reached.
pub(crate) struct Optimiser {
	rules: Vec<Box<dyn OptimiserRule>>,
	max_passes: usize,
}

impl Optimiser {
	/// Create a new optimiser with no rules
	///
	/// By default, the optimiser will run a maximum of 3 passes.
	/// Use `with_max_passes()` to configure a different value, or use
	/// `Optimiser::all()` which automatically uses the configured value
	/// from `SURREAL_OPTIMISER_MAX_PASSES` environment variable.
	pub fn new() -> Self {
		Self {
			rules: Vec::new(),
			// Default to 3, but Optimiser::all() uses the configured value
			max_passes: 3,
		}
	}

	/// Create an optimiser with all available optimisation rules
	///
	/// This optimiser includes:
	/// - StaticLiteralFolding: Converts static literals to pre-computed values
	/// - ConstantFolding: Evaluates constant expressions at compile time
	///
	/// Note: The max_passes should be set by the caller using with_max_passes().
	/// When used through the Datastore, it will be configured from EngineOptions.
	pub fn all() -> Self {
		Self::new().add_rule(Box::new(StaticLiteralFolding)).add_rule(Box::new(ConstantFolding))
		// Caller should set max_passes via with_max_passes()
	}

	/// Add an optimisation rule to the optimiser
	pub fn add_rule(mut self, rule: Box<dyn OptimiserRule>) -> Self {
		self.rules.push(rule);
		self
	}

	/// Set the maximum number of optimisation passes
	pub fn with_max_passes(mut self, max_passes: usize) -> Self {
		self.max_passes = max_passes;
		self
	}

	/// Optimise a logical plan by applying all rules iteratively
	pub fn optimise(&self, mut plan: LogicalPlan) -> Result<LogicalPlan> {
		for pass in 0..self.max_passes {
			let mut changed = false;

			// Apply each rule in sequence
			for rule in &self.rules {
				let result = self.optimise_plan_with_rule(plan, rule.as_ref())?;
				changed = changed || result.transformed;
				plan = result.data;
			}

			// If nothing changed in this pass, we're done
			if !changed {
				tracing::debug!("Optimiser converged after {} passes", pass + 1);
				break;
			}
		}

		Ok(plan)
	}

	/// Apply a single rule to the entire plan
	fn optimise_plan_with_rule(
		&self,
		plan: LogicalPlan,
		rule: &dyn OptimiserRule,
	) -> Result<Transformed<LogicalPlan>> {
		let mut transformed = false;
		let mut optimised_expressions = Vec::with_capacity(plan.expressions.len());

		for expr in plan.expressions {
			let result = rule.optimise_top_level_expr(expr)?;
			transformed = transformed || result.transformed;
			optimised_expressions.push(result.data);
		}

		Ok(Transformed {
			data: LogicalPlan {
				expressions: optimised_expressions,
			},
			transformed,
		})
	}
}

impl Default for Optimiser {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::{BinaryOperator, Literal, ObjectEntry};
	use crate::val::{Number, Value};

	#[test]
	fn test_transformed_yes() {
		let t = Transformed::yes(42);
		assert_eq!(t.data, 42);
		assert!(t.transformed);
	}

	#[test]
	fn test_transformed_no() {
		let t = Transformed::no(42);
		assert_eq!(t.data, 42);
		assert!(!t.transformed);
	}

	#[test]
	fn test_transformed_map() {
		let t = Transformed::yes(42);
		let mapped = t.map(|x| x * 2);
		assert_eq!(mapped.data, 84);
		assert!(mapped.transformed);
	}

	#[test]
	fn test_transformed_and_then() {
		let t = Transformed::yes(42);
		let result = t.and_then(|x| Ok(Transformed::no(x * 2))).unwrap();
		assert_eq!(result.data, 84);
		assert!(result.transformed); // Original was transformed
	}

	#[test]
	fn test_optimiser_empty() {
		let optimiser = Optimiser::new();
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(Expr::Literal(Literal::Integer(42)))],
		};
		let result = optimiser.optimise(plan).unwrap();
		assert_eq!(result.expressions.len(), 1);
	}

	#[test]
	fn test_static_literal_folding_simple() {
		let optimiser = Optimiser::new().add_rule(Box::new(StaticLiteralFolding));
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(Expr::Literal(Literal::Integer(42)))],
		};
		let result = optimiser.optimise(plan).unwrap();
		match &result.expressions[0] {
			TopLevelExpr::Expr(Expr::Value(Value::Number(Number::Int(42)))) => (),
			other => panic!("Expected Value(42), got {:?}", other),
		}
	}

	#[test]
	fn test_static_literal_folding_array() {
		let optimiser = Optimiser::new().add_rule(Box::new(StaticLiteralFolding));
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(Expr::Literal(Literal::Array(vec![
				Expr::Literal(Literal::Integer(1)),
				Expr::Literal(Literal::Integer(2)),
				Expr::Literal(Literal::Integer(3)),
			])))],
		};
		let result = optimiser.optimise(plan).unwrap();
		match &result.expressions[0] {
			TopLevelExpr::Expr(Expr::Value(Value::Array(arr))) => {
				assert_eq!(arr.len(), 3);
			}
			other => panic!("Expected Value(Array), got {:?}", other),
		}
	}

	#[test]
	fn test_static_literal_folding_object() {
		let optimiser = Optimiser::new().add_rule(Box::new(StaticLiteralFolding));
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(Expr::Literal(Literal::Object(vec![
				ObjectEntry {
					key: "a".to_string(),
					value: Expr::Literal(Literal::Integer(5)),
				},
			])))],
		};
		let result = optimiser.optimise(plan).unwrap();
		match &result.expressions[0] {
			TopLevelExpr::Expr(Expr::Value(Value::Object(obj))) => {
				assert_eq!(obj.get("a"), Some(&Value::Number(Number::Int(5))));
			}
			other => panic!("Expected Value(Object), got {:?}", other),
		}
	}

	#[test]
	fn test_constant_folding_add() {
		let optimiser = Optimiser::new().add_rule(Box::new(ConstantFolding));
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(Expr::Binary {
				left: Box::new(Expr::Value(Value::Number(Number::Int(1)))),
				op: BinaryOperator::Add,
				right: Box::new(Expr::Value(Value::Number(Number::Int(2)))),
			})],
		};
		let result = optimiser.optimise(plan).unwrap();
		match &result.expressions[0] {
			TopLevelExpr::Expr(Expr::Value(Value::Number(Number::Int(3)))) => (),
			other => panic!("Expected Value(3), got {:?}", other),
		}
	}

	#[test]
	fn test_constant_folding_multiply() {
		let optimiser = Optimiser::new().add_rule(Box::new(ConstantFolding));
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(Expr::Binary {
				left: Box::new(Expr::Value(Value::Number(Number::Int(10)))),
				op: BinaryOperator::Multiply,
				right: Box::new(Expr::Value(Value::Number(Number::Int(5)))),
			})],
		};
		let result = optimiser.optimise(plan).unwrap();
		match &result.expressions[0] {
			TopLevelExpr::Expr(Expr::Value(Value::Number(Number::Int(50)))) => (),
			other => panic!("Expected Value(50), got {:?}", other),
		}
	}

	#[test]
	fn test_constant_folding_nested() {
		let optimiser = Optimiser::new().add_rule(Box::new(ConstantFolding));
		// (1 + 2) * 3 = 9
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(Expr::Binary {
				left: Box::new(Expr::Binary {
					left: Box::new(Expr::Value(Value::Number(Number::Int(1)))),
					op: BinaryOperator::Add,
					right: Box::new(Expr::Value(Value::Number(Number::Int(2)))),
				}),
				op: BinaryOperator::Multiply,
				right: Box::new(Expr::Value(Value::Number(Number::Int(3)))),
			})],
		};
		let result = optimiser.optimise(plan).unwrap();
		match &result.expressions[0] {
			TopLevelExpr::Expr(Expr::Value(Value::Number(Number::Int(9)))) => (),
			other => panic!("Expected Value(9), got {:?}", other),
		}
	}

	#[test]
	fn test_constant_folding_comparison() {
		let optimiser = Optimiser::new().add_rule(Box::new(ConstantFolding));
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(Expr::Binary {
				left: Box::new(Expr::Value(Value::Number(Number::Int(5)))),
				op: BinaryOperator::MoreThan,
				right: Box::new(Expr::Value(Value::Number(Number::Int(3)))),
			})],
		};
		let result = optimiser.optimise(plan).unwrap();
		match &result.expressions[0] {
			TopLevelExpr::Expr(Expr::Value(Value::Bool(true))) => (),
			other => panic!("Expected Value(true), got {:?}", other),
		}
	}

	#[test]
	fn test_both_rules_together() {
		// Test that both rules work together: literals get folded to values,
		// then constant expressions get evaluated
		let optimiser = Optimiser::new()
			.add_rule(Box::new(StaticLiteralFolding))
			.add_rule(Box::new(ConstantFolding));

		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(Expr::Binary {
				left: Box::new(Expr::Literal(Literal::Integer(1))),
				op: BinaryOperator::Add,
				right: Box::new(Expr::Literal(Literal::Integer(2))),
			})],
		};
		let result = optimiser.optimise(plan).unwrap();
		match &result.expressions[0] {
			TopLevelExpr::Expr(Expr::Value(Value::Number(Number::Int(3)))) => (),
			other => panic!("Expected Value(3), got {:?}", other),
		}
	}

	#[test]
	fn test_multiple_passes_converge() {
		// Test that multiple passes work correctly
		let optimiser = Optimiser::new()
			.add_rule(Box::new(StaticLiteralFolding))
			.add_rule(Box::new(ConstantFolding))
			.with_max_passes(5);

		// ((1 + 2) * 3) + 4 = 13
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(Expr::Binary {
				left: Box::new(Expr::Binary {
					left: Box::new(Expr::Binary {
						left: Box::new(Expr::Literal(Literal::Integer(1))),
						op: BinaryOperator::Add,
						right: Box::new(Expr::Literal(Literal::Integer(2))),
					}),
					op: BinaryOperator::Multiply,
					right: Box::new(Expr::Literal(Literal::Integer(3))),
				}),
				op: BinaryOperator::Add,
				right: Box::new(Expr::Literal(Literal::Integer(4))),
			})],
		};
		let result = optimiser.optimise(plan).unwrap();
		match &result.expressions[0] {
			TopLevelExpr::Expr(Expr::Value(Value::Number(Number::Int(13)))) => (),
			other => panic!("Expected Value(13), got {:?}", other),
		}
	}

	#[test]
	fn test_non_static_literal_unchanged() {
		// Literals with parameters should not be folded
		let optimiser = Optimiser::new().add_rule(Box::new(StaticLiteralFolding));
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(Expr::Literal(Literal::Array(vec![
				Expr::Param(crate::expr::Param::new("x".to_string())),
			])))],
		};
		let result = optimiser.optimise(plan).unwrap();
		match &result.expressions[0] {
			TopLevelExpr::Expr(Expr::Literal(Literal::Array(_))) => (),
			other => panic!("Expected Literal(Array) with param, got {:?}", other),
		}
	}

	#[test]
	fn test_constant_expression() {
		let optimiser = Optimiser::new().add_rule(Box::new(ConstantFolding));
		let plan = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(Expr::Constant(crate::expr::Constant::MathPi))],
		};
		let result = optimiser.optimise(plan).unwrap();
		match &result.expressions[0] {
			TopLevelExpr::Expr(Expr::Value(Value::Number(Number::Float(f)))) => {
				assert!((f - std::f64::consts::PI).abs() < 1e-10);
			}
			other => panic!("Expected Value(pi), got {:?}", other),
		}
	}
}
