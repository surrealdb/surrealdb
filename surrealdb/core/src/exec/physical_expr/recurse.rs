//! Recursive traversal expressions.
//!
//! Note: This module is work-in-progress for recursive graph traversal.
#![allow(dead_code)]

use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::AccessMode;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::physical_part::PhysicalRecurseInstruction;
use crate::val::Value;

// ============================================================================
// RecurseExpr - Recursive traversal expression
// ============================================================================

/// Recursion expression that evaluates bounded/unbounded graph traversal.
///
/// This expression handles recursion patterns like `{1..5}->knows->person`
/// with various instructions (collect, path, shortest).
#[derive(Debug, Clone)]
pub struct RecurseExpr {
	/// Minimum recursion depth (default 1)
	pub(crate) min_depth: u32,

	/// Maximum recursion depth (None = unbounded up to system limit)
	pub(crate) max_depth: Option<u32>,

	/// The path expression to evaluate at each recursion step
	pub(crate) path_expr: Arc<dyn PhysicalExpr>,

	/// The recursion instruction
	pub(crate) instruction: PhysicalRecurseInstruction,

	/// Whether to include the starting node in results
	pub(crate) inclusive: bool,
}

#[async_trait]
impl PhysicalExpr for RecurseExpr {
	fn name(&self) -> &'static str {
		"RecurseExpr"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		use crate::exec::ContextLevel;
		use crate::exec::physical_part::PhysicalRecurseInstruction;

		// Combine path_expr context with instruction context
		let path_ctx = self.path_expr.required_context();

		let instruction_ctx = match &self.instruction {
			PhysicalRecurseInstruction::Default
			| PhysicalRecurseInstruction::Collect
			| PhysicalRecurseInstruction::Path => ContextLevel::Root,
			PhysicalRecurseInstruction::Shortest {
				target,
			} => target.required_context(),
		};

		path_ctx.max(instruction_ctx)
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		let current =
			ctx.current_value.ok_or_else(|| anyhow::anyhow!("Recursion requires current value"))?;

		// Implement recursion based on instruction type
		match &self.instruction {
			PhysicalRecurseInstruction::Default => self.evaluate_default(current, ctx).await,
			PhysicalRecurseInstruction::Collect => self.evaluate_collect(current, ctx).await,
			PhysicalRecurseInstruction::Path => self.evaluate_path(current, ctx).await,
			PhysicalRecurseInstruction::Shortest {
				target,
			} => {
				let target_value = target.evaluate(ctx.clone()).await?;
				self.evaluate_shortest(current, &target_value, ctx).await
			}
		}
	}

	fn references_current_value(&self) -> bool {
		true
	}

	fn access_mode(&self) -> AccessMode {
		let path_mode = self.path_expr.access_mode();

		let instruction_mode = match &self.instruction {
			PhysicalRecurseInstruction::Default
			| PhysicalRecurseInstruction::Collect
			| PhysicalRecurseInstruction::Path => AccessMode::ReadOnly,
			PhysicalRecurseInstruction::Shortest {
				target,
			} => target.access_mode(),
		};

		path_mode.combine(instruction_mode)
	}
}

impl RecurseExpr {
	/// Default recursion: keep following the path until bounds or dead end.
	async fn evaluate_default(&self, start: &Value, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		let max_depth = self.max_depth.unwrap_or(100); // System limit
		let mut current = start.clone();
		let mut depth = 0u32;

		while depth < max_depth {
			// Evaluate the path expression on the current value
			let next_ctx = ctx.with_value(&current);
			let next = self.path_expr.evaluate(next_ctx).await?;

			depth += 1;

			// Check termination
			if matches!(next, Value::None) || next == current {
				break;
			}

			// Check if we've reached minimum depth
			if depth >= self.min_depth {
				current = next;
				break;
			}

			current = next;
		}

		// Return final value if depth is within bounds
		if depth >= self.min_depth {
			Ok(current)
		} else {
			Ok(Value::None)
		}
	}

	/// Collect: gather all unique nodes encountered during traversal.
	async fn evaluate_collect(&self, start: &Value, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		let max_depth = self.max_depth.unwrap_or(100);
		let mut collected = Vec::new();
		let mut seen = std::collections::HashSet::new();
		let mut frontier = vec![start.clone()];

		if self.inclusive {
			collected.push(start.clone());
			seen.insert(value_hash(start));
		}

		let mut depth = 0u32;

		while depth < max_depth && !frontier.is_empty() {
			let mut next_frontier = Vec::new();

			for value in frontier {
				let value_ctx = ctx.with_value(&value);
				let result = self.path_expr.evaluate(value_ctx).await?;

				// Process result (may be single value or array)
				let values = match result {
					Value::Array(arr) => arr.iter().cloned().collect::<Vec<_>>(),
					Value::None => continue,
					other => vec![other],
				};

				for v in values {
					let hash = value_hash(&v);
					if !seen.contains(&hash) {
						seen.insert(hash);
						if depth + 1 >= self.min_depth {
							collected.push(v.clone());
						}
						next_frontier.push(v);
					}
				}
			}

			frontier = next_frontier;
			depth += 1;
		}

		Ok(Value::Array(collected.into()))
	}

	/// Path: return all paths as arrays of arrays.
	async fn evaluate_path(&self, start: &Value, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		let max_depth = self.max_depth.unwrap_or(100);
		let mut completed_paths = Vec::new();
		let mut active_paths: Vec<Vec<Value>> = if self.inclusive {
			vec![vec![start.clone()]]
		} else {
			vec![vec![]]
		};

		let mut depth = 0u32;

		while depth < max_depth && !active_paths.is_empty() {
			let mut next_paths = Vec::new();

			for path in active_paths {
				let current = path.last().unwrap_or(start);
				let value_ctx = ctx.with_value(current);
				let result = self.path_expr.evaluate(value_ctx).await?;

				let values = match result {
					Value::Array(arr) => arr.iter().cloned().collect::<Vec<_>>(),
					Value::None => {
						// Dead end - this path is complete
						if depth >= self.min_depth && !path.is_empty() {
							completed_paths.push(Value::Array(path.into()));
						}
						continue;
					}
					other => vec![other],
				};

				if values.is_empty() {
					// Dead end
					if depth >= self.min_depth && !path.is_empty() {
						completed_paths.push(Value::Array(path.into()));
					}
				} else {
					for v in values {
						let mut new_path = path.clone();
						new_path.push(v);
						next_paths.push(new_path);
					}
				}
			}

			active_paths = next_paths;
			depth += 1;
		}

		// Add any remaining active paths that reached max depth
		for path in active_paths {
			if !path.is_empty() {
				completed_paths.push(Value::Array(path.into()));
			}
		}

		Ok(Value::Array(completed_paths.into()))
	}

	/// Shortest: find the shortest path to a target node.
	async fn evaluate_shortest(
		&self,
		start: &Value,
		target: &Value,
		ctx: EvalContext<'_>,
	) -> anyhow::Result<Value> {
		let max_depth = self.max_depth.unwrap_or(100);
		let mut seen = std::collections::HashSet::new();

		// BFS with path tracking
		let initial_path = if self.inclusive {
			vec![start.clone()]
		} else {
			vec![]
		};
		let mut queue = std::collections::VecDeque::new();
		queue.push_back((start.clone(), initial_path));
		seen.insert(value_hash(start));

		let mut depth = 0u32;

		while depth < max_depth && !queue.is_empty() {
			let level_size = queue.len();

			for _ in 0..level_size {
				let (current, path) = queue.pop_front().unwrap();

				let value_ctx = ctx.with_value(&current);
				let result = self.path_expr.evaluate(value_ctx).await?;

				let values = match result {
					Value::Array(arr) => arr.iter().cloned().collect::<Vec<_>>(),
					Value::None => continue,
					other => vec![other],
				};

				for v in values {
					// Check if we found the target
					if &v == target {
						let mut final_path = path.clone();
						final_path.push(v);
						return Ok(Value::Array(final_path.into()));
					}

					let hash = value_hash(&v);
					if !seen.contains(&hash) {
						seen.insert(hash);
						let mut new_path = path.clone();
						new_path.push(v.clone());
						queue.push_back((v, new_path));
					}
				}
			}

			depth += 1;
		}

		// Target not found
		Ok(Value::None)
	}
}

impl ToSql for RecurseExpr {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(".{");
		if self.min_depth > 1 {
			f.push_str(&self.min_depth.to_string());
		}
		f.push_str("..");
		if let Some(max) = self.max_depth {
			f.push_str(&max.to_string());
		}

		match &self.instruction {
			PhysicalRecurseInstruction::Default => {}
			PhysicalRecurseInstruction::Collect => f.push_str("+collect"),
			PhysicalRecurseInstruction::Path => f.push_str("+path"),
			PhysicalRecurseInstruction::Shortest {
				..
			} => f.push_str("+shortest=..."),
		}

		if self.inclusive {
			f.push_str("+inclusive");
		}

		f.push('}');
	}
}

/// Helper function to create a hash for value deduplication.
pub(crate) fn value_hash(value: &Value) -> u64 {
	use std::hash::{Hash, Hasher};
	let mut hasher = std::collections::hash_map::DefaultHasher::new();
	// Use the display representation as a proxy for equality
	format!("{:?}", value).hash(&mut hasher);
	hasher.finish()
}
