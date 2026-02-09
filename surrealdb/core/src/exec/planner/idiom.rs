//! Idiom and part conversion for the planner.
//!
//! Converts SurrealQL idiom AST nodes to physical part expressions for runtime evaluation.
//! Each part is an `Arc<dyn PhysicalExpr>` that reads its input from `ctx.current_value`.

use std::sync::Arc;

use super::Planner;
use crate::err::Error;
use crate::exec::PhysicalExpr;
use crate::exec::parts::{
	AllPart, ClosureFieldCallPart, DestructureField, DestructurePart, FieldPart, FirstPart,
	FlattenPart, IndexPart, LastPart, LookupDirection, LookupPart, MethodPart, OptionalChainPart,
	PhysicalRecurseInstruction, RecursePart, RepeatRecursePart, WherePart,
};
use crate::exec::physical_expr::IdiomExpr;
use crate::expr::part::{DestructurePart as AstDestructurePart, Part, RecurseInstruction};

// ============================================================================
// impl Planner — Idiom Conversion
// ============================================================================

impl<'ctx> Planner<'ctx> {
	/// Convert an idiom to a physical expression.
	///
	/// If the first part is `Part::Start(expr)`, the expression is converted to a
	/// physical expression and stored as the start expression on `IdiomExpr`.
	pub(crate) fn convert_idiom(
		&self,
		idiom: crate::expr::idiom::Idiom,
	) -> Result<Arc<dyn PhysicalExpr>, Error> {
		use surrealdb_types::ToSql;

		// Pre-compute the display string before consuming the idiom's parts
		let display = idiom.to_sql();
		let mut parts = idiom.0;

		if let Some(Part::Start(_)) = parts.first() {
			let start_part = parts.remove(0);
			let Part::Start(start_expr) = start_part else {
				unreachable!()
			};
			let start_phys = self.physical_expr(start_expr)?;
			let remaining_parts = self.convert_parts(parts)?;
			return Ok(Arc::new(IdiomExpr::new(display, Some(start_phys), remaining_parts)));
		}

		let physical_parts = self.convert_parts(parts)?;
		Ok(Arc::new(IdiomExpr::new(display, None, physical_parts)))
	}

	/// Convert idiom parts to physical part expressions.
	///
	/// When a `Part::Recurse` has no explicit inner path, all remaining parts
	/// after the recurse are absorbed as the recursion's inner path.
	///
	/// This function also handles two planner-time responsibilities:
	/// 1. Inserts `FlattenPart` between consecutive `LookupPart`/`WherePart` pairs.
	/// 2. Wraps remaining parts in `OptionalChainPart` when encountering `Part::Optional`.
	pub(crate) fn convert_parts(
		&self,
		parts: Vec<Part>,
	) -> Result<Vec<Arc<dyn PhysicalExpr>>, Error> {
		let mut converted = Vec::with_capacity(parts.len());
		let mut iter = parts.into_iter();

		while let Some(part) = iter.next() {
			// Handle implicit recursion (Recurse with no inner path absorbs remaining parts)
			if let Part::Recurse(recurse, None, instruction) = part {
				let (min_depth, max_depth) = match recurse {
					crate::expr::part::Recurse::Fixed(n) => (n, Some(n)),
					crate::expr::part::Recurse::Range(min, max) => (min.unwrap_or(1), max),
				};

				let remaining: Vec<Part> = iter.collect();
				let has_repeat_recurse = ast_contains_repeat_recurse(&remaining);
				let path = self.convert_parts(remaining)?;
				let inclusive = is_inclusive_recurse(&instruction);
				let instr = self.convert_recurse_instruction(instruction)?;

				converted.push(Arc::new(RecursePart {
					min_depth,
					max_depth,
					path,
					instruction: instr,
					inclusive,
					has_repeat_recurse,
				}) as Arc<dyn PhysicalExpr>);

				break;
			}

			// Handle optional chaining -- wrap all remaining parts in OptionalChainPart
			if matches!(part, Part::Optional) {
				let remaining: Vec<Part> = iter.collect();
				let tail = self.convert_parts(remaining)?;
				converted.push(Arc::new(OptionalChainPart {
					tail,
				}) as Arc<dyn PhysicalExpr>);
				break;
			}

			let physical_part = self.convert_part(part)?;
			converted.push(physical_part);
		}

		// Insert FlattenPart between consecutive Lookup and Lookup/Where pairs.
		// This is a planner-time responsibility that replaces the runtime introspection
		// that was previously done in IdiomExpr's evaluation loop.
		let converted = insert_auto_flattens(converted);

		Ok(converted)
	}

	/// Convert a single `Part` to an `Arc<dyn PhysicalExpr>`.
	pub(crate) fn convert_part(&self, part: Part) -> Result<Arc<dyn PhysicalExpr>, Error> {
		match part {
			Part::Field(name) => Ok(Arc::new(FieldPart {
				name,
			})),

			Part::Value(expr) => {
				let phys_expr = self.physical_expr(expr)?;
				Ok(Arc::new(IndexPart {
					expr: phys_expr,
				}))
			}

			Part::All => Ok(Arc::new(AllPart)),
			Part::Flatten => Ok(Arc::new(FlattenPart)),
			Part::First => Ok(Arc::new(FirstPart)),
			Part::Last => Ok(Arc::new(LastPart)),

			Part::Optional => {
				// Standalone optional without remaining parts -- should not happen
				// normally because convert_parts handles it, but handle gracefully.
				Ok(Arc::new(OptionalChainPart {
					tail: vec![],
				}))
			}

			Part::Where(expr) => {
				let phys_expr = self.physical_expr(expr)?;
				Ok(Arc::new(WherePart {
					predicate: phys_expr,
				}))
			}

			Part::Method(name, args) => {
				let mut phys_args = Vec::with_capacity(args.len());
				for arg in args {
					phys_args.push(self.physical_expr(arg)?);
				}
				let registry = self.function_registry();
				match registry.get_method(&name) {
					Some(descriptor) => Ok(Arc::new(MethodPart {
						descriptor: descriptor.clone(),
						args: phys_args,
					})),
					None => Ok(Arc::new(ClosureFieldCallPart {
						field: name,
						args: phys_args,
					})),
				}
			}

			Part::Destructure(parts) => {
				let fields = self.convert_destructure(parts)?;
				Ok(Arc::new(DestructurePart {
					fields,
				}))
			}

			Part::Start(_) => Err(Error::Unimplemented(
				"Start parts should be handled at the idiom level".to_string(),
			)),

			Part::Lookup(lookup) => {
				// Extract metadata before consuming lookup
				let direction = match &lookup.kind {
					crate::expr::lookup::LookupKind::Graph(dir) => LookupDirection::from(dir),
					crate::expr::lookup::LookupKind::Reference => LookupDirection::Reference,
				};
				let edge_tables: Vec<_> = lookup
					.what
					.iter()
					.map(|s| match s {
						crate::expr::lookup::LookupSubject::Table {
							table,
							..
						} => table.clone(),
						crate::expr::lookup::LookupSubject::Range {
							table,
							..
						} => table.clone(),
					})
					.collect();
				let needs_full_pipeline = lookup.expr.is_some() || lookup.group.is_some();
				let needs_full_records =
					needs_full_pipeline || lookup.cond.is_some() || lookup.split.is_some();
				let extract_id = needs_full_records && !needs_full_pipeline;
				let plan = self.plan_lookup(lookup)?;
				Ok(Arc::new(LookupPart {
					direction,
					edge_tables,
					plan,
					extract_id,
				}))
			}

			Part::Recurse(recurse, inner_path, instruction) => {
				let (min_depth, max_depth) = match recurse {
					crate::expr::part::Recurse::Fixed(n) => (n, Some(n)),
					crate::expr::part::Recurse::Range(min, max) => (min.unwrap_or(1), max),
				};

				let (path, has_repeat_recurse) = if let Some(p) = inner_path {
					let has_rr = ast_contains_repeat_recurse(&p.0);
					let converted = self.convert_parts(p.0)?;
					(converted, has_rr)
				} else {
					(vec![], false)
				};

				let inclusive = is_inclusive_recurse(&instruction);
				let instr = self.convert_recurse_instruction(instruction)?;

				Ok(Arc::new(RecursePart {
					min_depth,
					max_depth,
					path,
					instruction: instr,
					inclusive,
					has_repeat_recurse,
				}))
			}

			Part::Doc => Ok(Arc::new(FieldPart {
				name: "id".to_string(),
			})),

			Part::RepeatRecurse => Ok(Arc::new(RepeatRecursePart)),
		}
	}

	/// Convert destructure parts to physical destructure fields.
	pub(crate) fn convert_destructure(
		&self,
		parts: Vec<AstDestructurePart>,
	) -> Result<Vec<DestructureField>, Error> {
		let mut fields = Vec::with_capacity(parts.len());

		for part in parts {
			let field = match part {
				AstDestructurePart::All(name) => DestructureField::All(name),
				AstDestructurePart::Field(name) => DestructureField::Field(name),
				AstDestructurePart::Aliased(name, idiom) => {
					let path = self.convert_parts(idiom.0)?;
					DestructureField::Aliased {
						field: name,
						path,
					}
				}
				AstDestructurePart::Destructure(name, nested) => {
					let nested_fields = self.convert_destructure(nested)?;
					DestructureField::Nested {
						field: name,
						parts: nested_fields,
					}
				}
			};
			fields.push(field);
		}

		Ok(fields)
	}

	/// Convert a `RecurseInstruction` to a `PhysicalRecurseInstruction`.
	pub(crate) fn convert_recurse_instruction(
		&self,
		instruction: Option<RecurseInstruction>,
	) -> Result<PhysicalRecurseInstruction, Error> {
		match instruction {
			None => Ok(PhysicalRecurseInstruction::Default),
			Some(RecurseInstruction::Collect {
				..
			}) => Ok(PhysicalRecurseInstruction::Collect),
			Some(RecurseInstruction::Path {
				..
			}) => Ok(PhysicalRecurseInstruction::Path),
			Some(RecurseInstruction::Shortest {
				expects,
				..
			}) => {
				let target = self.physical_expr(expects)?;
				Ok(PhysicalRecurseInstruction::Shortest {
					target,
				})
			}
		}
	}
}

// ============================================================================
// Free Functions
// ============================================================================

/// Check if a `RecurseInstruction` has the `inclusive` flag set.
fn is_inclusive_recurse(instruction: &Option<RecurseInstruction>) -> bool {
	matches!(
		instruction,
		Some(RecurseInstruction::Path {
			inclusive: true,
			..
		}) | Some(RecurseInstruction::Collect {
			inclusive: true,
			..
		}) | Some(RecurseInstruction::Shortest {
			inclusive: true,
			..
		})
	)
}

/// Insert `FlattenPart` between consecutive `LookupPart` and `LookupPart`/`WherePart` pairs.
///
/// This is a planner-time transformation that replaces the runtime introspection
/// that was previously done in `IdiomExpr`'s evaluation loop and `evaluate_physical_path`.
fn insert_auto_flattens(parts: Vec<Arc<dyn PhysicalExpr>>) -> Vec<Arc<dyn PhysicalExpr>> {
	if parts.len() < 2 {
		return parts;
	}

	let mut result = Vec::with_capacity(parts.len() * 2);
	for i in 0..parts.len() {
		result.push(parts[i].clone());
		if parts[i].name() == "Lookup" {
			if let Some(next) = parts.get(i + 1) {
				if next.name() == "Lookup" || next.name() == "Where" {
					result.push(Arc::new(FlattenPart) as Arc<dyn PhysicalExpr>);
				}
			}
		}
	}
	result
}

/// Check if a slice of AST parts contains a `RepeatRecurse` marker at any nesting level.
///
/// This operates on the AST representation (before conversion to PhysicalExpr)
/// so it has full access to the part structure without needing downcasting.
fn ast_contains_repeat_recurse(parts: &[Part]) -> bool {
	for part in parts {
		match part {
			Part::RepeatRecurse => return true,
			Part::Destructure(dest_parts) => {
				for dp in dest_parts {
					if let AstDestructurePart::Aliased(_, idiom) = dp {
						if ast_contains_repeat_recurse(&idiom.0) {
							return true;
						}
					}
					if let AstDestructurePart::Destructure(_, nested) = dp {
						for np in nested {
							if let AstDestructurePart::Aliased(_, idiom) = np {
								if ast_contains_repeat_recurse(&idiom.0) {
									return true;
								}
							}
						}
					}
				}
			}
			Part::Recurse(_, Some(inner_path), _) => {
				if ast_contains_repeat_recurse(&inner_path.0) {
					return true;
				}
			}
			_ => {}
		}
	}
	false
}
