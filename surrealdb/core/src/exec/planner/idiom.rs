//! Idiom and part conversion for the planner.
//!
//! Converts SurrealQL idiom AST nodes to physical parts for runtime evaluation.

use std::sync::Arc;

use super::Planner;
use crate::err::Error;
use crate::exec::physical_expr::IdiomExpr;
use crate::exec::physical_part::{
	PhysicalDestructurePart, PhysicalPart, PhysicalRecurse, PhysicalRecurseInstruction,
};
use crate::expr::part::{DestructurePart, Part, RecurseInstruction};

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
	) -> Result<Arc<dyn crate::exec::PhysicalExpr>, Error> {
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

	/// Convert idiom parts to physical parts.
	///
	/// When a `Part::Recurse` has no explicit inner path, all remaining parts
	/// after the recurse are absorbed as the recursion's inner path.
	pub(crate) fn convert_parts(&self, parts: Vec<Part>) -> Result<Vec<PhysicalPart>, Error> {
		let mut physical_parts = Vec::with_capacity(parts.len());
		let mut iter = parts.into_iter();

		while let Some(part) = iter.next() {
			if let Part::Recurse(recurse, None, instruction) = part {
				let (min_depth, max_depth) = match recurse {
					crate::expr::part::Recurse::Fixed(n) => (n, Some(n)),
					crate::expr::part::Recurse::Range(min, max) => (min.unwrap_or(1), max),
				};

				let remaining: Vec<Part> = iter.collect();
				let path = self.convert_parts(remaining)?;
				let inclusive = is_inclusive_recurse(&instruction);
				let instr = self.convert_recurse_instruction(instruction)?;
				let has_repeat_recurse = contains_repeat_recurse(&path);

				physical_parts.push(PhysicalPart::Recurse(PhysicalRecurse {
					min_depth,
					max_depth,
					path,
					instruction: instr,
					inclusive,
					has_repeat_recurse,
				}));

				break;
			}

			let physical_part = self.convert_part(part)?;
			physical_parts.push(physical_part);
		}

		Ok(physical_parts)
	}

	/// Convert a single `Part` to a `PhysicalPart`.
	pub(crate) fn convert_part(&self, part: Part) -> Result<PhysicalPart, Error> {
		use crate::exec::physical_part::{LookupDirection, PhysicalLookup};

		match part {
			Part::Field(name) => Ok(PhysicalPart::Field(name)),

			Part::Value(expr) => {
				let phys_expr = self.physical_expr(expr)?;
				Ok(PhysicalPart::Index(phys_expr))
			}

			Part::All => Ok(PhysicalPart::All),
			Part::Flatten => Ok(PhysicalPart::Flatten),
			Part::First => Ok(PhysicalPart::First),
			Part::Last => Ok(PhysicalPart::Last),
			Part::Optional => Ok(PhysicalPart::Optional),

			Part::Where(expr) => {
				let phys_expr = self.physical_expr(expr)?;
				Ok(PhysicalPart::Where(phys_expr))
			}

			Part::Method(name, args) => {
				let mut phys_args = Vec::with_capacity(args.len());
				for arg in args {
					phys_args.push(self.physical_expr(arg)?);
				}
				let registry = self.function_registry();
				match registry.get_method(&name) {
					Some(descriptor) => Ok(PhysicalPart::Method {
						descriptor: descriptor.clone(),
						args: phys_args,
					}),
					None => Ok(PhysicalPart::ClosureFieldCall {
						field: name,
						args: phys_args,
					}),
				}
			}

			Part::Destructure(parts) => {
				let phys_parts = self.convert_destructure(parts)?;
				Ok(PhysicalPart::Destructure(phys_parts))
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
				Ok(PhysicalPart::Lookup(PhysicalLookup {
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

				let path = if let Some(p) = inner_path {
					self.convert_parts(p.0)?
				} else {
					vec![]
				};

				let inclusive = is_inclusive_recurse(&instruction);
				let instr = self.convert_recurse_instruction(instruction)?;
				let has_repeat_recurse = contains_repeat_recurse(&path);

				Ok(PhysicalPart::Recurse(PhysicalRecurse {
					min_depth,
					max_depth,
					path,
					instruction: instr,
					inclusive,
					has_repeat_recurse,
				}))
			}

			Part::Doc => Ok(PhysicalPart::Field("id".to_string())),

			Part::RepeatRecurse => Ok(PhysicalPart::RepeatRecurse),
		}
	}

	/// Convert destructure parts to physical destructure parts.
	pub(crate) fn convert_destructure(
		&self,
		parts: Vec<DestructurePart>,
	) -> Result<Vec<PhysicalDestructurePart>, Error> {
		let mut physical_parts = Vec::with_capacity(parts.len());

		for part in parts {
			let phys_part = match part {
				DestructurePart::All(field) => PhysicalDestructurePart::All(field),
				DestructurePart::Field(field) => PhysicalDestructurePart::Field(field),
				DestructurePart::Aliased(field, idiom) => {
					let path = self.convert_parts(idiom.0)?;
					PhysicalDestructurePart::Aliased {
						field,
						path,
					}
				}
				DestructurePart::Destructure(field, nested) => {
					let nested_parts = self.convert_destructure(nested)?;
					PhysicalDestructurePart::Nested {
						field,
						parts: nested_parts,
					}
				}
			};
			physical_parts.push(phys_part);
		}

		Ok(physical_parts)
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

/// Check if a slice of physical parts contains a `RepeatRecurse` marker at any nesting level.
pub(super) fn contains_repeat_recurse(parts: &[PhysicalPart]) -> bool {
	for part in parts {
		match part {
			PhysicalPart::RepeatRecurse => return true,
			PhysicalPart::Destructure(dest_parts) => {
				for dp in dest_parts {
					if let PhysicalDestructurePart::Aliased {
						path,
						..
					} = dp
					{
						if contains_repeat_recurse(path) {
							return true;
						}
					}
					if let PhysicalDestructurePart::Nested {
						parts: nested,
						..
					} = dp
					{
						for np in nested {
							if let PhysicalDestructurePart::Aliased {
								path,
								..
							} = np
							{
								if contains_repeat_recurse(path) {
									return true;
								}
							}
						}
					}
				}
			}
			PhysicalPart::Recurse(recurse) => {
				if contains_repeat_recurse(&recurse.path) {
					return true;
				}
			}
			_ => {}
		}
	}
	false
}
