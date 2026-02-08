//! Physical representation of idiom parts for streaming execution.
//!
//! This module defines `PhysicalPart`, the physical execution equivalent of `expr::Part`.
//! Each part variant is designed for efficient evaluation within the streaming execution
//! context, without using the legacy `compute()` methods.

use std::sync::Arc;

use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::function::MethodDescriptor;
use crate::exec::{AccessMode, CombineAccessModes, ContextLevel, ExecOperator, PhysicalExpr};
use crate::expr::Dir;
use crate::expr::part::DestructurePart;
use crate::val::TableName;

/// Physical representation of an idiom part for streaming execution.
///
/// This enum mirrors `expr::Part` but with pre-converted physical expressions
/// and operator plans for efficient evaluation.
#[derive(Debug, Clone)]
pub enum PhysicalPart {
	/// Simple field access on an object - `foo`
	Field(String),

	/// Computed index access - `[expr]`
	Index(Arc<dyn PhysicalExpr>),

	/// All elements - `[*]` or `.*`
	All,

	/// Flatten nested arrays - `...` or flatten operation
	Flatten,

	/// First element - `[$]` or `.first()`
	First,

	/// Last element - `[~]` or `.last()`
	Last,

	/// Filter predicate on arrays - `[WHERE condition]`
	Where(Arc<dyn PhysicalExpr>),

	/// Method call on value - `.method(args...)`
	///
	/// The descriptor is resolved at plan time from the method name and contains
	/// per-type function dispatch information.
	Method {
		descriptor: Arc<MethodDescriptor>,
		args: Vec<Arc<dyn PhysicalExpr>>,
	},

	/// Closure field call - `.name(args...)` where `name` is not a known method.
	///
	/// When the method name is not found in the method registry at plan time,
	/// we fall back to field access + closure invocation at runtime.
	/// This handles patterns like `$obj.fnc()` where `fnc` is a field
	/// containing a closure value, not a built-in method.
	ClosureFieldCall {
		field: String,
		args: Vec<Arc<dyn PhysicalExpr>>,
	},

	/// Object destructuring - `{ field1, field2: alias, ... }`
	Destructure(Vec<PhysicalDestructurePart>),

	/// Optional chaining - `?.`
	Optional,

	/// Graph/reference lookup - `->table`, `<-table`, `<~table`
	/// Contains a pre-planned operator for the lookup operation
	Lookup(PhysicalLookup),

	/// Recursion with bounds and instruction - `{min..max+instruction}`
	Recurse(PhysicalRecurse),

	/// Repeat the current recursion from this point - `.@`
	///
	/// This is a marker that, when evaluated inside a recursion context,
	/// re-invokes the entire recursion on the current value. Used for
	/// building recursive tree structures via destructuring patterns like:
	/// `person:alice.{..}.{name, reports_to: ->reports_to->person.@}`
	RepeatRecurse,
}

impl PhysicalPart {
	/// Returns true if this part can be evaluated synchronously without I/O.
	pub fn is_simple(&self) -> bool {
		matches!(
			self,
			PhysicalPart::Field(_)
				| PhysicalPart::All
				| PhysicalPart::Flatten
				| PhysicalPart::First
				| PhysicalPart::Last
				| PhysicalPart::Optional
		)
	}

	/// Returns true if this part requires database access (Lookup, Recurse).
	pub fn requires_database(&self) -> bool {
		matches!(
			self,
			PhysicalPart::Lookup(_) | PhysicalPart::Recurse(_) | PhysicalPart::RepeatRecurse
		)
	}

	/// Returns the access mode for this part.
	pub fn access_mode(&self) -> AccessMode {
		match self {
			// Simple parts are read-only
			PhysicalPart::Field(_)
			| PhysicalPart::All
			| PhysicalPart::Flatten
			| PhysicalPart::First
			| PhysicalPart::Last
			| PhysicalPart::Optional
			| PhysicalPart::RepeatRecurse => AccessMode::ReadOnly,

			// Expression-based parts inherit from the expression
			PhysicalPart::Index(expr) => expr.access_mode(),
			PhysicalPart::Where(expr) => expr.access_mode(),
			PhysicalPart::Method {
				args,
				..
			} => args.iter().map(|a| a.access_mode()).combine_all(),

			// Closure field calls may do anything (closures can read/write)
			PhysicalPart::ClosureFieldCall {
				args,
				..
			} => AccessMode::ReadWrite.combine(args.iter().map(|a| a.access_mode()).combine_all()),

			// Destructure inherits from all parts
			PhysicalPart::Destructure(parts) => parts.iter().map(|p| p.access_mode()).combine_all(),

			// Lookup and Recurse inherit from their plans
			PhysicalPart::Lookup(lookup) => lookup.access_mode(),
			PhysicalPart::Recurse(recurse) => recurse.access_mode(),
		}
	}

	/// Returns the method name if this is a Method part.
	pub fn method_name(&self) -> Option<&str> {
		match self {
			PhysicalPart::Method {
				descriptor,
				..
			} => Some(descriptor.name),
			_ => None,
		}
	}

	/// Returns the required context level for this part.
	pub fn required_context(&self) -> ContextLevel {
		match self {
			// Simple parts don't need database access
			PhysicalPart::All
			| PhysicalPart::Flatten
			| PhysicalPart::First
			| PhysicalPart::Last
			| PhysicalPart::Optional => ContextLevel::Root,

			// Field access might need database (for RecordId auto-fetch)
			PhysicalPart::Field(_) => ContextLevel::Database,

			// Expression-based parts inherit from the expression
			PhysicalPart::Index(expr) => expr.required_context(),
			PhysicalPart::Where(expr) => expr.required_context(),
			PhysicalPart::Method {
				args,
				..
			} => args.iter().map(|a| a.required_context()).max().unwrap_or(ContextLevel::Database),

			// Closure field calls are conservative (closure body may need database)
			PhysicalPart::ClosureFieldCall {
				..
			} => ContextLevel::Database,

			// Destructure inherits from all parts
			PhysicalPart::Destructure(parts) => {
				parts.iter().map(|p| p.required_context()).max().unwrap_or(ContextLevel::Root)
			}

			// Lookup and Recurse require database access
			PhysicalPart::Lookup(_) | PhysicalPart::RepeatRecurse => ContextLevel::Database,
			PhysicalPart::Recurse(recurse) => recurse.required_context(),
		}
	}
}

impl ToSql for PhysicalPart {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			PhysicalPart::Field(name) => {
				f.push('.');
				f.push_str(name);
			}
			PhysicalPart::Index(expr) => {
				f.push('[');
				expr.fmt_sql(f, fmt);
				f.push(']');
			}
			PhysicalPart::All => f.push_str("[*]"),
			PhysicalPart::Flatten => f.push_str("..."),
			PhysicalPart::First => f.push_str("[$]"),
			PhysicalPart::Last => f.push_str("[~]"),
			PhysicalPart::Where(expr) => {
				f.push_str("[WHERE ");
				expr.fmt_sql(f, fmt);
				f.push(']');
			}
			PhysicalPart::Method {
				descriptor,
				args,
			} => {
				f.push('.');
				f.push_str(descriptor.name);
				f.push('(');
				for (i, arg) in args.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					arg.fmt_sql(f, fmt);
				}
				f.push(')');
			}
			PhysicalPart::ClosureFieldCall {
				field,
				args,
			} => {
				f.push('.');
				f.push_str(field);
				f.push('(');
				for (i, arg) in args.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					arg.fmt_sql(f, fmt);
				}
				f.push(')');
			}
			PhysicalPart::Destructure(parts) => {
				f.push_str(".{ ");
				for (i, part) in parts.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					part.fmt_sql(f, fmt);
				}
				f.push_str(" }");
			}
			PhysicalPart::Optional => f.push('?'),
			PhysicalPart::Lookup(lookup) => lookup.fmt_sql(f, fmt),
			PhysicalPart::Recurse(recurse) => recurse.fmt_sql(f, fmt),
			PhysicalPart::RepeatRecurse => f.push('@'),
		}
	}
}

// ============================================================================
// Destructure Part
// ============================================================================

/// Physical representation of a destructure part.
#[derive(Debug, Clone)]
pub enum PhysicalDestructurePart {
	/// Include all fields from field - `field.*`
	All(String),

	/// Simple field extraction - `field`
	Field(String),

	/// Aliased field - `field: alias_path`
	Aliased {
		field: String,
		path: Vec<PhysicalPart>,
	},

	/// Nested destructure - `field: { nested_field, ... }`
	Nested {
		field: String,
		parts: Vec<PhysicalDestructurePart>,
	},
}

impl PhysicalDestructurePart {
	/// Returns the field name for this destructure part.
	pub fn field_name(&self) -> &str {
		match self {
			PhysicalDestructurePart::All(f) => f,
			PhysicalDestructurePart::Field(f) => f,
			PhysicalDestructurePart::Aliased {
				field,
				..
			} => field,
			PhysicalDestructurePart::Nested {
				field,
				..
			} => field,
		}
	}

	/// Returns the access mode for this destructure part.
	pub fn access_mode(&self) -> AccessMode {
		match self {
			PhysicalDestructurePart::All(_) | PhysicalDestructurePart::Field(_) => {
				AccessMode::ReadOnly
			}
			PhysicalDestructurePart::Aliased {
				path,
				..
			} => path.iter().map(|p| p.access_mode()).combine_all(),
			PhysicalDestructurePart::Nested {
				parts,
				..
			} => parts.iter().map(|p| p.access_mode()).combine_all(),
		}
	}

	/// Returns the required context level for this destructure part.
	pub fn required_context(&self) -> ContextLevel {
		match self {
			// Simple field extraction doesn't need database
			PhysicalDestructurePart::All(_) | PhysicalDestructurePart::Field(_) => {
				ContextLevel::Root
			}
			// Aliased paths may require database (for lookups, etc.)
			PhysicalDestructurePart::Aliased {
				path,
				..
			} => path.iter().map(|p| p.required_context()).max().unwrap_or(ContextLevel::Root),
			// Nested destructures inherit from all parts
			PhysicalDestructurePart::Nested {
				parts,
				..
			} => parts.iter().map(|p| p.required_context()).max().unwrap_or(ContextLevel::Root),
		}
	}
}

impl ToSql for PhysicalDestructurePart {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			PhysicalDestructurePart::All(field) => {
				f.push_str(field);
				f.push_str(".*");
			}
			PhysicalDestructurePart::Field(field) => {
				f.push_str(field);
			}
			PhysicalDestructurePart::Aliased {
				field,
				path,
			} => {
				f.push_str(field);
				f.push_str(": ");
				for part in path {
					part.fmt_sql(f, fmt);
				}
			}
			PhysicalDestructurePart::Nested {
				field,
				parts,
			} => {
				f.push_str(field);
				f.push_str(": { ");
				for (i, part) in parts.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					part.fmt_sql(f, fmt);
				}
				f.push_str(" }");
			}
		}
	}
}

// ============================================================================
// Lookup
// ============================================================================

/// Physical representation of a graph/reference lookup operation.
///
/// This wraps a pre-planned operator tree that performs the lookup,
/// including any filtering, sorting, limiting, and projection.
#[derive(Debug, Clone)]
pub struct PhysicalLookup {
	/// The direction of the lookup (In, Out, Both for graph; Reference for <~)
	pub direction: LookupDirection,

	/// The edge/reference tables to scan (e.g., `knows`, `follows`)
	/// Empty means scan all tables in that direction.
	pub edge_tables: Vec<TableName>,

	/// The pre-planned operator tree for executing the lookup.
	/// This includes GraphEdgeScan/ReferenceScan + optional Filter, Sort, Limit, Project.
	pub plan: Arc<dyn ExecOperator>,

	/// When true, extract just the RecordId from result objects.
	/// This is set when the scan uses FullEdge mode for WHERE/SPLIT filtering
	/// but no explicit SELECT clause is present, so the final result should be
	/// RecordIds rather than full objects.
	pub extract_id: bool,
}

/// Direction for lookup operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupDirection {
	/// Outgoing edges: `->`
	Out,
	/// Incoming edges: `<-`
	In,
	/// Both directions: `<->`
	Both,
	/// Record references: `<~`
	Reference,
}

impl From<Dir> for LookupDirection {
	fn from(dir: Dir) -> Self {
		match dir {
			Dir::Out => LookupDirection::Out,
			Dir::In => LookupDirection::In,
			Dir::Both => LookupDirection::Both,
		}
	}
}

impl From<&Dir> for LookupDirection {
	fn from(dir: &Dir) -> Self {
		match dir {
			Dir::Out => LookupDirection::Out,
			Dir::In => LookupDirection::In,
			Dir::Both => LookupDirection::Both,
		}
	}
}

impl PhysicalLookup {
	/// Returns the access mode for this lookup.
	pub fn access_mode(&self) -> AccessMode {
		self.plan.access_mode()
	}
}

impl ToSql for PhysicalLookup {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self.direction {
			LookupDirection::Out => f.push_str("->..."),
			LookupDirection::In => f.push_str("<-..."),
			LookupDirection::Both => f.push_str("<->..."),
			LookupDirection::Reference => f.push_str("<~..."),
		}
	}
}

// ============================================================================
// Recurse
// ============================================================================

/// Physical representation of a recursion operation.
#[derive(Debug, Clone)]
pub struct PhysicalRecurse {
	/// Minimum recursion depth (default 1)
	pub min_depth: u32,

	/// Maximum recursion depth (None = unbounded up to system limit)
	pub max_depth: Option<u32>,

	/// The path to traverse at each recursion step
	pub path: Vec<PhysicalPart>,

	/// The recursion instruction (how to collect results)
	pub instruction: PhysicalRecurseInstruction,

	/// Whether to include the starting node in results
	pub inclusive: bool,

	/// Whether the inner path contains RepeatRecurse markers.
	/// When true, the recursion uses a single-step evaluation where
	/// tree building is handled by the RepeatRecurse callbacks.
	pub has_repeat_recurse: bool,
}

/// Instruction for how to handle recursion results.
#[derive(Debug, Clone)]
pub enum PhysicalRecurseInstruction {
	/// Default: return the final values after recursion
	Default,

	/// Collect all unique nodes encountered during traversal
	Collect,

	/// Return all paths as arrays of arrays
	Path,

	/// Find shortest path to a target node
	Shortest {
		/// Expression that evaluates to the target RecordId
		target: Arc<dyn PhysicalExpr>,
	},
}

impl PhysicalRecurse {
	/// Returns the access mode for this recursion.
	pub fn access_mode(&self) -> AccessMode {
		let path_mode = self.path.iter().map(|p| p.access_mode()).combine_all();

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

	/// Returns the required context level for this recursion.
	pub fn required_context(&self) -> ContextLevel {
		let path_ctx =
			self.path.iter().map(|p| p.required_context()).max().unwrap_or(ContextLevel::Root);

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
}

impl ToSql for PhysicalRecurse {
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

// ============================================================================
// Conversion from expr::Part
// ============================================================================

/// Converts a logical `DestructurePart` to a physical `PhysicalDestructurePart`.
///
/// This is a shallow conversion - aliased paths need to be converted separately.
pub fn convert_destructure_part(part: &DestructurePart) -> PhysicalDestructurePart {
	match part {
		DestructurePart::All(field) => PhysicalDestructurePart::All(field.clone()),
		DestructurePart::Field(field) => PhysicalDestructurePart::Field(field.clone()),
		DestructurePart::Aliased(field, _idiom) => {
			// Note: The idiom path needs to be converted separately with full context
			// For now, we just create a placeholder that will be filled in by the planner
			PhysicalDestructurePart::Aliased {
				field: field.clone(),
				path: vec![], // Will be filled by planner
			}
		}
		DestructurePart::Destructure(field, parts) => PhysicalDestructurePart::Nested {
			field: field.clone(),
			parts: parts.iter().map(convert_destructure_part).collect(),
		},
	}
}
