use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, CombineAccessModes, ContextLevel};
use crate::expr::FlowResult;
use crate::expr::record_id::RecordIdKeyGen;
use crate::fmt::EscapeRidKey;
use crate::val::{Array, Object, RecordId, RecordIdKey, RecordIdKeyRange, TableName, Uuid, Value};

// ============================================================================
// PhysicalRecordIdKey
// ============================================================================

/// A record ID key that may contain dynamic expressions requiring runtime evaluation.
///
/// Mirrors `RecordIdKeyLit` from the expression layer, but Array and Object
/// variants hold physical expression children instead of raw `Expr` nodes.
#[derive(Debug)]
pub enum PhysicalRecordIdKey {
	Number(i64),
	String(String),
	Uuid(Uuid),
	Generate(RecordIdKeyGen),
	Array(Vec<Arc<dyn PhysicalExpr>>),
	Object(Vec<(String, Arc<dyn PhysicalExpr>)>),
	Range {
		start: Bound<Box<PhysicalRecordIdKey>>,
		end: Bound<Box<PhysicalRecordIdKey>>,
	},
}

impl PhysicalRecordIdKey {
	/// Evaluate this key to a concrete `RecordIdKey` value.
	///
	/// Uses `Box::pin` for the Range variant to break the recursive async cycle
	/// (Range bounds are themselves `PhysicalRecordIdKey`).
	pub fn evaluate<'a>(
		&'a self,
		ctx: EvalContext<'a>,
	) -> crate::exec::BoxFut<'a, FlowResult<RecordIdKey>> {
		Box::pin(async move {
			match self {
				PhysicalRecordIdKey::Number(n) => Ok(RecordIdKey::Number(*n)),
				PhysicalRecordIdKey::String(s) => Ok(RecordIdKey::String(s.clone())),
				PhysicalRecordIdKey::Uuid(u) => Ok(RecordIdKey::Uuid(*u)),
				PhysicalRecordIdKey::Generate(generator) => Ok(generator.compute()),
				PhysicalRecordIdKey::Array(elements) => {
					let mut values = Vec::with_capacity(elements.len());
					for elem in elements {
						values.push(elem.evaluate(ctx.clone()).await?);
					}
					Ok(RecordIdKey::Array(Array(values)))
				}
				PhysicalRecordIdKey::Object(entries) => {
					let mut obj = Object::default();
					for (key, expr) in entries {
						let value = expr.evaluate(ctx.clone()).await?;
						obj.insert(key.clone(), value);
					}
					Ok(RecordIdKey::Object(obj))
				}
				PhysicalRecordIdKey::Range {
					start,
					end,
				} => {
					let start = evaluate_bound(start, ctx.clone()).await?;
					let end = evaluate_bound(end, ctx).await?;
					Ok(RecordIdKey::Range(Box::new(RecordIdKeyRange {
						start,
						end,
					})))
				}
			}
		})
	}

	/// Returns the maximum context level required by any child expression.
	fn required_context(&self) -> ContextLevel {
		match self {
			PhysicalRecordIdKey::Number(_)
			| PhysicalRecordIdKey::String(_)
			| PhysicalRecordIdKey::Uuid(_)
			| PhysicalRecordIdKey::Generate(_) => ContextLevel::Root,
			PhysicalRecordIdKey::Array(elements) => {
				elements.iter().map(|e| e.required_context()).max().unwrap_or(ContextLevel::Root)
			}
			PhysicalRecordIdKey::Object(entries) => entries
				.iter()
				.map(|(_, e)| e.required_context())
				.max()
				.unwrap_or(ContextLevel::Root),
			PhysicalRecordIdKey::Range {
				start,
				end,
			} => {
				let s = bound_required_context(start);
				let e = bound_required_context(end);
				s.max(e)
			}
		}
	}

	/// Returns the combined access mode of all child expressions.
	fn access_mode(&self) -> AccessMode {
		match self {
			PhysicalRecordIdKey::Number(_)
			| PhysicalRecordIdKey::String(_)
			| PhysicalRecordIdKey::Uuid(_)
			| PhysicalRecordIdKey::Generate(_) => AccessMode::ReadOnly,
			PhysicalRecordIdKey::Array(elements) => {
				elements.iter().map(|e| e.access_mode()).combine_all()
			}
			PhysicalRecordIdKey::Object(entries) => {
				entries.iter().map(|(_, e)| e.access_mode()).combine_all()
			}
			PhysicalRecordIdKey::Range {
				start,
				end,
			} => {
				let s = bound_access_mode(start);
				let e = bound_access_mode(end);
				[s, e].into_iter().combine_all()
			}
		}
	}
}

// ============================================================================
// Bound helpers
// ============================================================================

async fn evaluate_bound(
	bound: &Bound<Box<PhysicalRecordIdKey>>,
	ctx: EvalContext<'_>,
) -> FlowResult<Bound<RecordIdKey>> {
	match bound {
		Bound::Unbounded => Ok(Bound::Unbounded),
		Bound::Included(k) => Ok(Bound::Included(k.evaluate(ctx).await?)),
		Bound::Excluded(k) => Ok(Bound::Excluded(k.evaluate(ctx).await?)),
	}
}

fn bound_required_context(bound: &Bound<Box<PhysicalRecordIdKey>>) -> ContextLevel {
	match bound {
		Bound::Unbounded => ContextLevel::Root,
		Bound::Included(k) | Bound::Excluded(k) => k.required_context(),
	}
}

fn bound_access_mode(bound: &Bound<Box<PhysicalRecordIdKey>>) -> AccessMode {
	match bound {
		Bound::Unbounded => AccessMode::ReadOnly,
		Bound::Included(k) | Bound::Excluded(k) => k.access_mode(),
	}
}

// ============================================================================
// RecordIdExpr
// ============================================================================

/// Physical expression for a record ID literal (`table:key`).
///
/// Evaluates the key (which may contain dynamic expressions in Array, Object,
/// or Range variants) at runtime and produces a `Value::RecordId`.
#[derive(Debug)]
pub struct RecordIdExpr {
	pub(crate) table: TableName,
	pub(crate) key: PhysicalRecordIdKey,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for RecordIdExpr {
	fn name(&self) -> &'static str {
		"RecordIdExpr"
	}

	fn required_context(&self) -> ContextLevel {
		self.key.required_context()
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let key = self.key.evaluate(ctx).await?;
		Ok(Value::RecordId(RecordId {
			table: self.table.clone(),
			key,
		}))
	}

	fn access_mode(&self) -> AccessMode {
		self.key.access_mode()
	}
}

impl ToSql for RecordIdExpr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		// Use EscapeRidKey for the table name to match Value::RecordId rendering.
		EscapeRidKey(&self.table).fmt_sql(f, fmt);
		f.push(':');
		self.key.fmt_sql(f, fmt);
	}
}

impl ToSql for PhysicalRecordIdKey {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			PhysicalRecordIdKey::Number(v) => write_sql!(f, fmt, "{v}"),
			PhysicalRecordIdKey::String(v) => EscapeRidKey(v).fmt_sql(f, fmt),
			PhysicalRecordIdKey::Uuid(v) => v.fmt_sql(f, fmt),
			PhysicalRecordIdKey::Generate(v) => match v {
				RecordIdKeyGen::Rand => f.push_str("rand()"),
				RecordIdKeyGen::Ulid => f.push_str("ulid()"),
				RecordIdKeyGen::Uuid => f.push_str("uuid()"),
			},
			PhysicalRecordIdKey::Array(elements) => {
				f.push('[');
				for (i, elem) in elements.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					elem.fmt_sql(f, fmt);
				}
				f.push(']');
			}
			PhysicalRecordIdKey::Object(entries) => {
				f.push_str("{ ");
				for (i, (key, expr)) in entries.iter().enumerate() {
					if i > 0 {
						f.push_str(", ");
					}
					write_sql!(f, fmt, "{}: {}", key, expr);
				}
				f.push_str(" }");
			}
			PhysicalRecordIdKey::Range {
				start,
				end,
			} => {
				match start {
					Bound::Unbounded => {}
					Bound::Included(v) => v.fmt_sql(f, fmt),
					Bound::Excluded(v) => {
						v.fmt_sql(f, fmt);
						f.push('>');
					}
				}
				match end {
					Bound::Unbounded => f.push_str(".."),
					Bound::Excluded(v) => {
						f.push_str("..");
						v.fmt_sql(f, fmt);
					}
					Bound::Included(v) => {
						f.push_str("..=");
						v.fmt_sql(f, fmt);
					}
				}
			}
		}
	}
}
