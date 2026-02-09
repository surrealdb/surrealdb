//! Pure utility functions for the planner.
//!
//! These functions have no dependency on `Planner` or `FrozenContext` and perform
//! static conversions, validation, or predicate checks.

use crate::err::Error;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::expr::field::{Field, Fields};
use crate::expr::{BinaryOperator, Cond, Expr, Literal};

// ============================================================================
// Literal / Value Conversion
// ============================================================================

/// Convert a `RecordIdKeyLit` to a `RecordIdKey` for record ID construction.
pub(super) fn convert_record_key_lit(
	key_lit: &crate::expr::record_id::RecordIdKeyLit,
) -> Result<crate::val::RecordIdKey, Error> {
	use crate::expr::record_id::RecordIdKeyLit;
	use crate::val::RecordIdKey;

	match key_lit {
		RecordIdKeyLit::Number(n) => Ok(RecordIdKey::Number(*n)),
		RecordIdKeyLit::String(s) => Ok(RecordIdKey::String(s.clone())),
		RecordIdKeyLit::Uuid(u) => Ok(RecordIdKey::Uuid(*u)),
		RecordIdKeyLit::Generate(generator) => Ok(generator.compute()),
		RecordIdKeyLit::Array(exprs) => {
			let mut values = Vec::with_capacity(exprs.len());
			for expr in exprs {
				let value = static_expr_to_value(expr)?;
				values.push(value);
			}
			Ok(RecordIdKey::Array(crate::val::Array(values)))
		}
		RecordIdKeyLit::Object(entries) => {
			let mut obj = crate::val::Object::default();
			for entry in entries {
				let value = static_expr_to_value(&entry.value)?;
				obj.insert(entry.key.clone(), value);
			}
			Ok(RecordIdKey::Object(obj))
		}
		RecordIdKeyLit::Range(_) => Err(Error::Unimplemented(
			"Nested range record keys not supported in execution plans".to_string(),
		)),
	}
}

/// Convert a static `Expr` to a `Value` at plan time.
pub(super) fn static_expr_to_value(expr: &Expr) -> Result<crate::val::Value, Error> {
	match expr {
		Expr::Literal(lit) => literal_to_value(lit.clone()),
		_ => Err(Error::Unimplemented(
			"Dynamic expressions in record ID keys not yet supported in execution plans"
				.to_string(),
		)),
	}
}

/// Convert a `Literal` to a `Value` for static (non-computed) cases.
pub(super) fn literal_to_value(
	lit: crate::expr::literal::Literal,
) -> Result<crate::val::Value, Error> {
	use crate::expr::literal::Literal;
	use crate::val::{Number, Range, Value};

	match lit {
		Literal::None => Ok(Value::None),
		Literal::Null => Ok(Value::Null),
		Literal::UnboundedRange => Ok(Value::Range(Box::new(Range::unbounded()))),
		Literal::Bool(x) => Ok(Value::Bool(x)),
		Literal::Float(x) => Ok(Value::Number(Number::Float(x))),
		Literal::Integer(i) => Ok(Value::Number(Number::Int(i))),
		Literal::Decimal(d) => Ok(Value::Number(Number::Decimal(d))),
		Literal::String(s) => Ok(Value::String(s)),
		Literal::Bytes(b) => Ok(Value::Bytes(b)),
		Literal::Regex(r) => Ok(Value::Regex(r)),
		Literal::Duration(d) => Ok(Value::Duration(d)),
		Literal::Datetime(dt) => Ok(Value::Datetime(dt)),
		Literal::Uuid(u) => Ok(Value::Uuid(u)),
		Literal::Geometry(g) => Ok(Value::Geometry(g)),
		Literal::File(f) => Ok(Value::File(f)),
		Literal::RecordId(rid_lit) => {
			use std::ops::Bound;

			use crate::expr::record_id::RecordIdKeyLit;
			use crate::val::{RecordId, RecordIdKey, RecordIdKeyRange};

			let key = match &rid_lit.key {
				RecordIdKeyLit::Number(n) => RecordIdKey::Number(*n),
				RecordIdKeyLit::String(s) => RecordIdKey::String(s.clone()),
				RecordIdKeyLit::Uuid(u) => RecordIdKey::Uuid(*u),
				RecordIdKeyLit::Generate(generator) => generator.compute(),
				RecordIdKeyLit::Range(range_lit) => {
					let start = match &range_lit.start {
						Bound::Unbounded => Bound::Unbounded,
						Bound::Included(key_lit) => {
							Bound::Included(convert_record_key_lit(key_lit)?)
						}
						Bound::Excluded(key_lit) => {
							Bound::Excluded(convert_record_key_lit(key_lit)?)
						}
					};
					let end = match &range_lit.end {
						Bound::Unbounded => Bound::Unbounded,
						Bound::Included(key_lit) => {
							Bound::Included(convert_record_key_lit(key_lit)?)
						}
						Bound::Excluded(key_lit) => {
							Bound::Excluded(convert_record_key_lit(key_lit)?)
						}
					};
					RecordIdKey::Range(Box::new(RecordIdKeyRange {
						start,
						end,
					}))
				}
				RecordIdKeyLit::Array(_) | RecordIdKeyLit::Object(_) => {
					convert_record_key_lit(&rid_lit.key)?
				}
			};

			Ok(Value::RecordId(RecordId {
				table: rid_lit.table,
				key,
			}))
		}
		Literal::Array(_) => Err(Error::Unimplemented(
			"Array literals in USE statements not yet supported".to_string(),
		)),
		Literal::Set(_) => Err(Error::Unimplemented(
			"Set literals in USE statements not yet supported".to_string(),
		)),
		Literal::Object(_) => Err(Error::Unimplemented(
			"Object literals in USE statements not yet supported".to_string(),
		)),
	}
}

/// Convert a `RecordIdKeyLit` to an `Expr`.
pub(super) fn key_lit_to_expr(lit: &crate::expr::RecordIdKeyLit) -> Result<Expr, Error> {
	use crate::expr::RecordIdKeyLit;
	match lit {
		RecordIdKeyLit::Number(n) => Ok(Expr::Literal(crate::expr::literal::Literal::Integer(*n))),
		RecordIdKeyLit::String(s) => {
			Ok(Expr::Literal(crate::expr::literal::Literal::String(s.clone())))
		}
		RecordIdKeyLit::Uuid(u) => Ok(Expr::Literal(crate::expr::literal::Literal::Uuid(*u))),
		RecordIdKeyLit::Array(exprs) => {
			Ok(Expr::Literal(crate::expr::literal::Literal::Array(exprs.clone())))
		}
		RecordIdKeyLit::Object(entries) => {
			Ok(Expr::Literal(crate::expr::literal::Literal::Object(entries.clone())))
		}
		RecordIdKeyLit::Generate(_) | RecordIdKeyLit::Range(_) => {
			Err(Error::Unimplemented("Generated/range keys in graph range bounds".to_string()))
		}
	}
}

// ============================================================================
// Predicate / Validation Helpers
// ============================================================================

/// Check if an expression contains KNN (vector search) operators.
pub(super) fn contains_knn_operator(expr: &Expr) -> bool {
	match expr {
		Expr::Binary {
			left,
			op,
			right,
		} => {
			if matches!(op, BinaryOperator::NearestNeighbor(_)) {
				return true;
			}
			contains_knn_operator(left) || contains_knn_operator(right)
		}
		Expr::Prefix {
			expr: inner,
			..
		} => contains_knn_operator(inner),
		_ => false,
	}
}

/// Check if an expression contains MATCHES operators that cannot be indexed.
pub(super) fn contains_non_indexable_matches(expr: &Expr) -> bool {
	contains_matches_in_or(expr, false)
}

fn contains_matches_in_or(expr: &Expr, inside_or: bool) -> bool {
	match expr {
		Expr::Binary {
			left,
			op,
			right,
		} => {
			if inside_or && matches!(op, BinaryOperator::Matches(_)) {
				return true;
			}
			let new_inside_or = inside_or || matches!(op, BinaryOperator::Or);
			contains_matches_in_or(left, new_inside_or)
				|| contains_matches_in_or(right, new_inside_or)
		}
		Expr::Prefix {
			expr: inner,
			..
		} => contains_matches_in_or(inner, inside_or),
		_ => false,
	}
}

/// Check if a source expression represents a "value source" (array, primitive).
pub(super) fn is_value_source_expr(expr: &Expr) -> bool {
	match expr {
		Expr::Literal(Literal::Array(_)) => true,
		Expr::Literal(Literal::String(_))
		| Expr::Literal(Literal::Integer(_))
		| Expr::Literal(Literal::Float(_))
		| Expr::Literal(Literal::Decimal(_))
		| Expr::Literal(Literal::Bool(_))
		| Expr::Literal(Literal::None)
		| Expr::Literal(Literal::Null) => true,
		Expr::Table(_) => false,
		Expr::Literal(Literal::RecordId(_)) => false,
		Expr::Param(_) => false,
		Expr::Select(_) => false,
		_ => false,
	}
}

/// Check if ALL source expressions are value sources.
pub(super) fn all_value_sources(sources: &[Expr]) -> bool {
	!sources.is_empty() && sources.iter().all(is_value_source_expr)
}

// ============================================================================
// MATCHES Context Extraction
// ============================================================================

/// Extract MATCHES clause information from a WHERE condition for index functions.
pub(super) fn extract_matches_context(cond: &Cond) -> crate::exec::function::MatchesContext {
	let mut ctx = crate::exec::function::MatchesContext::new();
	collect_matches(&cond.0, &mut ctx);
	ctx
}

fn collect_matches(expr: &Expr, ctx: &mut crate::exec::function::MatchesContext) {
	match expr {
		Expr::Binary {
			left,
			op: BinaryOperator::Matches(matches_op),
			right,
		} => {
			if let Expr::Idiom(idiom) = left.as_ref() {
				let query = match right.as_ref() {
					Expr::Literal(Literal::String(s)) => s.clone(),
					_ => return,
				};
				let match_ref = matches_op.rf.unwrap_or(0);
				ctx.insert(
					match_ref,
					crate::exec::function::MatchInfo {
						idiom: idiom.clone(),
						query,
					},
				);
			}
		}
		Expr::Binary {
			left,
			right,
			..
		} => {
			collect_matches(left, ctx);
			collect_matches(right, ctx);
		}
		Expr::Prefix {
			expr: inner,
			..
		} => collect_matches(inner, ctx),
		_ => {}
	}
}

/// Try to extract the primary table name from the frozen context.
pub(super) fn extract_table_from_context(ctx: &crate::ctx::FrozenContext) -> crate::val::TableName {
	if let Some(mc) = ctx.get_matches_context()
		&& let Some(table) = mc.table()
	{
		return table.clone();
	}
	crate::val::TableName::from("unknown".to_string())
}

// ============================================================================
// VERSION Extraction
// ============================================================================

/// Extract version timestamp from VERSION clause expression.
pub(super) fn extract_version(version_expr: Expr) -> Result<Option<u64>, Error> {
	match version_expr {
		Expr::Literal(Literal::None) => Ok(None),
		Expr::Literal(Literal::Datetime(dt)) => {
			let stamp = dt.to_version_stamp().map_err(|e| Error::Query {
				message: format!("Invalid VERSION timestamp: {}", e),
			})?;
			Ok(Some(stamp))
		}
		_ => Err(Error::Query {
			message: "VERSION clause only supports literal datetime values in execution plans"
				.to_string(),
		}),
	}
}

// ============================================================================
// GROUP BY Validation
// ============================================================================

/// Check if fields contain `$this` or `$parent` parameters (invalid in GROUP BY).
pub(super) fn check_forbidden_group_by_params(fields: &Fields) -> Result<(), Error> {
	match fields {
		Fields::Value(selector) => check_expr_for_forbidden_params(&selector.expr),
		Fields::Select(field_list) => {
			for field in field_list {
				match field {
					Field::All => {}
					Field::Single(selector) => {
						check_expr_for_forbidden_params(&selector.expr)?;
					}
				}
			}
			Ok(())
		}
	}
}

fn check_expr_for_forbidden_params(expr: &Expr) -> Result<(), Error> {
	match expr {
		Expr::Param(param) => {
			let name = param.as_str();
			if name == "this" || name == "self" {
				return Err(Error::Query {
					message: "Found a `$this` parameter refering to the document of a group by select statement\nSelect statements with a group by currently have no defined document to refer to".to_string(),
				});
			}
			if name == "parent" {
				return Err(Error::Query {
					message: "Found a `$parent` parameter refering to the document of a GROUP select statement\nSelect statements with a GROUP BY or GROUP ALL currently have no defined document to refer to".to_string(),
				});
			}
			Ok(())
		}
		Expr::Binary {
			left,
			right,
			..
		} => {
			check_expr_for_forbidden_params(left)?;
			check_expr_for_forbidden_params(right)
		}
		Expr::Prefix {
			expr,
			..
		} => check_expr_for_forbidden_params(expr),
		Expr::Postfix {
			expr,
			..
		} => check_expr_for_forbidden_params(expr),
		Expr::FunctionCall(fc) => {
			for arg in &fc.arguments {
				check_expr_for_forbidden_params(arg)?;
			}
			Ok(())
		}
		Expr::Literal(Literal::Array(elements)) => {
			for elem in elements {
				check_expr_for_forbidden_params(elem)?;
			}
			Ok(())
		}
		Expr::Literal(Literal::Object(entries)) => {
			for entry in entries {
				check_expr_for_forbidden_params(&entry.value)?;
			}
			Ok(())
		}
		Expr::Select(select) => match &select.fields {
			Fields::Value(selector) => check_expr_for_forbidden_params(&selector.expr),
			Fields::Select(field_list) => {
				for field in field_list {
					if let Field::Single(selector) = field {
						check_expr_for_forbidden_params(&selector.expr)?;
					}
				}
				Ok(())
			}
		},
		Expr::Block(block) => {
			for stmt in &block.0 {
				check_expr_for_forbidden_params(stmt)?;
			}
			Ok(())
		}
		Expr::IfElse(ifelse) => {
			for (cond, body) in &ifelse.exprs {
				check_expr_for_forbidden_params(cond)?;
				check_expr_for_forbidden_params(body)?;
			}
			if let Some(close) = &ifelse.close {
				check_expr_for_forbidden_params(close)?;
			}
			Ok(())
		}
		Expr::Closure(closure) => check_expr_for_forbidden_params(&closure.body),
		Expr::Idiom(idiom) => {
			for part in &idiom.0 {
				match part {
					crate::expr::Part::Start(expr)
					| crate::expr::Part::Where(expr)
					| crate::expr::Part::Value(expr) => {
						check_expr_for_forbidden_params(expr)?;
					}
					crate::expr::Part::Method(_, args) => {
						for arg in args {
							check_expr_for_forbidden_params(arg)?;
						}
					}
					_ => {}
				}
			}
			Ok(())
		}
		Expr::Literal(_) | Expr::Constant(_) | Expr::Table(_) | Expr::Break | Expr::Continue => {
			Ok(())
		}
		_ => Ok(()),
	}
}

// ============================================================================
// LIMIT Helpers
// ============================================================================

/// Try to get the effective limit (start + limit) if both are literals.
pub(super) fn get_effective_limit_literal(
	start: &Option<crate::expr::start::Start>,
	limit: &Option<crate::expr::limit::Limit>,
) -> Option<usize> {
	let limit_val = limit.as_ref().and_then(|l| match &l.0 {
		Expr::Literal(Literal::Integer(n)) if *n >= 0 => Some(*n as usize),
		Expr::Literal(Literal::Float(n)) if *n >= 0.0 => Some(*n as usize),
		_ => None,
	})?;

	let start_val = start
		.as_ref()
		.map(|s| match &s.0 {
			Expr::Literal(Literal::Integer(n)) if *n >= 0 => Some(*n as usize),
			Expr::Literal(Literal::Float(n)) if *n >= 0.0 => Some(*n as usize),
			_ => None,
		})
		.unwrap_or(Some(0))?;

	Some(start_val + limit_val)
}

// ============================================================================
// Field Name Derivation
// ============================================================================

/// Derive a field name from an expression for projection output.
pub(super) fn derive_field_name(expr: &Expr) -> String {
	match expr {
		Expr::Idiom(idiom) => idiom_to_field_name(idiom),
		Expr::Param(param) => param.as_str().to_string(),
		Expr::FunctionCall(call) => {
			let idiom: crate::expr::idiom::Idiom = call.receiver.to_idiom();
			idiom_to_field_name(&idiom)
		}
		_ => {
			use surrealdb_types::ToSql;
			expr.to_sql()
		}
	}
}

/// Extract a field name from an idiom.
pub(super) fn idiom_to_field_name(idiom: &crate::expr::idiom::Idiom) -> String {
	use surrealdb_types::ToSql;

	use crate::expr::part::Part;

	for part in idiom.0.iter() {
		if let Part::Lookup(lookup) = part
			&& let Some(alias) = &lookup.alias
		{
			return idiom_to_field_name(alias);
		}
	}

	let simplified = idiom.simplify();

	if simplified.len() == 1
		&& let Some(Part::Field(name)) = simplified.first()
	{
		return name.clone();
	}
	simplified.to_sql()
}

/// Extract a field path from an idiom for nested output construction.
pub(super) fn idiom_to_field_path(idiom: &crate::expr::idiom::Idiom) -> FieldPath {
	use surrealdb_types::ToSql;

	use crate::expr::part::Part;

	for part in idiom.0.iter() {
		if let Part::Lookup(lookup) = part
			&& lookup.alias.is_some()
		{
			return FieldPath::field(idiom_to_field_name(idiom));
		}
	}

	let has_lookups = idiom.0.iter().any(|p| matches!(p, Part::Lookup(_)));

	if !has_lookups {
		let name = idiom_to_field_name(idiom);
		if name.contains('.') && !name.contains(['[', '(', ' ']) {
			return FieldPath(
				name.split('.').map(|s| FieldPathPart::Field(s.to_string())).collect(),
			);
		}
		return FieldPath::field(name);
	}

	let mut parts = Vec::new();
	for part in idiom.0.iter() {
		match part {
			Part::Lookup(lookup) => {
				let lookup_key = lookup.to_sql();
				parts.push(FieldPathPart::Lookup(lookup_key));
			}
			Part::Field(name) => {
				parts.push(FieldPathPart::Field(name.clone()));
			}
			_ => {}
		}
	}

	if parts.is_empty() {
		return FieldPath::field(idiom.to_sql());
	}

	FieldPath(parts)
}
