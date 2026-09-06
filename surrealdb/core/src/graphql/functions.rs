//! GraphQL function field generation.
//!
//! Exposes user-defined database functions (`DEFINE FUNCTION fn::name ...`) as
//! Query root fields.  Each function with a declared return type becomes a
//! field named `fn_<name>` on the Query type, with typed arguments and return
//! value.
//!
//! Functions without a return type annotation are skipped since GraphQL requires
//! a known output type for every field.

use std::sync::Arc;

use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, Object, Type};

use super::GraphqlError;
use super::schema::{graphql_to_sql_kind_with_scope, sql_value_to_graphql_value_with_kind};
use super::utils::execute_plan;
use crate::catalog::FunctionDefinition;
use crate::dbs::Session;
use crate::expr::{Expr, FunctionCall, Kind, LogicalPlan, TopLevelExpr};
use crate::graphql::schema::{TableTypeNames, kind_to_type_with_enum_prefix};
use crate::kvs::Datastore;
use crate::val::Value;

/// Process all exposed functions and add them as Query root fields.
///
/// For each function definition with a return type, creates a field
/// `fn_<name>` on the Query object with:
/// - Typed arguments matching the function's parameter list
/// - A return type derived from the function's `RETURNS` clause
/// - A resolver that converts GraphQL arguments to SurrealQL values, invokes the function via a
///   `LogicalPlan`, and converts the result back
pub async fn process_fns(
	fns: Arc<[FunctionDefinition]>,
	mut query: Object,
	types: &mut Vec<Type>,
	datastore: &Arc<Datastore>,
	table_types: &Arc<TableTypeNames>,
) -> Result<Object, GraphqlError> {
	for fnd in fns.iter() {
		let Some(kind) = &fnd.returns else {
			// Skip functions without a declared return type
			continue;
		};

		// SECURITY: do NOT close over the schema-generation-time session. The
		// schema is cached per (ns, db, graphql-config), so a session captured here
		// would be reused for every later caller's fn_* invocation — running
		// their queries under the first caller's auth. Read the per-request
		// session from the GraphQL context instead, matching the other
		// resolvers in this crate.
		let kvs1 = Arc::clone(datastore);
		let fnd1 = fnd.clone();
		let table_types1 = Arc::clone(table_types);
		// Decided by the declared return type, which is fixed for the life of
		// the schema — so it is resolved here rather than on every invocation.
		let tag_record_type = returns_abstract_record(fnd.returns.as_ref());

		// Honour an explicit `GRAPHQL <ident>` alias when valid; otherwise fall
		// back to the auto-derived `fn_<name>` form. See GitHub issue #4537.
		let field_name = match fnd.graphql_alias.as_deref() {
			Some(alias) if super::tables::is_valid_graphql_identifier_pub(alias) => {
				alias.to_string()
			}
			_ => format!("fn_{}", fnd.name),
		};
		let mut field = Field::new(
			field_name,
			kind_to_type_with_enum_prefix(
				kind.clone(),
				types,
				false,
				Some(&format!("fn_{}_return", fnd.name)),
				table_types,
			)?,
			move |ctx| {
				let kvs1 = Arc::clone(&kvs1);
				let fnd1 = fnd1.clone();
				let table_types1 = Arc::clone(&table_types1);
				FieldFuture::new(async move {
					let sess1 = ctx.data::<Arc<Session>>()?;
					let graphql_args = ctx.args.as_index_map();
					let mut args = Vec::new();

					// Convert each GraphQL argument to its SurrealQL equivalent
					for (arg_name, arg_kind) in fnd1.args.iter() {
						if let Some(arg_val) = graphql_args.get(arg_name.as_str()) {
							let scope = format!("fn_{}_{}", fnd1.name, arg_name);
							let arg_val = graphql_to_sql_kind_with_scope(
								arg_val,
								arg_kind.clone(),
								Some(&scope),
							)?;
							args.push(arg_val.into_literal());
						} else {
							// Missing arguments default to None
							args.push(Value::None.into_literal());
						}
					}

					// Execute the function call via a LogicalPlan
					let func_call = Expr::FunctionCall(Box::new(FunctionCall {
						receiver: crate::expr::Function::Custom(fnd1.name.to_string()),
						arguments: args,
					}));
					let plan = LogicalPlan {
						expressions: vec![TopLevelExpr::Expr(func_call)],
					};
					let res = execute_plan(&kvs1, sess1.as_ref(), plan).await?;

					// Convert the SurrealQL result to a GraphQL value
					let graphql_res = match res {
						Value::RecordId(rid) => {
							let field_val = FieldValue::owned_any(rid.clone());
							// The tag names the table's *generated Object type*,
							// which follows its `GRAPHQL_ALIAS` rather than its
							// SurrealQL name (#7453). The map is fixed for the life
							// of the schema, so it is captured rather than looked
							// up in the per-request context.
							let field_val = if tag_record_type {
								field_val.with_type(table_types1.get(rid.table.as_str()).to_owned())
							} else {
								field_val
							};
							Some(field_val)
						}
						Value::None => None,
						_ => Some(FieldValue::value(sql_value_to_graphql_value_with_kind(
							res,
							fnd1.returns.as_ref(),
							Some(&format!("fn_{}_return", fnd1.name)),
						)?)),
					};

					Ok(graphql_res)
				})
			},
		);

		// Attach a description that surfaces the SurrealQL `COMMENT` and any
		// `GRAPHQL_DEPRECATED "reason"` to schema consumers (async-graphql
		// 7.2.1 doesn't yet expose the `@deprecated` directive setter).
		if let Some(desc) = super::naming::description_with_deprecation(
			fnd.comment.as_deref(),
			fnd.graphql_deprecated.as_deref(),
		) {
			field = field.description(desc);
		}

		// Register each function argument as a GraphQL input value
		for (arg_name, arg_kind) in fnd.args.iter() {
			let arg_ty = kind_to_type_with_enum_prefix(
				arg_kind.clone(),
				types,
				true,
				Some(&format!("fn_{}_{}", fnd.name, arg_name)),
				table_types,
			)?;
			field = field.argument(InputValue::new(arg_name, arg_ty))
		}

		query = query.field(field);
	}

	Ok(query)
}

/// `true` when a function's record-valued return lands on an *abstract* GraphQL
/// type and therefore needs a `FieldValue::with_type()` tag to resolve.
///
/// [`kind_to_type`](super::schema::kind_to_type) maps a bare `record` to the
/// `record` interface and a multi-target `record<a | b>` to a generated union;
/// async-graphql cannot pick the concrete member of either without the tag. A
/// single-target `record<t>` already *is* that Object type, so tagging it is
/// unnecessary. `option<T>` is stored as `Either([None, T])` and is unwrapped
/// here exactly as `kind_to_type` unwraps it when building the type reference.
fn returns_abstract_record(kind: Option<&Kind>) -> bool {
	match kind {
		Some(Kind::Record(ts)) => ts.is_empty() || ts.len() > 1,
		Some(Kind::Either(ks)) => {
			let mut concrete = ks.iter().filter(|k| !matches!(k, Kind::None | Kind::Null));
			match (concrete.next(), concrete.next()) {
				(Some(only), None) => returns_abstract_record(Some(only)),
				_ => false,
			}
		}
		_ => false,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn record(tables: &[&str]) -> Kind {
		Kind::Record(tables.iter().map(|t| (*t).into()).collect())
	}

	#[test]
	fn abstract_record_returns_need_a_concrete_type_tag() {
		// `record` is the interface, `record<a | b>` a union: neither resolves
		// without `.with_type()`.
		assert!(returns_abstract_record(Some(&record(&[]))));
		assert!(returns_abstract_record(Some(&record(&["gadget", "widget"]))));
		// `option<record>` strips to the same interface reference.
		assert!(returns_abstract_record(Some(&Kind::Either(vec![Kind::None, record(&[])]))));
	}

	#[test]
	fn concrete_and_non_record_returns_do_not() {
		// A single-target record already resolves to that Object type.
		assert!(!returns_abstract_record(Some(&record(&["gadget"]))));
		assert!(!returns_abstract_record(Some(&Kind::Either(vec![
			Kind::None,
			record(&["gadget"])
		]))));
		assert!(!returns_abstract_record(Some(&Kind::String)));
		assert!(!returns_abstract_record(None));
		// A genuine multi-kind Either is its own union of those types; the
		// record member is not what the field resolves to.
		assert!(!returns_abstract_record(Some(&Kind::Either(vec![Kind::String, record(&[])]))));
	}
}
