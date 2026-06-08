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

use super::GqlError;
use super::schema::{gql_to_sql_kind_with_scope, sql_value_to_gql_value_with_kind};
use super::utils::execute_plan;
use crate::catalog::FunctionDefinition;
use crate::dbs::Session;
use crate::expr::{Expr, FunctionCall, Kind, LogicalPlan, TopLevelExpr};
use crate::gql::schema::kind_to_type_with_enum_prefix;
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
) -> Result<Object, GqlError> {
	for fnd in fns.iter() {
		let Some(kind) = &fnd.returns else {
			// Skip functions without a declared return type
			continue;
		};

		// SECURITY: do NOT close over the schema-generation-time session. The
		// schema is cached per (ns, db, gql-config), so a session captured here
		// would be reused for every later caller's fn_* invocation — running
		// their queries under the first caller's auth. Read the per-request
		// session from the GraphQL context instead, matching the other
		// resolvers in this crate.
		let kvs1 = Arc::clone(datastore);
		let fnd1 = fnd.clone();

		// Honour an explicit `GRAPHQL <ident>` alias when valid; otherwise fall
		// back to the auto-derived `fn_<name>` form. See GitHub issue #4537.
		let field_name = match fnd.graphql_alias.as_deref() {
			Some(alias) if super::tables::is_valid_gql_identifier_pub(alias) => alias.to_string(),
			_ => format!("fn_{}", fnd.name),
		};
		let mut field = Field::new(
			field_name,
			kind_to_type_with_enum_prefix(
				kind.clone(),
				types,
				false,
				Some(&format!("fn_{}_return", fnd.name)),
			)?,
			move |ctx| {
				let kvs1 = Arc::clone(&kvs1);
				let fnd1 = fnd1.clone();
				FieldFuture::new(async move {
					let sess1 = ctx.data::<Arc<Session>>()?;
					let gql_args = ctx.args.as_index_map();
					let mut args = Vec::new();

					// Convert each GraphQL argument to its SurrealQL equivalent
					for (arg_name, arg_kind) in fnd1.args.iter() {
						if let Some(arg_val) = gql_args.get(arg_name.as_str()) {
							let scope = format!("fn_{}_{}", fnd1.name, arg_name);
							let arg_val = gql_to_sql_kind_with_scope(
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
					let gql_res = match res {
						Value::RecordId(rid) => {
							let field_val = FieldValue::owned_any(rid.clone());
							// Untyped record returns need `.with_type()` for
							// interface resolution; typed `record<T>` do not.
							let field_val = match &fnd1.returns {
								Some(Kind::Record(ts)) if ts.is_empty() => {
									field_val.with_type(rid.table)
								}
								_ => field_val,
							};
							Some(field_val)
						}
						Value::None => None,
						_ => Some(FieldValue::value(sql_value_to_gql_value_with_kind(
							res,
							fnd1.returns.as_ref(),
							Some(&format!("fn_{}_return", fnd1.name)),
						)?)),
					};

					Ok(gql_res)
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
			)?;
			field = field.argument(InputValue::new(arg_name, arg_ty))
		}

		query = query.field(field);
	}

	Ok(query)
}
