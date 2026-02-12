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
use super::schema::{gql_to_sql_kind, sql_value_to_gql_value};
use super::utils::execute_plan;
use crate::catalog::FunctionDefinition;
use crate::dbs::Session;
use crate::expr::{Expr, FunctionCall, Kind, LogicalPlan, TopLevelExpr};
use crate::gql::schema::kind_to_type;
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
	session: &Session,
	datastore: &Arc<Datastore>,
) -> Result<Object, GqlError> {
	for fnd in fns.iter() {
		let Some(kind) = &fnd.returns else {
			// Skip functions without a declared return type
			continue;
		};

		// Clone values that will be moved into the resolver closure
		let sess1 = session.clone();
		let kvs1 = datastore.clone();
		let fnd1 = fnd.clone();

		let mut field = Field::new(
			format!("fn_{}", fnd.name),
			kind_to_type(kind.clone(), types, false)?,
			move |ctx| {
				let sess1 = sess1.clone();
				let kvs1 = kvs1.clone();
				let fnd1 = fnd1.clone();
				FieldFuture::new(async move {
					let gql_args = ctx.args.as_index_map();
					let mut args = Vec::new();

					// Convert each GraphQL argument to its SurrealQL equivalent
					for (arg_name, arg_kind) in fnd1.args.iter() {
						if let Some(arg_val) = gql_args.get(arg_name.as_str()) {
							let arg_val = gql_to_sql_kind(arg_val, arg_kind.clone())?;
							args.push(arg_val.into_literal());
						} else {
							// Missing arguments default to None
							args.push(Value::None.into_literal());
						}
					}

					// Execute the function call via a LogicalPlan
					let func_call = Expr::FunctionCall(Box::new(FunctionCall {
						receiver: crate::expr::Function::Custom(fnd1.name.clone()),
						arguments: args,
					}));
					let plan = LogicalPlan {
						expressions: vec![TopLevelExpr::Expr(func_call)],
					};
					let res = execute_plan(&kvs1, &sess1, plan).await?;

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
						_ => Some(FieldValue::value(sql_value_to_gql_value(res)?)),
					};

					Ok(gql_res)
				})
			},
		);

		// Register each function argument as a GraphQL input value
		for (arg_name, arg_kind) in fnd.args.iter() {
			let arg_ty = kind_to_type(arg_kind.clone(), types, true)?;
			field = field.argument(InputValue::new(arg_name, arg_ty))
		}

		query = query.field(field);
	}

	Ok(query)
}
