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

pub async fn process_fns(
	fns: Arc<[FunctionDefinition]>,
	mut query: Object,
	types: &mut Vec<Type>,
	session: &Session,
	datastore: &Arc<Datastore>,
) -> Result<Object, GqlError> {
	for fnd in fns.iter() {
		let Some(kind) = &fnd.returns else {
			// TODO: handle case where there are no typed functions and give graceful error
			continue;
		};
		let sess1 = session.clone();
		let kvs1 = datastore.clone();
		let fnd1 = fnd.clone();
		let kind1 = kind.clone();
		let mut field = Field::new(
			format!("fn_{}", fnd.name),
			kind_to_type(kind.clone(), types)?,
			move |ctx| {
				let sess1 = sess1.clone();
				let kvs1 = kvs1.clone();
				let fnd1 = fnd1.clone();
				let kind1 = kind1.clone();
				FieldFuture::new(async move {
					let gql_args = ctx.args.as_index_map();
					let mut args = Vec::new();

					for (arg_name, arg_kind) in fnd1.args.iter() {
						if let Some(arg_val) = gql_args.get(arg_name.as_str()) {
							let arg_val = gql_to_sql_kind(arg_val, arg_kind.clone())?;
							args.push(arg_val.into_literal());
						} else {
							args.push(Value::None.into_literal());
						}
					}

					// Build function call as LogicalPlan
					let func_call = Expr::FunctionCall(Box::new(FunctionCall {
						receiver: crate::expr::Function::Custom(fnd1.name.clone()),
						arguments: args,
					}));

					let plan = LogicalPlan {
						expressions: vec![TopLevelExpr::Expr(func_call)],
					};

					let res = execute_plan(&kvs1, &sess1, plan).await?;

					let gql_res = match res {
						Value::RecordId(rid) => {
							let mut tmp =
								FieldValue::owned_any(rid.clone());
							match kind1 {
								Kind::Record(ts) if ts.len() != 1 => {
									tmp = tmp.with_type(rid.table.clone())
								}
								_ => {}
							}
							Some(tmp)
						}
						Value::None => None,
						_ => Some(FieldValue::value(sql_value_to_gql_value(res)?)),
					};

					Ok(gql_res)
				})
			},
		);

		for (arg_name, arg_kind) in fnd.args.iter() {
			let arg_ty = kind_to_type(arg_kind.clone(), types)?;
			field = field.argument(InputValue::new(arg_name, arg_ty))
		}

		query = query.field(field);
	}

	Ok(query)
}
