use std::sync::Arc;

use super::schema::{gql_to_sql_kind, sql_value_to_gql_value};
use super::utils::field_val_erase_owned;
use super::GqlError;
use crate::dbs::Session;
use crate::gql::schema::kind_to_type;
use crate::gql::utils::GQLTx;
use crate::kvs::Datastore;
use crate::sql::statements::DefineFunctionStatement;
use crate::sql::{Kind, Value as SqlValue};
use async_graphql::dynamic::FieldFuture;
use async_graphql::dynamic::InputValue;
use async_graphql::dynamic::Object;
use async_graphql::dynamic::Type;
use async_graphql::dynamic::{Field, FieldValue};

pub async fn process_fns(
	fns: Arc<[DefineFunctionStatement]>,
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
					let gtx = GQLTx::new(&kvs1, &sess1).await?;
					let gql_args = ctx.args.as_index_map();
					let mut args = Vec::new();

					for (arg_name, arg_kind) in fnd1.args {
						if let Some(arg_val) = gql_args.get(arg_name.as_str()) {
							let arg_val = gql_to_sql_kind(arg_val, arg_kind)?;
							args.push(arg_val);
						} else {
							args.push(SqlValue::None);
						}
					}

					let res = gtx.run_fn(&fnd1.name, args).await?;

					let gql_res = match res {
						SqlValue::Thing(rid) => {
							let mut tmp = field_val_erase_owned((gtx.clone(), rid.clone()));
							match kind1 {
								Kind::Record(ts) if ts.len() != 1 => {
									tmp = tmp.with_type(rid.tb.clone())
								}
								_ => {}
							}
							tmp
						}
						_ => FieldValue::value(sql_value_to_gql_value(res)?),
					};

					Ok(Some(gql_res))
				})
			},
		);

		let fnd2 = fnd.clone();
		for (arg_name, arg_kind) in fnd2.args {
			let arg_ty = kind_to_type(arg_kind.clone(), types)?;
			field = field.argument(InputValue::new(&arg_name.0, arg_ty))
		}

		query = query.field(field);
	}

	Ok(query)
}
