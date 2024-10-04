use std::fmt::Display;
use std::sync::Arc;

use crate::dbs::Session;
use crate::gql::schema::{kind_to_type, unwrap_type};
use crate::kvs::{Datastore, Transaction};
use crate::sql::statements::{DefineFieldStatement, DefineFunctionStatement, SelectStatement};
use crate::sql::{self, Table};
use crate::sql::{Cond, Fields};
use crate::sql::{Expression, Function};
use crate::sql::{Idiom, Kind};
use crate::sql::{Statement, Thing};
use async_graphql::dynamic::Field;
use async_graphql::dynamic::FieldFuture;
use async_graphql::dynamic::InputValue;
use async_graphql::dynamic::TypeRef;
use async_graphql::dynamic::{Enum, FieldValue, ResolverContext, Type};
use async_graphql::dynamic::{InputObject, Object};
use async_graphql::indexmap::IndexMap;
use async_graphql::Name;
use async_graphql::Value as GqlValue;

use super::error::resolver_error;
use super::ext::IntoExt;
use super::schema::{gql_to_sql_kind, sql_value_to_gql_value};
use super::GqlError;
use crate::gql::error::internal_error;
use crate::gql::ext::TryAsExt;
use crate::gql::utils::{GQLTx, GqlValueUtils};
use crate::sql::Value as SqlValue;

use crate::sql::statements::DefineTableStatement;

pub async fn process_fns(
	fns: Arc<[DefineFunctionStatement]>,
	mut query: Object,
	types: &mut Vec<Type>,
	session: &Session,
	datastore: &Arc<Datastore>,
) -> Result<Object, GqlError> {
	for fnd in fns.iter() {
		let Some(kind) = &fnd.returns else {
			continue;
		};
		let sess1 = session.clone();
		let kvs1 = datastore.clone();
		let fnd1 = fnd.clone();
		let mut field = Field::new(
			format!("fn_{}", fnd.name),
			kind_to_type(kind.clone(), types)?,
			move |ctx| {
				let sess1 = sess1.clone();
				let kvs1 = kvs1.clone();
				let fnd1 = fnd1.clone();
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

					let gql_res = sql_value_to_gql_value(res)?;

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
