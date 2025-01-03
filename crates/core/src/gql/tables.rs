use std::fmt::Display;
use std::sync::Arc;

use crate::dbs::Session;
use crate::gql::ext::TryAsExt;
use crate::gql::schema::{kind_to_type, unwrap_type};
use crate::kvs::{Datastore, Transaction};
use crate::sql::order::{OrderList, Ordering};
use crate::sql::statements::{DefineFieldStatement, DefineTableStatement, SelectStatement};
use crate::sql::{self, Table};
use crate::sql::{Cond, Fields};
use crate::sql::{Expression, Value as SqlValue};
use crate::sql::{Idiom, Kind};
use crate::sql::{Statement, Thing};
use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::FieldFuture;
use async_graphql::dynamic::InputValue;
use async_graphql::dynamic::TypeRef;
use async_graphql::dynamic::{Enum, FieldValue, Type};
use async_graphql::dynamic::{Field, ResolverContext};
use async_graphql::dynamic::{InputObject, Object};
use async_graphql::Name;
use async_graphql::Value as GqlValue;

use super::error::{resolver_error, GqlError};
use super::ext::IntoExt;
use super::schema::{gql_to_sql_kind, sql_value_to_gql_value};
use crate::gql::error::internal_error;
use crate::gql::utils::{field_val_erase_owned, ErasedRecord, GQLTx, GqlValueUtils};

macro_rules! order {
	(asc, $field:expr) => {{
		let mut tmp = sql::Order::default();
		tmp.value = $field.into();
		tmp.direction = true;
		tmp
	}};
	(desc, $field:expr) => {{
		let mut tmp = sql::Order::default();
		tmp.value = $field.into();
		tmp
	}};
}

macro_rules! limit_input {
	() => {
		InputValue::new("limit", TypeRef::named(TypeRef::INT))
	};
}

macro_rules! start_input {
	() => {
		InputValue::new("start", TypeRef::named(TypeRef::INT))
	};
}

macro_rules! id_input {
	() => {
		InputValue::new("id", TypeRef::named_nn(TypeRef::ID))
	};
}

fn filter_name_from_table(tb_name: impl Display) -> String {
	format!("_filter_{tb_name}")
}

#[allow(clippy::too_many_arguments)]
pub async fn process_tbs(
	tbs: Arc<[DefineTableStatement]>,
	mut query: Object,
	types: &mut Vec<Type>,
	tx: &Transaction,
	ns: &str,
	db: &str,
	session: &Session,
	datastore: &Arc<Datastore>,
) -> Result<Object, GqlError> {
	for tb in tbs.iter() {
		trace!("Adding table: {}", tb.name);
		let tb_name = tb.name.to_string();
		let first_tb_name = tb_name.clone();
		let second_tb_name = tb_name.clone();

		let table_orderable_name = format!("_orderable_{tb_name}");
		let mut table_orderable = Enum::new(&table_orderable_name).item("id");
		table_orderable = table_orderable.description(format!(
			"Generated from `{}` the fields which a query can be ordered by",
			tb.name
		));
		let table_order_name = format!("_order_{tb_name}");
		let table_order = InputObject::new(&table_order_name)
			.description(format!(
				"Generated from `{}` an object representing a query ordering",
				tb.name
			))
			.field(InputValue::new("asc", TypeRef::named(&table_orderable_name)))
			.field(InputValue::new("desc", TypeRef::named(&table_orderable_name)))
			.field(InputValue::new("then", TypeRef::named(&table_order_name)));

		let table_filter_name = filter_name_from_table(tb_name);
		let mut table_filter = InputObject::new(&table_filter_name);
		table_filter = table_filter
			.field(InputValue::new("id", TypeRef::named("_filter_id")))
			.field(InputValue::new("and", TypeRef::named_nn_list(&table_filter_name)))
			.field(InputValue::new("or", TypeRef::named_nn_list(&table_filter_name)))
			.field(InputValue::new("not", TypeRef::named(&table_filter_name)));
		types.push(Type::InputObject(filter_id()));

		let sess1 = session.to_owned();
		let fds = tx.all_tb_fields(ns, db, &tb.name.0, None).await?;
		let fds1 = fds.clone();
		let kvs1 = datastore.clone();

		query = query.field(
        Field::new(
            tb.name.to_string(),
            TypeRef::named_nn_list_nn(tb.name.to_string()),
            move |ctx| {
                let tb_name = first_tb_name.clone();
                let sess1 = sess1.clone();
                let fds1 = fds1.clone();
                let kvs1 = kvs1.clone();
                FieldFuture::new(async move {
                    let gtx = GQLTx::new(&kvs1, &sess1).await?;

                    let args = ctx.args.as_index_map();
                    trace!("received request with args: {args:?}");

                    let start = args.get("start").and_then(|v| v.as_i64()).map(|s| s.intox());

                    let limit = args.get("limit").and_then(|v| v.as_i64()).map(|l| l.intox());

                    let order = args.get("order");

                    let filter = args.get("filter");

					let orders = match order {
						Some(GqlValue::Object(o)) => {
							let mut orders = vec![];
							let mut current = o;
							loop {
								let asc = current.get("asc");
								let desc = current.get("desc");
								match (asc, desc) {
									(Some(_), Some(_)) => {
										return Err("Found both ASC and DESC in order".into());
									}
									(Some(GqlValue::Enum(a)), None) => {
										orders.push(order!(asc, a.as_str()))
									}
									(None, Some(GqlValue::Enum(d))) => {
										orders.push(order!(desc, d.as_str()))
									}
									(_, _) => {
										break;
									}
								}
								if let Some(GqlValue::Object(next)) = current.get("then") {
									current = next;
								} else {
									break;
								}
							}
							Some(orders)
						}
						_ => None,
					};

                    trace!("parsed orders: {orders:?}");

                    let cond = match filter {
                        Some(f) => {
                            let o = match f {
                                GqlValue::Object(o) => o,
                                f => {
                                    error!("Found filter {f}, which should be object and should have been rejected by async graphql.");
                                    return Err("Value in cond doesn't fit schema".into());
                                }
                            };

                            let cond = cond_from_filter(o, &fds1)?;

                            Some(cond)
                        }
                        None => None,
                    };

                    trace!("parsed filter: {cond:?}");

                    // SELECT VALUE id FROM ...
                    let ast = Statement::Select({
                        SelectStatement {
                            what: vec![SqlValue::Table(tb_name.intox())].into(),
                            expr: Fields(
                                vec![sql::Field::Single {
                                    expr: SqlValue::Idiom(Idiom::from("id")),
                                    alias: None,
                                }],
                                // this means the `value` keyword
                                true,
                            ),
                            order: orders.map(|x| Ordering::Order(OrderList(x))),
                            cond,
                            limit,
                            start,
                            ..Default::default()
                        }
                    });

                    trace!("generated query ast: {ast:?}");

                    let res = gtx.process_stmt(ast).await?;

                    let res_vec =
                        match res {
                            SqlValue::Array(a) => a,
                            v => {
                                error!("Found top level value, in result which should be array: {v:?}");
                                return Err("Internal Error".into());
                            }
                        };

                    let out: Result<Vec<FieldValue>, SqlValue> = res_vec
                        .0
                        .into_iter()
                        .map(|v| {
                            v.try_as_thing().map(|t| {
                                let erased: ErasedRecord = (gtx.clone(), t);
                                field_val_erase_owned(erased)
                            })
                        })
                        .collect();

                    match out {
                        Ok(l) => Ok(Some(FieldValue::list(l))),
                        Err(v) => {
                            Err(internal_error(format!("expected thing, found: {v:?}")).into())
                        }
                    }
                })
            },
        )
        .description(if let Some(ref c) = &tb.comment { format!("{c}") } else { format!("Generated from table `{}`\nallows querying a table with filters", tb.name) })
        .argument(limit_input!())
        .argument(start_input!())
        .argument(InputValue::new("order", TypeRef::named(&table_order_name)))
        .argument(InputValue::new("filter", TypeRef::named(&table_filter_name))),
    );

		let sess2 = session.to_owned();
		let kvs2 = datastore.to_owned();
		query =
			query.field(
				Field::new(
					format!("_get_{}", tb.name),
					TypeRef::named(tb.name.to_string()),
					move |ctx| {
						let tb_name = second_tb_name.clone();
						let kvs2 = kvs2.clone();
						FieldFuture::new({
							let sess2 = sess2.clone();
							async move {
								let gtx = GQLTx::new(&kvs2, &sess2).await?;

								let args = ctx.args.as_index_map();
								let id = match args.get("id").and_then(GqlValueUtils::as_string) {
									Some(i) => i,
									None => {
										return Err(internal_error(
											"Schema validation failed: No id found in _get_",
										)
										.into());
									}
								};
								let thing = match id.clone().try_into() {
									Ok(t) => t,
									Err(_) => Thing::from((tb_name, id)),
								};

								match gtx.get_record_field(thing, "id").await? {
									SqlValue::Thing(t) => {
										let erased: ErasedRecord = (gtx, t);
										Ok(Some(field_val_erase_owned(erased)))
									}
									_ => Ok(None),
								}
							}
						})
					},
				)
				.description(if let Some(ref c) = &tb.comment {
					format!("{c}")
				} else {
					format!("Generated from table `{}`\nallows querying a single record in a table by ID", tb.name)
				})
				.argument(id_input!()),
			);

		let mut table_ty_obj = Object::new(tb.name.to_string())
			.field(Field::new(
				"id",
				TypeRef::named_nn(TypeRef::ID),
				make_table_field_resolver(
					"id",
					Some(Kind::Record(vec![Table::from(tb.name.to_string())])),
				),
			))
			.implement("record");

		for fd in fds.iter() {
			let Some(ref kind) = fd.kind else {
				continue;
			};
			let fd_name = Name::new(fd.name.to_string());
			let fd_type = kind_to_type(kind.clone(), types)?;
			table_orderable = table_orderable.item(fd_name.to_string());
			let type_filter_name = format!("_filter_{}", unwrap_type(fd_type.clone()));

			let type_filter =
				Type::InputObject(filter_from_type(kind.clone(), type_filter_name.clone(), types)?);
			trace!("\n{type_filter:?}\n");
			types.push(type_filter);

			table_filter = table_filter
				.field(InputValue::new(fd.name.to_string(), TypeRef::named(type_filter_name)));

			table_ty_obj = table_ty_obj
				.field(Field::new(
					fd.name.to_string(),
					fd_type,
					make_table_field_resolver(fd_name.as_str(), fd.kind.clone()),
				))
				.description(if let Some(ref c) = fd.comment {
					format!("{c}")
				} else {
					"".to_string()
				});
		}

		types.push(Type::Object(table_ty_obj));
		types.push(table_order.into());
		types.push(Type::Enum(table_orderable));
		types.push(Type::InputObject(table_filter));
	}

	let sess3 = session.to_owned();
	let kvs3 = datastore.to_owned();
	query = query.field(
		Field::new("_get", TypeRef::named("record"), move |ctx| {
			FieldFuture::new({
				let sess3 = sess3.clone();
				let kvs3 = kvs3.clone();
				async move {
					let gtx = GQLTx::new(&kvs3, &sess3).await?;

					let args = ctx.args.as_index_map();
					let id = match args.get("id").and_then(GqlValueUtils::as_string) {
						Some(i) => i,
						None => {
							return Err(internal_error(
								"Schema validation failed: No id found in _get",
							)
							.into());
						}
					};

					let thing: Thing = match id.clone().try_into() {
						Ok(t) => t,
						Err(_) => return Err(resolver_error(format!("invalid id: {id}")).into()),
					};

					match gtx.get_record_field(thing, "id").await? {
						SqlValue::Thing(t) => {
							let ty = t.tb.to_string();
							let out = field_val_erase_owned((gtx, t)).with_type(ty);
							Ok(Some(out))
						}
						_ => Ok(None),
					}
				}
			})
		})
		.description("Allows fetching arbitrary records".to_string())
		.argument(id_input!()),
	);

	Ok(query)
}

fn make_table_field_resolver(
	fd_name: impl Into<String>,
	kind: Option<Kind>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	let fd_name = fd_name.into();
	move |ctx: ResolverContext| {
		let fd_name = fd_name.clone();
		let field_kind = kind.clone();
		FieldFuture::new({
			async move {
				let (ref gtx, ref rid) = ctx
					.parent_value
					.downcast_ref::<ErasedRecord>()
					.ok_or_else(|| internal_error("failed to downcast"))?;

				let val = gtx.get_record_field(rid.clone(), fd_name.as_str()).await?;

				let out = match val {
					SqlValue::Thing(rid) if fd_name != "id" => {
						let mut tmp = field_val_erase_owned((gtx.clone(), rid.clone()));
						match field_kind {
							Some(Kind::Record(ts)) if ts.len() != 1 => {
								tmp = tmp.with_type(rid.tb.clone())
							}
							_ => {}
						}
						Ok(Some(tmp))
					}
					SqlValue::None | SqlValue::Null => Ok(None),
					v => {
						match field_kind {
							Some(Kind::Either(ks)) if ks.len() != 1 => {}
							_ => {}
						}
						let out = sql_value_to_gql_value(v.to_owned())
							.map_err(|_| "SQL to GQL translation failed")?;
						Ok(Some(FieldValue::value(out)))
					}
				};
				out
			}
		})
	}
}

macro_rules! filter_impl {
	($filter:ident, $ty:ident, $name:expr) => {
		$filter = $filter.field(InputValue::new($name, $ty.clone()));
	};
}

fn filter_id() -> InputObject {
	let mut filter = InputObject::new("_filter_id");
	let ty = TypeRef::named(TypeRef::ID);
	filter_impl!(filter, ty, "eq");
	filter_impl!(filter, ty, "ne");
	filter
}
fn filter_from_type(
	kind: Kind,
	filter_name: String,
	types: &mut Vec<Type>,
) -> Result<InputObject, GqlError> {
	let ty = match &kind {
		Kind::Record(ts) => match ts.len() {
			1 => TypeRef::named(filter_name_from_table(
				ts.first().expect("ts should have exactly one element").as_str(),
			)),
			_ => TypeRef::named(TypeRef::ID),
		},
		k => unwrap_type(kind_to_type(k.clone(), types)?),
	};

	let mut filter = InputObject::new(filter_name);
	filter_impl!(filter, ty, "eq");
	filter_impl!(filter, ty, "ne");

	match kind {
		Kind::Any => {}
		Kind::Null => {}
		Kind::Bool => {}
		Kind::Bytes => {}
		Kind::Datetime => {}
		Kind::Decimal => {}
		Kind::Duration => {}
		Kind::Float => {}
		Kind::Int => {}
		Kind::Number => {}
		Kind::Object => {}
		Kind::Point => {}
		Kind::String => {}
		Kind::Uuid => {}
		Kind::Record(_) => {}
		Kind::Geometry(_) => {}
		Kind::Option(_) => {}
		Kind::Either(_) => {}
		Kind::Set(_, _) => {}
		Kind::Array(_, _) => {}
		Kind::Function(_, _) => {}
		Kind::Range => {}
		Kind::Literal(_) => {}
	};
	Ok(filter)
}

fn cond_from_filter(
	filter: &IndexMap<Name, GqlValue>,
	fds: &[DefineFieldStatement],
) -> Result<Cond, GqlError> {
	val_from_filter(filter, fds).map(IntoExt::intox)
}

fn val_from_filter(
	filter: &IndexMap<Name, GqlValue>,
	fds: &[DefineFieldStatement],
) -> Result<SqlValue, GqlError> {
	if filter.len() != 1 {
		return Err(resolver_error("Table Filter must have one item"));
	}

	let (k, v) = filter.iter().next().unwrap();

	let cond = match k.as_str().to_lowercase().as_str() {
		"or" => aggregate(v, AggregateOp::Or, fds),
		"and" => aggregate(v, AggregateOp::And, fds),
		"not" => negate(v, fds),
		_ => binop(k.as_str(), v, fds),
	};

	cond
}

fn parse_op(name: impl AsRef<str>) -> Result<sql::Operator, GqlError> {
	match name.as_ref() {
		"eq" => Ok(sql::Operator::Equal),
		"ne" => Ok(sql::Operator::NotEqual),
		op => Err(resolver_error(format!("Unsupported op: {op}"))),
	}
}

fn negate(filter: &GqlValue, fds: &[DefineFieldStatement]) -> Result<SqlValue, GqlError> {
	let obj = filter.as_object().ok_or(resolver_error("Value of NOT must be object"))?;
	let inner_cond = val_from_filter(obj, fds)?;

	Ok(Expression::Unary {
		o: sql::Operator::Not,
		v: inner_cond,
	}
	.into())
}

enum AggregateOp {
	And,
	Or,
}

fn aggregate(
	filter: &GqlValue,
	op: AggregateOp,
	fds: &[DefineFieldStatement],
) -> Result<SqlValue, GqlError> {
	let op_str = match op {
		AggregateOp::And => "AND",
		AggregateOp::Or => "OR",
	};
	let op = match op {
		AggregateOp::And => sql::Operator::And,
		AggregateOp::Or => sql::Operator::Or,
	};
	let list =
		filter.as_list().ok_or(resolver_error(format!("Value of {op_str} should be a list")))?;
	let filter_arr = list
		.iter()
		.map(|v| v.as_object().map(|o| val_from_filter(o, fds)))
		.collect::<Option<Result<Vec<SqlValue>, GqlError>>>()
		.ok_or(resolver_error(format!("List of {op_str} should contain objects")))??;

	let mut iter = filter_arr.into_iter();

	let mut cond = iter
		.next()
		.ok_or(resolver_error(format!("List of {op_str} should contain at least one object")))?;

	for clause in iter {
		cond = Expression::Binary {
			l: clause,
			o: op.clone(),
			r: cond,
		}
		.into();
	}

	Ok(cond)
}

fn binop(
	field_name: &str,
	val: &GqlValue,
	fds: &[DefineFieldStatement],
) -> Result<SqlValue, GqlError> {
	let obj = val.as_object().ok_or(resolver_error("Field filter should be object"))?;

	let Some(fd) = fds.iter().find(|fd| fd.name.to_string() == field_name) else {
		return Err(resolver_error(format!("Field `{field_name}` not found")));
	};

	if obj.len() != 1 {
		return Err(resolver_error("Field Filter must have one item"));
	}

	let lhs = sql::Value::Idiom(field_name.intox());

	let (k, v) = obj.iter().next().unwrap();
	let op = parse_op(k)?;

	let rhs = gql_to_sql_kind(v, fd.kind.clone().unwrap_or_default())?;

	let expr = sql::Expression::Binary {
		l: lhs,
		o: op,
		r: rhs,
	};

	Ok(expr.into())
}
