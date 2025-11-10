use std::fmt::Display;
use std::sync::Arc;

use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{
	Enum, Field, FieldFuture, FieldValue, InputObject, InputValue, Object, ResolverContext, Type,
	TypeRef,
};
use async_graphql::{Name, Value as GqlValue};

use super::error::{GqlError, resolver_error};
use super::schema::{gql_to_sql_kind, sql_value_to_gql_value};
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, FieldDefinition, NamespaceId, TableDefinition};
use crate::dbs::Session;
use crate::expr::order::{OrderDirection, OrderList, Ordering};
use crate::expr::statements::SelectStatement;
use crate::expr::{
	self, BinaryOperator, Cond, Expr, Fields, Idiom, Kind, Limit, Literal, LogicalPlan, Start,
	TopLevelExpr,
};
use crate::gql::error::internal_error;
use crate::gql::schema::{kind_to_type, unwrap_type};
use crate::gql::utils::{GqlValueUtils, execute_plan};
use crate::kvs::{Datastore, Transaction};
use crate::val::{RecordId, Value};

fn order_asc(field_name: String) -> expr::Order {
	expr::Order {
		value: Idiom::field(field_name),
		direction: OrderDirection::Ascending,
		..Default::default()
	}
}

fn order_desc(field_name: String) -> expr::Order {
	expr::Order {
		value: Idiom::field(field_name),
		direction: OrderDirection::Descending,
		..Default::default()
	}
}

fn filter_name_from_table(tb_name: impl Display) -> String {
	format!("_filter_{tb_name}")
}

#[expect(clippy::too_many_arguments)]
pub async fn process_tbs(
	tbs: Arc<[TableDefinition]>,
	mut query: Object,
	types: &mut Vec<Type>,
	tx: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	_session: &Session,
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

		let fds = tx.all_tb_fields(ns, db, &tb.name, None).await?;
		let fds1 = fds.clone();
		let kvs1 = datastore.clone();

		query = query.field(
		Field::new(
			tb.name.to_string(),
			TypeRef::named_nn_list_nn(tb.name.to_string()),
			move |ctx| {
				let tb_name = first_tb_name.clone();
				let fds1 = fds1.clone();
				let kvs1 = kvs1.clone();
				FieldFuture::new(async move {
					// Get session from GraphQL context (has proper user permissions)
					let sess1 = ctx.data::<Arc<Session>>()?;
						let args = ctx.args.as_index_map();
						trace!("received request with args: {args:?}");

						let start = args.get("start").and_then(|v| v.as_i64()).map(|s| Start(Expr::Literal(Literal::Integer(s))));
						let limit = args.get("limit").and_then(|v| v.as_i64()).map(|l| Limit(Expr::Literal(Literal::Integer(l))));
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
											orders.push(order_asc(a.as_str().to_string()))
										}
										(None, Some(GqlValue::Enum(d))) => {
											orders.push(order_desc(d.as_str().to_string()))
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

					// SELECT * FROM ...
					// Note: We select * (not just id) so that permissions are properly checked
					let expr = expr::Expr::Select(
						Box::new(SelectStatement {
							what: vec![Expr::Table(tb_name)],
							expr: Fields::all(),
							order: orders.map(|x| Ordering::Order(OrderList(x))),
							cond,
							limit,
							start,
							..Default::default()
						})
					);

						// Convert to LogicalPlan and execute
						let plan = LogicalPlan {
							expressions: vec![TopLevelExpr::Expr(expr)],
						};

						tracing::warn!("generated logical plan: {plan:?}");

						let res = execute_plan(&kvs1, sess1, plan).await?;

					tracing::warn!("result: {res:?}");

					let res_vec =
						match res {
							Value::Array(a) => a,
							v => {
								error!("Found top level value, in result which should be array: {v:?}");
								return Err("Internal Error".into());
							}
						};

					let out: Result<Vec<FieldValue>, Value> = res_vec
						.0
						.into_iter()
						.map(|v| {
							match v {
								Value::Object(obj) => {
									// Extract the 'id' field which should be a RecordId
									match obj.get("id") {
										Some(Value::RecordId(rid)) => {
											Ok(FieldValue::owned_any(rid.clone()))
										}
										_ => {
											error!("Object missing 'id' field or id is not a RecordId: {obj:?}");
											Err("Internal Error".into())
										}
									}
								}
								_ => {
									error!("Found top level value, in result which should be object: {v:?}");
									Err("Internal Error".into())
								}
							}
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
        .description(if let Some(c) = &tb.comment { c.to_string() } else { format!("Generated from table `{}`\nallows querying a table with filters", tb.name) })
        .argument(InputValue::new("limit", TypeRef::named(TypeRef::INT)))
        .argument(InputValue::new("start", TypeRef::named(TypeRef::INT)))
        .argument(InputValue::new("order", TypeRef::named(&table_order_name)))
        .argument(InputValue::new("filter", TypeRef::named(&table_filter_name))));

		let kvs2 = datastore.to_owned();
		query = query.field(
			Field::new(
				format!("_get_{}", tb.name),
				TypeRef::named(tb.name.to_string()),
				move |ctx| {
					let tb_name = second_tb_name.clone();
					let kvs2 = kvs2.clone();
					FieldFuture::new({
						async move {
							// Get session from GraphQL context (has proper user permissions)
							let sess2 = ctx.data::<Arc<Session>>()?;
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

							// TODO: STU: Parse record id.
							let record_id = RecordId::new(tb_name, id);

							// Build SELECT VALUE id FROM ONLY <record_id>
							let select_stmt = SelectStatement {
								what: vec![Value::RecordId(record_id.clone()).into_literal()],
								expr: Fields::Value(Box::new(expr::Field::Single {
									expr: expr::Expr::Idiom(Idiom::field("id".to_string())),
									alias: None,
								})),
								only: true,
								..Default::default()
							};

							let plan = LogicalPlan {
								expressions: vec![TopLevelExpr::Expr(Expr::Select(Box::new(
									select_stmt,
								)))],
							};

							let res = execute_plan(&kvs2, sess2, plan).await?;

							match res {
								Value::RecordId(t) => {
									// let erased: ErasedRecord = (kvs2.clone(), sess2.clone(), t);
									Ok(Some(FieldValue::owned_any(t)))
								}
								_ => Ok(None),
							}
						}
					})
				},
			)
			.description(if let Some(c) = &tb.comment {
				c.to_string()
			} else {
				format!(
					"Generated from table `{}`\nallows querying a single record in a table by ID",
					tb.name
				)
			})
			.argument(InputValue::new("id", TypeRef::named_nn(TypeRef::ID))),
		);

		let mut table_ty_obj = Object::new(tb.name.to_string())
			.field(Field::new(
				"id",
				TypeRef::named_nn(TypeRef::ID),
				make_table_field_resolver("id", Some(Kind::Record(vec![tb.name.to_string()]))),
			))
			.implement("record");

		for fd in fds.iter() {
			let Some(ref kind) = fd.field_kind else {
				continue;
			};
			if fd.name.is_id() {
				// We have already defined "id"
				// so we don't take any new definition for it.
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
					make_table_field_resolver(fd_name.as_str(), fd.field_kind.clone()),
				))
				.description(if let Some(ref c) = fd.comment {
					c.to_string()
				} else {
					"".to_string()
				});
		}

		types.push(Type::Object(table_ty_obj));
		types.push(table_order.into());
		types.push(Type::Enum(table_orderable));
		types.push(Type::InputObject(table_filter));
	}

	let kvs3 = datastore.to_owned();
	query = query.field(
		Field::new("_get", TypeRef::named("record"), move |ctx| {
			FieldFuture::new({
				let kvs3 = kvs3.clone();
				async move {
					// Get session from GraphQL context (has proper user permissions)
					let sess3 = ctx.data::<Arc<Session>>()?;
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

					let record_id = RecordId::new("TODO: STU".to_string(), id); // TODO: STU: Parse record id.

					// Build SELECT VALUE id FROM ONLY <record_id>
					let select_stmt = SelectStatement {
						what: vec![Value::RecordId(record_id.clone()).into_literal()],
						expr: Fields::Value(Box::new(expr::Field::Single {
							expr: expr::Expr::Idiom(Idiom::field("id".to_string())),
							alias: None,
						})),
						only: true,
						..Default::default()
					};

					let plan = LogicalPlan {
						expressions: vec![TopLevelExpr::Expr(Expr::Select(Box::new(select_stmt)))],
					};

					let res = execute_plan(&kvs3, sess3, plan).await?;

					match res {
						Value::RecordId(t) => {
							// Generic _get returns interface type "record", needs .with_type()
							Ok(Some(
								FieldValue::owned_any(t.clone()).with_type(t.table.to_string()),
							))
						}
						_ => Ok(None),
					}
				}
			})
		})
		.description("Allows fetching arbitrary records".to_string())
		.argument(InputValue::new("id", TypeRef::named_nn(TypeRef::ID))),
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
				let ds = ctx.data::<Arc<Datastore>>()?;
				let sess = ctx.data::<Arc<Session>>()?;
				let rid = ctx.parent_value.try_downcast_ref::<RecordId>()?;

				// Build SELECT VALUE <field> FROM ONLY <record_id>
				let select_stmt = SelectStatement {
					what: vec![Value::RecordId(rid.clone()).into_literal()],
					expr: Fields::Value(Box::new(expr::Field::Single {
						expr: expr::Expr::Idiom(Idiom::field(fd_name.clone())),
						alias: None,
					})),
					only: true,
					..Default::default()
				};

				let plan = LogicalPlan {
					expressions: vec![TopLevelExpr::Expr(Expr::Select(Box::new(select_stmt)))],
				};

				let val = execute_plan(ds, sess, plan).await?;

				match val {
					Value::RecordId(rid) if fd_name != "id" => {
						// Check if this is an interface/union type that needs .with_type()
						let field_val = FieldValue::owned_any(rid.clone());
						let field_val = match field_kind {
							Some(Kind::Record(ts)) if ts.is_empty() || ts.len() > 1 => {
								// Interface or union type, needs .with_type()
								field_val.with_type(rid.table.to_string())
							}
							_ => {
								// Concrete type, no .with_type() needed
								field_val
							}
						};
						Ok(Some(field_val))
					}
					Value::None | Value::Null => Ok(None),
					v => {
						match field_kind {
							Some(Kind::Either(ks)) if ks.len() != 1 => {}
							_ => {}
						}
						let out = sql_value_to_gql_value(v.clone())
							.map_err(|_| "SQL to GQL translation failed")?;
						Ok(Some(FieldValue::value(out)))
					}
				}
			}
		})
	}
}

macro_rules! filter_impl {
	($filter:ident, $ty:ident, $name:expr_2021) => {
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
		Kind::None => {}
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
		Kind::String => {}
		Kind::Uuid => {}
		Kind::Regex => {}
		Kind::Table(_) => {}
		Kind::Record(_) => {}
		Kind::Geometry(_) => {}
		Kind::Either(_) => {}
		Kind::Set(_, _) => {}
		Kind::Array(_, _) => {}
		Kind::Function(_, _) => {}
		Kind::Range => {}
		Kind::Literal(_) => {}
		Kind::File(_) => {}
	};
	Ok(filter)
}

fn cond_from_filter(
	filter: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
) -> Result<Cond, GqlError> {
	val_from_filter(filter, fds).map(Cond)
}

fn val_from_filter(
	filter: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
) -> Result<Expr, GqlError> {
	if filter.len() != 1 {
		return Err(resolver_error("Table Filter must have one item"));
	}

	let (k, v) = filter.iter().next().expect("filter has exactly one item");

	match k.as_str().to_lowercase().as_str() {
		"or" => aggregate(v, AggregateOp::Or, fds),
		"and" => aggregate(v, AggregateOp::And, fds),
		"not" => negate(v, fds),
		_ => binop(k.as_str(), v, fds),
	}
}

fn parse_op(name: impl AsRef<str>) -> Result<expr::BinaryOperator, GqlError> {
	match name.as_ref() {
		"eq" => Ok(expr::BinaryOperator::Equal),
		"ne" => Ok(expr::BinaryOperator::NotEqual),
		op => Err(resolver_error(format!("Unsupported op: {op}"))),
	}
}

fn negate(filter: &GqlValue, fds: &[FieldDefinition]) -> Result<Expr, GqlError> {
	let obj = filter.as_object().ok_or(resolver_error("Value of NOT must be object"))?;
	let inner_cond = val_from_filter(obj, fds)?;

	Ok(Expr::Prefix {
		op: expr::PrefixOperator::Not,
		expr: Box::new(inner_cond),
	})
}

enum AggregateOp {
	And,
	Or,
}

fn aggregate(
	filter: &GqlValue,
	op: AggregateOp,
	fds: &[FieldDefinition],
) -> Result<Expr, GqlError> {
	let op_str = match op {
		AggregateOp::And => "AND",
		AggregateOp::Or => "OR",
	};
	let op = match op {
		AggregateOp::And => BinaryOperator::And,
		AggregateOp::Or => BinaryOperator::Or,
	};
	let list =
		filter.as_list().ok_or(resolver_error(format!("Value of {op_str} should be a list")))?;
	let filter_arr = list
		.iter()
		.map(|v| v.as_object().map(|o| val_from_filter(o, fds)))
		.collect::<Option<Result<Vec<Expr>, GqlError>>>()
		.ok_or(resolver_error(format!("List of {op_str} should contain objects")))??;

	let mut iter = filter_arr.into_iter();

	let mut cond = iter
		.next()
		.ok_or(resolver_error(format!("List of {op_str} should contain at least one object")))?;

	for clause in iter {
		cond = Expr::Binary {
			left: Box::new(clause),
			op: op.clone(),
			right: Box::new(cond),
		}
	}

	Ok(cond)
}

fn binop(field_name: &str, val: &GqlValue, fds: &[FieldDefinition]) -> Result<Expr, GqlError> {
	let obj = val.as_object().ok_or(resolver_error("Field filter should be object"))?;

	let Some(fd) = fds.iter().find(|fd| fd.name.to_string() == field_name) else {
		return Err(resolver_error(format!("Field `{field_name}` not found")));
	};

	if obj.len() != 1 {
		return Err(resolver_error("Field Filter must have one item"));
	}

	let lhs = Expr::Idiom(Idiom::field(field_name.to_string()));

	let (k, v) = obj.iter().next().expect("field filter has exactly one item");
	let op = parse_op(k)?;

	let rhs = gql_to_sql_kind(v, fd.field_kind.clone().unwrap_or_default())?;

	let expr = Expr::Binary {
		left: Box::new(lhs),
		op,
		right: Box::new(rhs.into_literal()),
	};

	Ok(expr)
}
