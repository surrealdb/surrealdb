use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::sync::Arc;

use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{
	Enum, Field, FieldFuture, FieldValue, InputObject, InputValue, Object, ResolverContext, Type,
	TypeRef,
};
use async_graphql::{Name, Value as GqlValue};
use surrealdb_types::ToSql;

use super::error::{GqlError, resolver_error};
use super::relations::{RelationDirection, RelationInfo};
use super::schema::{gql_to_sql_kind, sql_value_to_gql_value};
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, FieldDefinition, NamespaceId, TableDefinition};
use crate::dbs::Session;
use crate::expr::field::Selector;
use crate::expr::order::{OrderList, Ordering};
use crate::expr::part::Part;
use crate::expr::statements::SelectStatement;
use crate::expr::{
	self, BinaryOperator, Cond, Expr, Fields, Function, FunctionCall, Idiom, Kind, Limit, Literal,
	LogicalPlan, Start, TopLevelExpr,
};
use crate::gql::error::internal_error;
use crate::gql::schema::{geometry_gql_type_name, kind_to_type, unwrap_type};
use crate::gql::utils::{GqlValueUtils, execute_plan};
use crate::kvs::{Datastore, Transaction};
use crate::val::{Datetime, Object as SurObject, RecordId, TableName, Value};

fn order_asc(field_name: String) -> expr::Order {
	expr::Order {
		value: Idiom::field(field_name),
		direction: true,
		..Default::default()
	}
}

fn order_desc(field_name: String) -> expr::Order {
	expr::Order {
		value: Idiom::field(field_name),
		..expr::Order::default()
	}
}

/// A record ID with an optional version for temporal queries.
/// Propagates the version from top-level queries down to field and relation resolvers,
/// ensuring consistent versioned reads across the entire query tree.
///
/// Used as a fallback when full record data is not available (e.g., from custom
/// function return values). Prefer [`CachedRecord`] when the full record data
/// has already been fetched.
#[derive(Clone, Debug)]
pub(crate) struct VersionedRecord {
	pub rid: RecordId,
	pub version: Option<Datetime>,
}

/// A record with its full field data cached from a parent query.
///
/// Field resolvers extract values directly from the cached data without issuing
/// additional database queries, eliminating the N+1 query problem. When a list
/// query fetches `SELECT * FROM table`, the full objects are preserved in
/// `CachedRecord` instances and passed to field resolvers, which simply read
/// from the in-memory data instead of issuing per-field `SELECT VALUE` queries.
///
/// For record-link fields (`TYPE record<target>`), the resolver performs a
/// single `SELECT * FROM ONLY <target>` to fetch the linked record's full data
/// and wraps it in a new `CachedRecord`, so the target's field resolvers also
/// benefit from caching.
#[derive(Clone, Debug)]
pub(crate) struct CachedRecord {
	pub rid: RecordId,
	pub version: Option<Datetime>,
	/// The full record data. Field resolvers extract values from here
	/// instead of firing individual `SELECT VALUE` queries.
	pub data: SurObject,
}

/// Convert an optional `Datetime` version to the `Expr` representation
/// used in `SelectStatement.version`.
fn version_to_expr(version: &Option<Datetime>) -> Expr {
	match version {
		Some(dt) => Expr::Literal(Literal::Datetime(dt.clone())),
		None => Expr::Literal(Literal::None),
	}
}

/// Parse the optional `version` argument from GraphQL query arguments.
/// Expects an ISO 8601 / RFC 3339 datetime string (e.g. `"2024-06-01T00:00:00Z"`).
fn parse_version_arg(args: &IndexMap<Name, GqlValue>) -> Result<Option<Datetime>, GqlError> {
	match args.get("version") {
		Some(GqlValue::String(s)) => {
			let dt = crate::syn::datetime(s)
				.map_err(|_| resolver_error(format!("Invalid version datetime: {s}")))?;
			Ok(Some(dt.into()))
		}
		Some(GqlValue::Null) | None => Ok(None),
		Some(_) => Err(resolver_error("version must be a datetime string")),
	}
}

// ---------------------------------------------------------------------------
// Nested object and array sub-field resolution
// ---------------------------------------------------------------------------

/// Information about a sub-field of a nested object type.
struct NestedSubField {
	/// The GraphQL field name (e.g., "createdAt").
	name: String,
	/// The field's SurrealDB kind, if defined.
	kind: Option<Kind>,
	/// Optional comment from the field definition.
	comment: Option<String>,
}

/// Information about a parent field that has nested object children, requiring
/// a generated GraphQL Object type instead of the opaque `object` scalar.
struct NestedObjectInfo {
	/// The generated GraphQL type name (e.g., "item_time").
	gql_type_name: String,
	/// Whether the parent field is `TYPE array<object>` (vs. plain `TYPE object`).
	is_array: bool,
	/// Whether the parent field type is optional (nullable in GraphQL).
	optional: bool,
	/// The direct sub-fields of this nested object.
	sub_fields: Vec<NestedSubField>,
}

/// Analyze field definitions for a table and detect fields with nested object
/// children.
///
/// A parent field of `TYPE object` with children like `time.createdAt` or a
/// parent of `TYPE array<object>` with wildcard children like `tags.*.name`
/// will be detected. Returns a map from the parent field name to its nested
/// object info.
///
/// Currently handles one level of nesting (direct children of top-level fields).
fn detect_nested_objects(
	table_name: &str,
	fds: &[FieldDefinition],
) -> HashMap<String, NestedObjectInfo> {
	let mut children_by_parent: HashMap<String, Vec<NestedSubField>> = HashMap::new();
	let mut parent_has_wildcard: HashMap<String, bool> = HashMap::new();

	for fd in fds.iter() {
		let parts = &fd.name.0;
		if parts.len() < 2 {
			continue; // Skip top-level fields
		}

		// Get the parent field name (first part must be a Field)
		let parent_name = match &parts[0] {
			Part::Field(name) => name.clone(),
			_ => continue,
		};

		// Get the child field name (last part must be a Field)
		let child_name = match parts.last() {
			Some(Part::Field(name)) => name.clone(),
			_ => continue,
		};

		// Check if there's a Part::All (wildcard `*`) between parent and child
		let has_wildcard = parts[1..parts.len() - 1].iter().any(|p| matches!(p, Part::All));

		// Only handle direct children:
		// - depth 2 for plain object: [Field("parent"), Field("child")]
		// - depth 3 for array element: [Field("parent"), All, Field("child")]
		let expected_len = if has_wildcard {
			3
		} else {
			2
		};
		if parts.len() != expected_len {
			continue; // Skip deeper nesting for now
		}

		parent_has_wildcard.entry(parent_name.clone()).or_insert(has_wildcard);

		children_by_parent.entry(parent_name.clone()).or_default().push(NestedSubField {
			name: child_name,
			kind: fd.field_kind.clone(),
			comment: fd.comment.clone(),
		});
	}

	// Now verify that each parent actually exists and is of the right type
	// (TYPE object or TYPE array<object>, including option<...> variants)
	let mut result = HashMap::new();

	for (parent_name, sub_fields) in children_by_parent {
		// Find the parent field definition
		let parent_fd = fds.iter().find(|fd| {
			fd.name.0.len() == 1 && matches!(&fd.name.0[0], Part::Field(n) if n == &parent_name)
		});

		let is_array = parent_has_wildcard.get(&parent_name).copied().unwrap_or(false);

		// Verify the parent is `TYPE object` or `TYPE array<object>` (or their
		// option<...> variants). Also track whether the type is optional.
		let parent_kind = parent_fd.and_then(|fd| fd.field_kind.as_ref());
		let (kind_ok, optional) = match parent_kind {
			Some(Kind::Object) if !is_array => (true, false),
			Some(Kind::Array(inner, _)) if is_array => (matches!(**inner, Kind::Object), false),
			// Handle option<object> = Either([None, Object])
			// and option<array<object>> = Either([None, Array(Object)])
			Some(Kind::Either(ks)) => {
				let has_none = ks.iter().any(|k| matches!(k, Kind::None));
				if !is_array {
					let has_object = ks.iter().any(|k| matches!(k, Kind::Object));
					(has_none && has_object, has_none)
				} else {
					let has_array_obj = ks.iter().any(
						|k| matches!(k, Kind::Array(inner, _) if matches!(**inner, Kind::Object)),
					);
					(has_none && has_array_obj, has_none)
				}
			}
			// Also allow flexible/untyped parents if they have children defined
			None => (true, true),
			_ => (false, false),
		};

		if !kind_ok {
			continue;
		}

		let gql_type_name = format!("{table_name}_{parent_name}");
		result.insert(
			parent_name,
			NestedObjectInfo {
				gql_type_name,
				is_array,
				optional,
				sub_fields,
			},
		);
	}

	result
}

/// Build a GraphQL Object type for a nested object (e.g., `item_time`).
///
/// Sub-fields are resolved by extracting values from the parent `SurObject`.
fn make_nested_object_type(
	type_name: &str,
	sub_fields: &[NestedSubField],
	types: &mut Vec<Type>,
) -> Result<Object, GqlError> {
	let mut obj = Object::new(type_name);

	for sf in sub_fields {
		let Some(ref kind) = sf.kind else {
			continue;
		};
		let fd_type = kind_to_type(kind.clone(), types, false)?;
		let resolver = make_sub_field_resolver(sf.name.clone(), sf.kind.clone());
		let mut field = Field::new(&sf.name, fd_type, resolver);
		if let Some(ref comment) = sf.comment {
			field = field.description(comment.clone());
		}
		obj = obj.field(field);
	}

	Ok(obj)
}

/// Create a resolver for a sub-field within a nested object type.
///
/// The resolver downcasts the parent value to `SurObject` and extracts the
/// named field, converting it to the appropriate GraphQL value.
fn make_sub_field_resolver(
	field_name: String,
	kind: Option<Kind>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	move |ctx: ResolverContext| {
		let field_name = field_name.clone();
		let field_kind = kind.clone();
		FieldFuture::new(async move {
			let obj = ctx.parent_value.try_downcast_ref::<SurObject>()?;

			match obj.get(&field_name) {
				Some(val) => match val {
					Value::None | Value::Null => Ok(None),
					Value::RecordId(rid) => {
						// Record-link: store as owned_any for dereferencing
						let field_val = FieldValue::owned_any(VersionedRecord {
							rid: rid.clone(),
							version: None,
						});
						let field_val = match field_kind {
							Some(Kind::Record(ref ts)) if ts.is_empty() || ts.len() > 1 => {
								field_val.with_type(rid.table.clone())
							}
							_ => field_val,
						};
						Ok(Some(field_val))
					}
					Value::Geometry(g) => {
						let type_name = geometry_gql_type_name(g);
						let field_val = FieldValue::owned_any(g.clone());
						let field_val = match &field_kind {
							Some(Kind::Geometry(ks)) if ks.is_empty() || ks.len() > 1 => {
								field_val.with_type(type_name)
							}
							_ => field_val,
						};
						Ok(Some(field_val))
					}
					v => {
						let gql_val = sql_value_to_gql_value(v.clone())
							.map_err(|_| "SQL to GQL translation failed")?;
						Ok(Some(FieldValue::value(gql_val)))
					}
				},
				None => Ok(None),
			}
		})
	}
}

/// Create a resolver for a parent field that is a nested object (`TYPE object`
/// with sub-fields). Returns the `SurObject` as `owned_any` so sub-field
/// resolvers can extract values from it.
fn make_nested_object_field_resolver(
	fd_name: impl Into<String>,
	is_array: bool,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	let fd_name = fd_name.into();
	move |ctx: ResolverContext| {
		let fd_name = fd_name.clone();
		FieldFuture::new(async move {
			// ── Fast path: extract nested object from CachedRecord ──
			if let Ok(cached) = ctx.parent_value.try_downcast_ref::<CachedRecord>() {
				let val = cached.data.get(&fd_name).cloned().unwrap_or(Value::None);
				return resolve_nested_object_value(val, is_array);
			}

			// ── Slow path: fetch via database query ──
			let ds = ctx.data::<Arc<Datastore>>()?;
			let sess = ctx.data::<Arc<Session>>()?;

			// Extract record ID and optional version
			let (rid, version) = match ctx.parent_value.try_downcast_ref::<VersionedRecord>() {
				Ok(vr) => (vr.rid.clone(), vr.version.clone()),
				Err(_) => {
					let rid = ctx.parent_value.try_downcast_ref::<RecordId>()?;
					(rid.clone(), None)
				}
			};

			// Build SELECT VALUE <field> FROM ONLY <record_id>
			let select_stmt = SelectStatement {
				what: vec![Value::RecordId(rid.clone()).into_literal()],
				fields: Fields::Value(Box::new(Selector {
					expr: Expr::Idiom(Idiom::field(fd_name.clone())),
					alias: None,
				})),
				only: true,
				omit: vec![],
				with: None,
				cond: None,
				split: None,
				group: None,
				order: None,
				limit: None,
				start: None,
				fetch: None,
				version: version_to_expr(&version),
				timeout: Expr::Literal(Literal::None),
				explain: None,
				tempfiles: false,
			};

			let plan = LogicalPlan {
				expressions: vec![TopLevelExpr::Expr(Expr::Select(Box::new(select_stmt)))],
			};

			let val = execute_plan(ds, sess, plan).await?;
			resolve_nested_object_value(val, is_array)
		})
	}
}

/// Convert a nested object/array-of-object value to a GraphQL `FieldValue`.
///
/// For arrays, each `Value::Object` element becomes a `FieldValue::owned_any(SurObject(..))`.
/// For plain objects, the `SurObject` is returned directly.
fn resolve_nested_object_value(
	val: Value,
	is_array: bool,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	if is_array {
		match val {
			Value::Array(arr) => {
				let items: Vec<FieldValue> = arr
					.0
					.into_iter()
					.filter_map(|v| match v {
						Value::Object(obj) => Some(FieldValue::owned_any(obj)),
						_ => None,
					})
					.collect();
				Ok(Some(FieldValue::list(items)))
			}
			Value::None | Value::Null => Ok(None),
			_ => Ok(None),
		}
	} else {
		match val {
			Value::Object(obj) => Ok(Some(FieldValue::owned_any(obj))),
			Value::None | Value::Null => Ok(None),
			_ => {
				let out =
					sql_value_to_gql_value(val).map_err(|_| "SQL to GQL translation failed")?;
				Ok(Some(FieldValue::value(out)))
			}
		}
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
	relations: &[RelationInfo],
) -> Result<Object, GqlError> {
	// Pre-fetch field definitions for relation tables (needed for filter support
	// in relation field resolvers). These are captured by the resolver closures.
	let mut relation_table_fds: HashMap<String, Arc<[FieldDefinition]>> = HashMap::new();
	for rel in relations.iter() {
		let rel_name = rel.table_name.clone().into_string();
		if let std::collections::hash_map::Entry::Vacant(e) = relation_table_fds.entry(rel_name) {
			let fds = tx.all_tb_fields(ns, db, &rel.table_name, None).await?;
			e.insert(fds);
		}
	}

	// Set of exposed table names for checking that relation targets are visible
	let exposed_table_names: HashSet<String> =
		tbs.iter().map(|t| t.name.clone().into_string()).collect();

	for tb in tbs.iter() {
		trace!("Adding table: {}", tb.name);
		let tb_name = tb.name.clone();
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

		// Detect nested object fields (TYPE object with children, TYPE array<object>
		// with wildcard children). These will get generated GraphQL Object types
		// instead of the opaque `object` scalar.
		let nested_objects = detect_nested_objects(&tb.name.clone().into_string(), &fds);

		let fds1 = fds.clone();
		let kvs1 = datastore.clone();

		query = query.field(
			Field::new(
				tb.name.clone().into_string(),
				TypeRef::named_nn_list_nn(tb.name.clone().into_string()),
				move |ctx| {
					let tb_name = first_tb_name.clone();
					let fds1 = fds1.clone();
					let kvs1 = kvs1.clone();
					FieldFuture::new(async move {
						// Get session from GraphQL context (has proper user permissions)
						let sess1 = ctx.data::<Arc<Session>>()?;
						let args = ctx.args.as_index_map();
						trace!("received request with args: {args:?}");

						let start = args
							.get("start")
							.and_then(|v| v.as_i64())
							.map(|s| Start(Expr::Literal(Literal::Integer(s))));
						let limit = args
							.get("limit")
							.and_then(|v| v.as_i64())
							.map(|l| Limit(Expr::Literal(Literal::Integer(l))));
						let version = parse_version_arg(args)?;
						let order = args.get("order");
						// Accept both `filter` and `where` (aliases of each other)
						let filter = args.get("filter").or_else(|| args.get("where"));

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
										error!(
										"Found filter {f}, which should be object and should have been rejected by async graphql."
									);
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
						let expr = expr::Expr::Select(Box::new(SelectStatement {
							what: vec![Expr::Table(tb_name)],
							fields: Fields::all(),
							order: orders.map(|x| Ordering::Order(OrderList(x))),
							cond,
							limit,
							start,
							omit: vec![],
							only: false,
							with: None,
							split: None,
							group: None,
							fetch: None,
							version: version_to_expr(&version),
							timeout: Expr::Literal(Literal::None),
							explain: None,
							tempfiles: false,
						}));

						// Convert to LogicalPlan and execute
						let plan = LogicalPlan {
							expressions: vec![TopLevelExpr::Expr(expr)],
						};

						let res = execute_plan(&kvs1, sess1, plan).await?;

						let res_vec =
							match res {
								Value::Array(a) => a,
								v => {
									error!("Found top level value, in result which should be array: {v:?}");
									return Err("Internal Error".into());
								}
							};

						// Wrap each result object as a CachedRecord, preserving
						// the full record data so that field resolvers can extract
						// values directly without additional database queries.
						let out: Result<Vec<FieldValue>, GqlError> = res_vec
							.0
							.into_iter()
							.map(|v| match v {
								Value::Object(obj) => {
									let rid = match obj.get("id") {
										Some(Value::RecordId(rid)) => rid.clone(),
										_ => {
											error!(
												"Object missing 'id' field or id is not a RecordId: {obj:?}"
											);
											return Err(internal_error(
												"Record missing 'id' field",
											));
										}
									};
									Ok(FieldValue::owned_any(CachedRecord {
										rid,
										version: version.clone(),
										data: obj,
									}))
								}
								_ => {
									error!(
										"Found top level value, in result which should be object: {v:?}"
									);
									Err(internal_error("Expected object in result"))
								}
							})
							.collect();

						match out {
							Ok(l) => Ok(Some(FieldValue::list(l))),
							Err(e) => Err(e.into()),
						}
					})
				},
			)
			.description(if let Some(c) = &tb.comment {
				c.clone()
			} else {
				format!("Generated from table `{}`\nallows querying a table with filters", tb.name)
			})
			.argument(InputValue::new("limit", TypeRef::named(TypeRef::INT)))
			.argument(InputValue::new("start", TypeRef::named(TypeRef::INT)))
			.argument(InputValue::new("order", TypeRef::named(&table_order_name)))
			.argument(InputValue::new("filter", TypeRef::named(&table_filter_name)))
			.argument(InputValue::new("where", TypeRef::named(&table_filter_name)))
			.argument(InputValue::new("version", TypeRef::named(TypeRef::STRING))),
		);

		let kvs2 = datastore.to_owned();
		query = query.field(
			Field::new(
				format!("_get_{}", tb.name),
				TypeRef::named(tb.name.clone().into_string()),
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
							let version = parse_version_arg(args)?;

							// Parse the full record id string so that numeric
							// keys like "1" are correctly interpreted as Number(1)
							// rather than String("1"). Fall back to a plain string
							// key if parsing fails.
							let rid_str = format!("{tb_name}:{id}");
							let record_id: RecordId = match crate::syn::record_id(&rid_str) {
								Ok(x) => x.into(),
								Err(_) => RecordId::new(tb_name, id),
							};

							// Build SELECT * FROM ONLY <record_id> and cache
							// the full record for field resolvers.
							let select_stmt = SelectStatement {
								what: vec![Value::RecordId(record_id.clone()).into_literal()],
								fields: Fields::all(),
								only: true,
								omit: vec![],
								with: None,
								cond: None,
								split: None,
								group: None,
								order: None,
								limit: None,
								start: None,
								fetch: None,
								version: version_to_expr(&version),
								timeout: Expr::Literal(Literal::None),
								explain: None,
								tempfiles: false,
							};

							let plan = LogicalPlan {
								expressions: vec![TopLevelExpr::Expr(Expr::Select(Box::new(
									select_stmt,
								)))],
							};

							let res = execute_plan(&kvs2, sess2, plan).await?;

							match res {
								Value::Object(obj) => {
									let rid = match obj.get("id") {
										Some(Value::RecordId(rid)) => rid.clone(),
										_ => return Ok(None),
									};
									Ok(Some(FieldValue::owned_any(CachedRecord {
										rid,
										version,
										data: obj,
									})))
								}
								_ => Ok(None),
							}
						}
					})
				},
			)
			.description(if let Some(c) = &tb.comment {
				c.clone()
			} else {
				format!(
					"Generated from table `{}`\nallows querying a single record in a table by ID",
					tb.name
				)
			})
			.argument(InputValue::new("id", TypeRef::named_nn(TypeRef::ID)))
			.argument(InputValue::new("version", TypeRef::named(TypeRef::STRING))),
		);

		let mut table_ty_obj = Object::new(tb.name.clone().into_string())
			.field(Field::new(
				"id",
				TypeRef::named_nn(TypeRef::ID),
				make_table_field_resolver("id", Some(Kind::Record(vec![tb.name.clone()]))),
			))
			.implement("record");

		// Track existing field names to detect conflicts with relation fields
		let mut existing_field_names: HashSet<String> = HashSet::new();
		existing_field_names.insert("id".to_string());

		for fd in fds.iter() {
			let Some(ref kind) = fd.field_kind else {
				continue;
			};
			if fd.name.is_id() {
				// We have already defined "id"
				// so we don't take any new definition for it.
				continue;
			};

			// Skip nested child fields (multi-part idioms like `time.createdAt`
			// or `tags.*.name`). These are handled by their parent's generated
			// nested object type.
			if fd.name.0.len() > 1 {
				continue;
			}

			let fd_name = Name::new(fd.name.to_sql());
			existing_field_names.insert(fd_name.to_string());

			// Check if this field has nested children and should use a generated
			// GraphQL Object type instead of the default scalar/type mapping.
			if let Some(nested) = nested_objects.get(fd_name.as_str()) {
				// Generate the nested object type and register it
				let nested_type =
					make_nested_object_type(&nested.gql_type_name, &nested.sub_fields, types)?;
				types.push(Type::Object(nested_type));

				// Determine the GraphQL type for this field, respecting nullability.
				let fd_type = if nested.is_array {
					let list = TypeRef::List(Box::new(TypeRef::named_nn(&nested.gql_type_name)));
					if nested.optional {
						list
					} else {
						TypeRef::NonNull(Box::new(list))
					}
				} else if nested.optional {
					TypeRef::named(&nested.gql_type_name)
				} else {
					TypeRef::named_nn(&nested.gql_type_name)
				};

				// Add to orderable (parent field name only, not sub-fields)
				table_orderable = table_orderable.item(fd_name.to_string());

				// Use the nested object resolver instead of the default one
				table_ty_obj = table_ty_obj
					.field(Field::new(
						fd_name.as_str(),
						fd_type,
						make_nested_object_field_resolver(
							fd_name.as_str().to_string(),
							nested.is_array,
						),
					))
					.description(if let Some(ref c) = fd.comment {
						c.clone()
					} else {
						format!("Nested object field `{}`", fd_name.as_str())
					});

				continue;
			}

			let fd_type = kind_to_type(kind.clone(), types, false)?;
			table_orderable = table_orderable.item(fd_name.to_string());
			let type_filter_name = format!("_filter_{}", unwrap_type(fd_type.clone()));

			// Only create a new filter type if one with this name hasn't been registered yet.
			// This prevents record-link fields (e.g. `TYPE record<department>`) from
			// overwriting the target table's own, richer filter type.
			let filter_already_exists = types.iter().any(|t| match t {
				Type::InputObject(io) => io.type_name() == type_filter_name,
				_ => false,
			});
			if !filter_already_exists {
				let type_filter = Type::InputObject(filter_from_type(
					kind.clone(),
					type_filter_name.clone(),
					types,
				)?);
				trace!("\n{type_filter:?}\n");
				types.push(type_filter);
			}

			table_filter = table_filter
				.field(InputValue::new(fd.name.to_sql(), TypeRef::named(type_filter_name)));

			table_ty_obj = table_ty_obj
				.field(Field::new(
					fd.name.to_sql(),
					fd_type,
					make_table_field_resolver(fd_name.as_str(), fd.field_kind.clone()),
				))
				.description(if let Some(ref c) = fd.comment {
					c.clone()
				} else {
					"".to_string()
				});
		}

		// Add relation fields to this table's type.
		// For each relation table where this table is in the FROM list, add an
		// outgoing relation field. For each where this table is in the TO list,
		// add an incoming relation field.
		let tb_name_str = tb.name.clone().into_string();
		for rel in relations.iter() {
			let rel_table_str = rel.table_name.clone().into_string();

			// Only add relation fields if the relation table is also exposed
			if !exposed_table_names.contains(&rel_table_str) {
				continue;
			}

			let rel_fds = relation_table_fds.get(&rel_table_str).cloned();

			// Outgoing: this table is in the FROM list
			if rel.from_tables.contains(&tb_name_str) {
				let field_name = rel_table_str.clone();
				if !existing_field_names.contains(&field_name) {
					existing_field_names.insert(field_name.clone());
					table_ty_obj = table_ty_obj.field(make_relation_field(
						&field_name,
						&rel_table_str,
						rel.table_name.clone(),
						RelationDirection::Outgoing,
						rel_fds.clone(),
					));
				} else {
					trace!(
						"Skipping outgoing relation field '{}' on table '{}': \
						 conflicts with existing field",
						field_name, tb_name_str
					);
				}
			}

			// Incoming: this table is in the TO list
			if rel.to_tables.contains(&tb_name_str) {
				let field_name = format!("{}_in", rel_table_str);
				if !existing_field_names.contains(&field_name) {
					existing_field_names.insert(field_name.clone());
					table_ty_obj = table_ty_obj.field(make_relation_field(
						&field_name,
						&rel_table_str,
						rel.table_name.clone(),
						RelationDirection::Incoming,
						rel_fds.clone(),
					));
				} else {
					trace!(
						"Skipping incoming relation field '{}' on table '{}': \
						 conflicts with existing field",
						field_name, tb_name_str
					);
				}
			}
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
					let version = parse_version_arg(args)?;

					// Parse ID as a record id.
					let record_id: crate::val::RecordId = match crate::syn::record_id(&id) {
						Ok(x) => x.into(),
						Err(_) => {
							return Err(resolver_error("Invalid record ID format").into());
						}
					};

					// Build SELECT * FROM ONLY <record_id> and cache
					// the full record for field resolvers.
					let select_stmt = SelectStatement {
						what: vec![Value::RecordId(record_id.clone()).into_literal()],
						fields: Fields::all(),
						only: true,
						omit: vec![],
						with: None,
						cond: None,
						split: None,
						group: None,
						order: None,
						limit: None,
						start: None,
						fetch: None,
						version: version_to_expr(&version),
						timeout: Expr::Literal(Literal::None),
						explain: None,
						tempfiles: false,
					};

					let plan = LogicalPlan {
						expressions: vec![TopLevelExpr::Expr(Expr::Select(Box::new(select_stmt)))],
					};

					let res = execute_plan(&kvs3, sess3, plan).await?;

					match res {
						Value::Object(obj) => {
							let rid = match obj.get("id") {
								Some(Value::RecordId(rid)) => rid.clone(),
								_ => return Ok(None),
							};
							let table_name = rid.table.clone();
							// Generic _get returns interface type "record", needs .with_type()
							Ok(Some(
								FieldValue::owned_any(CachedRecord {
									rid,
									version,
									data: obj,
								})
								.with_type(table_name),
							))
						}
						_ => Ok(None),
					}
				}
			})
		})
		.description("Allows fetching arbitrary records".to_string())
		.argument(InputValue::new("id", TypeRef::named_nn(TypeRef::ID)))
		.argument(InputValue::new("version", TypeRef::named(TypeRef::STRING))),
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
				// ── Fast path: extract field from CachedRecord ──
				//
				// When the parent is a CachedRecord (from a list query, _get_,
				// relation, or mutation), the full record data is already in
				// memory. Extract the requested field directly instead of
				// issuing a separate database query.
				if let Ok(cached) = ctx.parent_value.try_downcast_ref::<CachedRecord>() {
					return resolve_field_from_cached_record(&ctx, cached, &fd_name, &field_kind)
						.await;
				}

				// ── Slow path: fetch field via database query ──
				//
				// Fallback for VersionedRecord (no cached data) or plain
				// RecordId (from custom functions, etc.).
				let ds = ctx.data::<Arc<Datastore>>()?;
				let sess = ctx.data::<Arc<Session>>()?;

				let (rid, version) = match ctx.parent_value.try_downcast_ref::<VersionedRecord>() {
					Ok(vr) => (vr.rid.clone(), vr.version.clone()),
					Err(_) => {
						let rid = ctx.parent_value.try_downcast_ref::<RecordId>()?;
						(rid.clone(), None)
					}
				};

				// Build SELECT VALUE <field> FROM ONLY <record_id>
				let select_stmt = SelectStatement {
					what: vec![Value::RecordId(rid.clone()).into_literal()],
					fields: Fields::Value(Box::new(Selector {
						expr: expr::Expr::Idiom(Idiom::field(fd_name.clone())),
						alias: None,
					})),
					only: true,
					omit: vec![],
					with: None,
					cond: None,
					split: None,
					group: None,
					order: None,
					limit: None,
					start: None,
					fetch: None,
					version: version_to_expr(&version),
					timeout: Expr::Literal(Literal::None),
					explain: None,
					tempfiles: false,
				};

				let plan = LogicalPlan {
					expressions: vec![TopLevelExpr::Expr(Expr::Select(Box::new(select_stmt)))],
				};

				let val = execute_plan(ds, sess, plan).await?;
				resolve_field_value(&ctx, val, &fd_name, &field_kind, &version).await
			}
		})
	}
}

/// Convert a resolved field value to a GraphQL `FieldValue`.
///
/// Handles record-link dereferencing (fetching the target record's full data
/// for caching), geometry values, and scalar conversions. Used by both the
/// cached and uncached paths in `make_table_field_resolver`.
async fn resolve_field_value(
	ctx: &ResolverContext<'_>,
	val: Value,
	fd_name: &str,
	field_kind: &Option<Kind>,
	version: &Option<Datetime>,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	match val {
		Value::RecordId(target_rid) if fd_name != "id" => {
			// Record-link dereferencing: fetch the full target record and
			// wrap it as CachedRecord so the target's field resolvers can
			// also benefit from caching.
			let ds = ctx.data::<Arc<Datastore>>()?;
			let sess = ctx.data::<Arc<Session>>()?;

			let select_stmt = SelectStatement {
				what: vec![Value::RecordId(target_rid.clone()).into_literal()],
				fields: Fields::all(),
				only: true,
				omit: vec![],
				with: None,
				cond: None,
				split: None,
				group: None,
				order: None,
				limit: None,
				start: None,
				fetch: None,
				version: version_to_expr(version),
				timeout: Expr::Literal(Literal::None),
				explain: None,
				tempfiles: false,
			};

			let plan = LogicalPlan {
				expressions: vec![TopLevelExpr::Expr(Expr::Select(Box::new(select_stmt)))],
			};

			let target_val = execute_plan(ds, sess, plan).await?;

			match target_val {
				Value::Object(obj) => {
					let field_val = FieldValue::owned_any(CachedRecord {
						rid: target_rid.clone(),
						version: version.clone(),
						data: obj,
					});
					let field_val = match field_kind {
						Some(Kind::Record(ts)) if ts.is_empty() || ts.len() > 1 => {
							field_val.with_type(target_rid.table)
						}
						_ => field_val,
					};
					Ok(Some(field_val))
				}
				Value::None | Value::Null => Ok(None),
				_ => Ok(None),
			}
		}
		Value::Geometry(g) => {
			let type_name = geometry_gql_type_name(&g);
			let field_val = FieldValue::owned_any(g);
			let field_val = match field_kind {
				Some(Kind::Geometry(ks)) if ks.is_empty() || ks.len() > 1 => {
					field_val.with_type(type_name)
				}
				_ => field_val,
			};
			Ok(Some(field_val))
		}
		Value::None | Value::Null => Ok(None),
		v => {
			let out = sql_value_to_gql_value(v).map_err(|_| "SQL to GQL translation failed")?;
			Ok(Some(FieldValue::value(out)))
		}
	}
}

/// Fast-path field resolution from a [`CachedRecord`].
///
/// Extracts the field value directly from the cached record data. For
/// record-link fields, fetches the linked record's full data in a single
/// `SELECT *` query (instead of N per-field queries).
async fn resolve_field_from_cached_record(
	ctx: &ResolverContext<'_>,
	cached: &CachedRecord,
	fd_name: &str,
	field_kind: &Option<Kind>,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	let val = cached.data.get(fd_name).cloned().unwrap_or(Value::None);
	resolve_field_value(ctx, val, fd_name, field_kind, &cached.version).await
}

/// Build a GraphQL field for a relation on a table type.
///
/// The field returns a list of records from the relation table, filtered by
/// the current record's id on the appropriate side (`in` for outgoing, `out`
/// for incoming). Supports `limit`, `start`, `order`, and `filter` arguments.
fn make_relation_field(
	field_name: &str,
	rel_table_type_name: &str,
	rel_table_name: TableName,
	direction: RelationDirection,
	rel_fds: Option<Arc<[FieldDefinition]>>,
) -> Field {
	let table_filter_name = filter_name_from_table(rel_table_type_name);
	let table_order_name = format!("_order_{}", rel_table_type_name);

	let desc = match direction {
		RelationDirection::Outgoing => {
			format!("Outgoing `{}` relations from this record", rel_table_type_name)
		}
		RelationDirection::Incoming => {
			format!("Incoming `{}` relations to this record", rel_table_type_name)
		}
	};

	Field::new(
		field_name,
		TypeRef::named_nn_list_nn(rel_table_type_name),
		make_relation_field_resolver(rel_table_name, direction, rel_fds),
	)
	.description(desc)
	.argument(InputValue::new("limit", TypeRef::named(TypeRef::INT)))
	.argument(InputValue::new("start", TypeRef::named(TypeRef::INT)))
	.argument(InputValue::new("order", TypeRef::named(&table_order_name)))
	.argument(InputValue::new("filter", TypeRef::named(&table_filter_name)))
	.argument(InputValue::new("where", TypeRef::named(&table_filter_name)))
}

/// Create a resolver for a relation field.
///
/// The resolver:
/// 1. Extracts the parent record's id
/// 2. Builds `SELECT * FROM <relation_table> WHERE <in|out> = $current_record`
/// 3. Optionally combines with user-supplied filter, ordering, and pagination
/// 4. Returns the matching relation records as a list
fn make_relation_field_resolver(
	relation_table_name: TableName,
	direction: RelationDirection,
	rel_fds: Option<Arc<[FieldDefinition]>>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	move |ctx: ResolverContext| {
		let relation_table = relation_table_name.clone();
		let fds = rel_fds.clone();
		FieldFuture::new(async move {
			let ds = ctx.data::<Arc<Datastore>>()?;
			let sess = ctx.data::<Arc<Session>>()?;

			// Extract record ID and optional version from parent.
			// Try CachedRecord first, then VersionedRecord, then plain RecordId.
			let (rid, version) =
				if let Ok(cached) = ctx.parent_value.try_downcast_ref::<CachedRecord>() {
					(cached.rid.clone(), cached.version.clone())
				} else if let Ok(vr) = ctx.parent_value.try_downcast_ref::<VersionedRecord>() {
					(vr.rid.clone(), vr.version.clone())
				} else {
					let rid = ctx.parent_value.try_downcast_ref::<RecordId>()?;
					(rid.clone(), None)
				};
			let args = ctx.args.as_index_map();

			// Parse limit/start arguments
			let start = args
				.get("start")
				.and_then(|v| v.as_i64())
				.map(|s| Start(Expr::Literal(Literal::Integer(s))));
			let limit = args
				.get("limit")
				.and_then(|v| v.as_i64())
				.map(|l| Limit(Expr::Literal(Literal::Integer(l))));

			// Parse order argument
			let order = args.get("order");
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
							(_, _) => break,
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

			// Build the base condition: WHERE in = $record or WHERE out = $record
			let filter_field = match direction {
				RelationDirection::Outgoing => "in",
				RelationDirection::Incoming => "out",
			};
			let mut base_cond = Expr::Binary {
				left: Box::new(Expr::Idiom(Idiom::field(filter_field.to_string()))),
				op: BinaryOperator::Equal,
				right: Box::new(Value::RecordId(rid.clone()).into_literal()),
			};

			// Parse and combine user-supplied filter (accept both `filter` and `where`)
			let filter = args.get("filter").or_else(|| args.get("where"));
			if let Some(f) = filter
				&& let Some(ref fds) = fds
			{
				let o = match f {
					GqlValue::Object(o) => o,
					f => {
						error!(
							"Found filter {f}, which should be object and should have \
							 been rejected by async graphql."
						);
						return Err("Value in cond doesn't fit schema".into());
					}
				};
				let user_cond = cond_from_filter(o, fds)?;
				base_cond = Expr::Binary {
					left: Box::new(base_cond),
					op: BinaryOperator::And,
					right: Box::new(user_cond.0),
				};
			}

			let cond = Some(Cond(base_cond));

			// Build SELECT * FROM <relation_table> WHERE ...
			// Propagate version from parent for consistent temporal queries
			let select_stmt = SelectStatement {
				what: vec![Expr::Table(relation_table)],
				fields: Fields::all(),
				order: orders.map(|x| Ordering::Order(OrderList(x))),
				cond,
				limit,
				start,
				omit: vec![],
				only: false,
				with: None,
				split: None,
				group: None,
				fetch: None,
				version: version_to_expr(&version),
				timeout: Expr::Literal(Literal::None),
				explain: None,
				tempfiles: false,
			};

			let plan = LogicalPlan {
				expressions: vec![TopLevelExpr::Expr(Expr::Select(Box::new(select_stmt)))],
			};

			let res = execute_plan(ds, sess, plan).await?;

			let res_vec = match res {
				Value::Array(a) => a,
				v => {
					error!("Expected array result for relation query, found: {v:?}");
					return Err(internal_error("Unexpected result type for relation query").into());
				}
			};

			// Wrap each relation result as CachedRecord so that the
			// relation table's field resolvers can use the cache path.
			let out: Result<Vec<FieldValue>, GqlError> = res_vec
				.0
				.into_iter()
				.map(|v| match v {
					Value::Object(obj) => {
						let rid = match obj.get("id") {
							Some(Value::RecordId(rid)) => rid.clone(),
							_ => {
								error!(
									"Relation object missing 'id' field or id is not a RecordId"
								);
								return Err(internal_error("Unexpected relation result format"));
							}
						};
						Ok(FieldValue::owned_any(CachedRecord {
							rid,
							version: version.clone(),
							data: obj,
						}))
					}
					_ => {
						error!("Expected object in relation result, found: {v:?}");
						Err(internal_error("Unexpected value type in relation result"))
					}
				})
				.collect();

			Ok(Some(FieldValue::list(out?)))
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
	// `in` accepts a list of IDs
	let list_ty = TypeRef::named_nn_list(TypeRef::ID);
	filter_impl!(filter, list_ty, "in");
	filter
}

fn filter_from_type(
	kind: Kind,
	filter_name: String,
	types: &mut Vec<Type>,
) -> Result<InputObject, GqlError> {
	// Normalise `option<record<T>>` (Kind::Either([None, Record([T])])) down to the
	// inner record kind so filters are generated correctly with ID-based filtering.
	let effective_kind = match &kind {
		Kind::Either(ks) => {
			let non_none: Vec<&Kind> =
				ks.iter().filter(|k| !matches!(k, Kind::None | Kind::Null)).collect();
			if non_none.len() == 1 {
				non_none[0].clone()
			} else {
				kind.clone()
			}
		}
		_ => kind.clone(),
	};

	let ty = match &effective_kind {
		Kind::Record(ts) => match ts.len() {
			1 => TypeRef::named(filter_name_from_table(
				ts.first().expect("ts should have exactly one element").as_str(),
			)),
			_ => TypeRef::named(TypeRef::ID),
		},
		k => unwrap_type(kind_to_type(k.clone(), types, true)?),
	};

	// All types get eq and ne
	let mut filter = InputObject::new(filter_name);
	filter_impl!(filter, ty, "eq");
	filter_impl!(filter, ty, "ne");

	match effective_kind {
		// String: contains, startsWith, endsWith, regex, in
		Kind::String => {
			let str_ty = TypeRef::named(TypeRef::STRING);
			filter_impl!(filter, str_ty, "contains");
			filter_impl!(filter, str_ty, "startsWith");
			filter_impl!(filter, str_ty, "endsWith");
			filter_impl!(filter, str_ty, "regex");
			let list_ty = TypeRef::named_nn_list(TypeRef::STRING);
			filter_impl!(filter, list_ty, "in");
		}
		// Numeric types: gt, gte, lt, lte, in
		Kind::Int => {
			let num_ty = TypeRef::named(TypeRef::INT);
			filter_impl!(filter, num_ty, "gt");
			filter_impl!(filter, num_ty, "gte");
			filter_impl!(filter, num_ty, "lt");
			filter_impl!(filter, num_ty, "lte");
			let list_ty = TypeRef::named_nn_list(TypeRef::INT);
			filter_impl!(filter, list_ty, "in");
		}
		Kind::Float => {
			let num_ty = TypeRef::named(TypeRef::FLOAT);
			filter_impl!(filter, num_ty, "gt");
			filter_impl!(filter, num_ty, "gte");
			filter_impl!(filter, num_ty, "lt");
			filter_impl!(filter, num_ty, "lte");
			let list_ty = TypeRef::named_nn_list(TypeRef::FLOAT);
			filter_impl!(filter, list_ty, "in");
		}
		Kind::Number => {
			let num_ty = TypeRef::named("number");
			filter_impl!(filter, num_ty, "gt");
			filter_impl!(filter, num_ty, "gte");
			filter_impl!(filter, num_ty, "lt");
			filter_impl!(filter, num_ty, "lte");
			let list_ty = TypeRef::named_nn_list("number");
			filter_impl!(filter, list_ty, "in");
		}
		Kind::Decimal => {
			let num_ty = TypeRef::named("decimal");
			filter_impl!(filter, num_ty, "gt");
			filter_impl!(filter, num_ty, "gte");
			filter_impl!(filter, num_ty, "lt");
			filter_impl!(filter, num_ty, "lte");
			let list_ty = TypeRef::named_nn_list("decimal");
			filter_impl!(filter, list_ty, "in");
		}
		// Datetime: gt, gte, lt, lte
		Kind::Datetime => {
			let dt_ty = TypeRef::named("datetime");
			filter_impl!(filter, dt_ty, "gt");
			filter_impl!(filter, dt_ty, "gte");
			filter_impl!(filter, dt_ty, "lt");
			filter_impl!(filter, dt_ty, "lte");
		}
		// Record: in (list of IDs)
		Kind::Record(_) => {
			let list_ty = TypeRef::named_nn_list(TypeRef::ID);
			filter_impl!(filter, list_ty, "in");
		}
		Kind::Any
		| Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Duration
		| Kind::Object
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Geometry(_)
		| Kind::Either(_)
		| Kind::Set(_, _)
		| Kind::Array(_, _)
		| Kind::Function(_, _)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_) => {}
	};
	Ok(filter)
}

pub(super) fn cond_from_filter(
	filter: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
) -> Result<Cond, GqlError> {
	val_from_filter(filter, fds).map(Cond)
}

fn val_from_filter(
	filter: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
) -> Result<Expr, GqlError> {
	if filter.is_empty() {
		return Err(resolver_error("Table filter must have at least one item"));
	}

	// If there is exactly one key, use the original dispatch logic
	if filter.len() == 1 {
		let (k, v) = filter.iter().next().expect("filter has exactly one item");

		return match k.as_str().to_lowercase().as_str() {
			"or" => aggregate(v, AggregateOp::Or, fds),
			"and" => aggregate(v, AggregateOp::And, fds),
			"not" => negate(v, fds),
			_ => binop(k.as_str(), v, fds),
		};
	}

	// Multiple fields: implicit AND across all entries.
	// Separate logical operators (and/or/not) from field conditions.
	let mut exprs = Vec::with_capacity(filter.len());

	for (k, v) in filter.iter() {
		let expr = match k.as_str().to_lowercase().as_str() {
			"or" => aggregate(v, AggregateOp::Or, fds)?,
			"and" => aggregate(v, AggregateOp::And, fds)?,
			"not" => negate(v, fds)?,
			_ => binop(k.as_str(), v, fds)?,
		};
		exprs.push(expr);
	}

	let mut iter = exprs.into_iter();
	let mut combined = iter.next().expect("at least one filter entry");
	for next_expr in iter {
		combined = Expr::Binary {
			left: Box::new(combined),
			op: BinaryOperator::And,
			right: Box::new(next_expr),
		};
	}

	Ok(combined)
}

/// Operators that map directly to SurrealDB binary operators.
fn parse_binary_op(name: &str) -> Option<expr::BinaryOperator> {
	match name {
		"eq" => Some(expr::BinaryOperator::Equal),
		"ne" => Some(expr::BinaryOperator::NotEqual),
		"gt" => Some(expr::BinaryOperator::MoreThan),
		"gte" => Some(expr::BinaryOperator::MoreThanEqual),
		"lt" => Some(expr::BinaryOperator::LessThan),
		"lte" => Some(expr::BinaryOperator::LessThanEqual),
		"in" => Some(expr::BinaryOperator::Inside),
		_ => None,
	}
}

/// Operators that map to SurrealDB function calls.
/// Returns the fully-qualified function name.
fn parse_function_op(name: &str) -> Option<&'static str> {
	match name {
		"contains" => Some("string::contains"),
		"startsWith" => Some("string::starts_with"),
		"endsWith" => Some("string::ends_with"),
		"regex" => Some("string::matches"),
		_ => None,
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

	let Some(fd) = fds.iter().find(|fd| fd.name.to_sql() == field_name) else {
		// Check if this is the `id` field (always present even if not in fds)
		if field_name == "id" {
			return binop_for_id(obj);
		}
		return Err(resolver_error(format!("Field `{field_name}` not found")));
	};

	if obj.is_empty() {
		return Err(resolver_error("Field filter must have at least one operator"));
	}

	// Support multiple operators on the same field (implicit AND)
	let field_kind = fd.field_kind.clone().unwrap_or_default();
	let mut exprs = Vec::with_capacity(obj.len());

	for (k, v) in obj.iter() {
		let op_name = k.as_str();
		let lhs = Expr::Idiom(Idiom::field(field_name.to_string()));

		if let Some(binary_op) = parse_binary_op(op_name) {
			// For `in` operator, the RHS is a list -- parse it as an array
			let rhs_kind = if op_name == "in" {
				Kind::Array(Box::new(field_kind.clone()), None)
			} else {
				field_kind.clone()
			};
			let rhs = gql_to_sql_kind(v, rhs_kind)?;
			exprs.push(Expr::Binary {
				left: Box::new(lhs),
				op: binary_op,
				right: Box::new(rhs.into_literal()),
			});
		} else if let Some(fn_name) = parse_function_op(op_name) {
			// Function-call operators: string::contains(field, value)
			let rhs = gql_to_sql_kind(v, Kind::String)?;
			exprs.push(Expr::FunctionCall(Box::new(FunctionCall {
				receiver: Function::Normal(fn_name.to_string()),
				arguments: vec![lhs, rhs.into_literal()],
			})));
		} else {
			return Err(resolver_error(format!("Unsupported filter operator: {op_name}")));
		}
	}

	// Combine multiple operators with AND
	let mut iter = exprs.into_iter();
	let mut combined = iter.next().expect("at least one operator");
	for next_expr in iter {
		combined = Expr::Binary {
			left: Box::new(combined),
			op: BinaryOperator::And,
			right: Box::new(next_expr),
		};
	}

	Ok(combined)
}

/// Handle binary operators for the `id` field which doesn't appear in field definitions.
fn binop_for_id(obj: &IndexMap<Name, GqlValue>) -> Result<Expr, GqlError> {
	if obj.is_empty() {
		return Err(resolver_error("ID filter must have at least one operator"));
	}

	let mut exprs = Vec::with_capacity(obj.len());

	for (k, v) in obj.iter() {
		let op_name = k.as_str();
		let lhs = Expr::Idiom(Idiom::field("id".to_string()));

		if let Some(binary_op) = parse_binary_op(op_name) {
			let rhs_kind = if op_name == "in" {
				Kind::Array(Box::new(Kind::Record(vec![])), None)
			} else {
				Kind::Record(vec![])
			};
			let rhs = gql_to_sql_kind(v, rhs_kind)?;
			exprs.push(Expr::Binary {
				left: Box::new(lhs),
				op: binary_op,
				right: Box::new(rhs.into_literal()),
			});
		} else {
			return Err(resolver_error(format!("Unsupported ID filter operator: {op_name}")));
		}
	}

	let mut iter = exprs.into_iter();
	let mut combined = iter.next().expect("at least one operator");
	for next_expr in iter {
		combined = Expr::Binary {
			left: Box::new(combined),
			op: BinaryOperator::And,
			right: Box::new(next_expr),
		};
	}

	Ok(combined)
}
