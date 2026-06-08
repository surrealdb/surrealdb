//! GraphQL table query generation and type construction.
//!
//! This module is responsible for generating the Query root fields and
//! Object types that correspond to each database table exposed via GraphQL.
//!
//! ## Generated Query fields
//!
//! For each table (e.g. `person`), the following Query fields are created:
//!
//! - `person(limit, start, order, filter/where, version)` -- list query returning `[person!]!`
//! - `_get_person(id, version)` -- single-record fetch returning `person`
//!
//! A generic `_get(id, version)` field is also added to fetch any record by
//! its full ID string (e.g. `"person:alice"`).
//!
//! ## Generated types
//!
//! For each table, the module generates:
//!
//! - An **Object type** with a field for each defined column, plus an `id` field and any relation
//!   fields.
//! - An **orderable enum** (`_orderable_<table>`) listing sortable fields.
//! - An **order input** (`_order_<table>`) for specifying sort criteria. Used by the list query and
//!   relation list fields; the cursor-paginated `<plural>Connection` field iterates by record id
//!   only and does not accept this input.
//! - A **filter input** (`_filter_<table>`) with per-field comparison operators.
//!
//! ## Performance: CachedRecord
//!
//! List and get queries issue `SELECT *` and wrap the full result objects in
//! [`CachedRecord`] instances.  Field resolvers then extract values directly
//! from the in-memory cache, eliminating the N+1 query problem.  Record-link
//! fields (`TYPE record<target>`) issue a single additional `SELECT *` on
//! the target and wrap it in a new `CachedRecord`.
//!
//! ## Nested objects
//!
//! Fields of `TYPE object` (or `TYPE array<object>`) that have sub-field
//! definitions (e.g. `DEFINE FIELD time.createdAt`) are detected and
//! represented as dedicated GraphQL Object types rather than the opaque
//! `object` scalar.

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

/// One filterable relation traversal exposed on a table's `_filter_<tbl>`
/// input. Captures the GraphQL field name (e.g. `sent` or `sent_in`), the
/// relation table to traverse, and the SurrealQL direction. Used by
/// [`binop`] to translate a relation `count` predicate into `count(->rel) <op> N`.
#[derive(Clone, Debug)]
pub(crate) struct RelationFieldInfo {
	pub field_name: String,
	pub relation_table: TableName,
	pub dir: Dir,
}
use super::schema::{
	SchemaContext, gql_to_sql_kind, gql_to_sql_kind_with_scope, sql_value_to_gql_value,
	sql_value_to_gql_value_with_kind,
};
use crate::catalog::providers::TableProvider;
use crate::catalog::{FieldDefinition, TableDefinition};
use crate::dbs::Session;
use crate::expr::field::{Field as SelectField, Selector};
use crate::expr::group::{Group, Groups};
use crate::expr::lookup::{Lookup, LookupKind, LookupSubject};
use crate::expr::order::{OrderList, Ordering};
use crate::expr::part::Part;
use crate::expr::statements::SelectStatement;
use crate::expr::{
	self, BinaryOperator, Cond, Dir, Expr, Fields, Function, FunctionCall, Idiom, Kind,
	KindLiteral, Limit, Literal, LogicalPlan, Start, TopLevelExpr,
};
use crate::gql::error::internal_error;
use crate::gql::schema::{
	filter_type_name, geometry_gql_type_name, kind_to_type, kind_to_type_with_enum_prefix,
	unwrap_type,
};
use crate::gql::utils::{GqlValueUtils, execute_plan};
use crate::kvs::Datastore;
use crate::val::{Array as SurArray, Datetime, Object as SurObject, RecordId, TableName, Value};

/// Upper bound on `id: { in: [...] }` list size. Each entry adds one branch to
/// the synthesised `OR` chain, so unbounded lists could expand the query
/// expression past what the SurrealQL executor handles efficiently.
const MAX_ID_IN_LIST: usize = 1000;

/// Convert a [`FieldDefinition::name`] (an `Idiom`) into a string usable as a
/// GraphQL Name.
///
/// SurrealQL's [`Idiom::to_sql`] backtick-quotes reserved-word identifiers
/// (`` `value` ``, `` `type` ``, …) and may produce dotted multi-part forms
/// for nested fields. Neither shape is a valid GraphQL Name per the spec
/// (`/[_A-Za-z][_0-9A-Za-z]*/`), and strict introspection clients such as
/// Postman reject the entire schema when even one field name violates this.
/// They also break the [`CachedRecord`] lookup (which keys on the raw field
/// name) at runtime.
///
/// This helper extracts the raw `Part::Field` segment when the idiom is
/// single-part, and falls back to a sanitised form for the (rare) multi-part
/// case. The result is suitable both as the GraphQL field name and as the
/// `CachedRecord` lookup key — they must match.
/// Return the GraphQL-facing name for a field: the explicit `GRAPHQL` alias if
/// present (validated lazily as a GraphQL identifier), otherwise the sanitised
/// form of the SurrealQL idiom via [`idiom_to_gql_name`]. The alias is only
/// honoured when it forms a valid GraphQL `Name` (`/[_A-Za-z][_0-9A-Za-z]*/`);
/// otherwise we silently fall back to the default to avoid breaking schema
/// generation. See GitHub issue #4537.
pub(crate) fn field_graphql_name(fd: &FieldDefinition) -> String {
	if let Some(ref alias) = fd.graphql_alias
		&& is_valid_gql_identifier(alias)
	{
		return alias.clone();
	}
	idiom_to_gql_name(&fd.name)
}

/// `true` when `s` matches the GraphQL `Name` production
/// (`/[_A-Za-z][_0-9A-Za-z]*/`). Used to validate user-supplied aliases.
pub(crate) fn is_valid_gql_identifier_pub(s: &str) -> bool {
	is_valid_gql_identifier(s)
}

fn is_valid_gql_identifier(s: &str) -> bool {
	let mut chars = s.chars();
	let Some(first) = chars.next() else {
		return false;
	};
	if !(first.is_ascii_alphabetic() || first == '_') {
		return false;
	}
	chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

pub(crate) fn idiom_to_gql_name(idiom: &Idiom) -> String {
	if idiom.0.len() == 1
		&& let Part::Field(name) = &idiom.0[0]
	{
		return name.as_str().to_owned();
	}
	// Multi-part fallback: stringify and sanitise. Replace any character that
	// isn't `_`/letter/digit with `_`, and prefix with `_` if the result
	// would start with a digit. This is best-effort — top-level fields
	// reaching this branch are unusual but we don't want to panic.
	let raw = idiom.to_sql();
	let mut out = String::with_capacity(raw.len());
	for (i, c) in raw.chars().enumerate() {
		let ok = if i == 0 {
			c.is_ascii_alphabetic() || c == '_'
		} else {
			c.is_ascii_alphanumeric() || c == '_'
		};
		out.push(if ok {
			c
		} else {
			'_'
		});
	}
	if out.is_empty() {
		out.push('_');
	}
	out
}

/// Create an ascending `ORDER BY` clause for the given field.
fn order_asc(field_name: String) -> expr::Order {
	expr::Order {
		value: Idiom::field(field_name),
		direction: true,
		..Default::default()
	}
}

/// Create a descending `ORDER BY` clause for the given field.
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
		Some(dt) => Expr::Literal(Literal::Datetime(*dt)),
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
// Query argument parsing helpers
// ---------------------------------------------------------------------------

/// Parse the optional `start` argument from GraphQL query arguments.
fn parse_start_arg(args: &IndexMap<Name, GqlValue>) -> Option<Start> {
	args.get("start").and_then(|v| v.as_i64()).map(|s| Start(Expr::Literal(Literal::Integer(s))))
}

/// Parse the optional `limit` argument from GraphQL query arguments.
fn parse_limit_arg(args: &IndexMap<Name, GqlValue>) -> Option<Limit> {
	args.get("limit").and_then(|v| v.as_i64()).map(|l| Limit(Expr::Literal(Literal::Integer(l))))
}

/// Parse the optional `order` argument from GraphQL query arguments.
///
/// The order argument is a linked-list structure:
/// ```graphql
/// { asc: "name", then: { desc: "age" } }
/// ```
/// Each node has exactly one of `asc` or `desc` (an enum value naming the
/// field) and an optional `then` link to the next ordering criterion.
fn parse_order_arg(
	args: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
) -> Result<Option<Ordering>, GqlError> {
	let order = args.get("order");
	// When an `order: { asc/desc: ENUM }` value is a GraphQL alias, translate
	// it back to the underlying SurrealQL field name before building the
	// `ORDER BY` clause. Aliases are GraphQL-only.
	let to_lookup = |name: &str| -> String {
		fds.iter()
			.find(|fd| field_graphql_name(fd) == name)
			.map(|fd| idiom_to_gql_name(&fd.name))
			.unwrap_or_else(|| name.to_string())
	};
	match order {
		Some(GqlValue::Object(o)) => {
			let mut orders = vec![];
			let mut current = o;
			loop {
				let asc = current.get("asc");
				let desc = current.get("desc");
				match (asc, desc) {
					(Some(_), Some(_)) => {
						return Err(resolver_error("Found both ASC and DESC in order"));
					}
					(Some(GqlValue::Enum(a)), None) => {
						orders.push(order_asc(to_lookup(a.as_str())))
					}
					(None, Some(GqlValue::Enum(d))) => {
						orders.push(order_desc(to_lookup(d.as_str())))
					}
					(_, _) => break,
				}
				if let Some(GqlValue::Object(next)) = current.get("then") {
					current = next;
				} else {
					break;
				}
			}
			Ok(Some(Ordering::Order(OrderList(orders))))
		}
		_ => Ok(None),
	}
}

/// Parse the optional `filter` / `where` argument from GraphQL query arguments.
///
/// Accepts either `filter` or `where` (aliases of each other). The value must
/// be a GraphQL input object whose shape matches the generated filter type for
/// the table.
pub(crate) fn parse_filter_arg(
	args: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
	tb_name: &str,
	relations: &[RelationFieldInfo],
) -> Result<Option<Cond>, GqlError> {
	let filter = args.get("filter").or_else(|| args.get("where"));
	match filter {
		Some(GqlValue::Object(o)) => Ok(Some(cond_from_filter(o, fds, tb_name, relations)?)),
		Some(f) => {
			error!(
				"Found filter {f}, which should be object and should have \
				 been rejected by async graphql."
			);
			Err(resolver_error("Value in cond doesn't fit schema"))
		}
		None => Ok(None),
	}
}

// ---------------------------------------------------------------------------
// SelectStatement builder helpers
// ---------------------------------------------------------------------------

/// Build a `SELECT * FROM ONLY <record_id>` statement with an optional version.
///
/// Used by `_get_`, `_get`, and record-link dereferencing to fetch a single
/// record's full data for caching.
fn select_all_from_record(rid: &RecordId, version: &Option<Datetime>) -> SelectStatement {
	SelectStatement {
		what: vec![Value::RecordId(rid.clone()).into_literal()],
		fields: Fields::all(),
		only: true,
		version: version_to_expr(version),
		timeout: Expr::Literal(Literal::None),
		omit: vec![],
		with: None,
		cond: None,
		split: None,
		group: None,
		order: None,
		limit: None,
		start: None,
		fetch: None,
		explain: None,
		tempfiles: false,
	}
}

/// Build a `SELECT VALUE <field> FROM ONLY <record_id>` statement with an
/// optional version.
///
/// Used by field resolvers and nested-object resolvers to fetch a single
/// field's value when the record data is not cached.
fn select_field_from_record(
	rid: &RecordId,
	field_name: &str,
	version: &Option<Datetime>,
) -> SelectStatement {
	SelectStatement {
		what: vec![Value::RecordId(rid.clone()).into_literal()],
		fields: Fields::Value(Box::new(Selector {
			expr: Expr::Idiom(Idiom::field(field_name.to_string())),
			alias: None,
		})),
		only: true,
		version: version_to_expr(version),
		timeout: Expr::Literal(Literal::None),
		omit: vec![],
		with: None,
		cond: None,
		split: None,
		group: None,
		order: None,
		limit: None,
		start: None,
		fetch: None,
		explain: None,
		tempfiles: false,
	}
}

/// Build a `SELECT * FROM <table>` statement with optional filtering,
/// ordering, pagination, and versioning.
///
/// Used by the table list query and relation field resolvers.
fn select_all_from_table(
	what: Expr,
	cond: Option<Cond>,
	order: Option<Ordering>,
	limit: Option<Limit>,
	start: Option<Start>,
	version: &Option<Datetime>,
) -> SelectStatement {
	SelectStatement {
		what: vec![what],
		fields: Fields::all(),
		order,
		cond,
		limit,
		start,
		version: version_to_expr(version),
		timeout: Expr::Literal(Literal::None),
		omit: vec![],
		only: false,
		with: None,
		split: None,
		group: None,
		fetch: None,
		explain: None,
		tempfiles: false,
	}
}

/// Execute a `SelectStatement` via `LogicalPlan` and return the result.
async fn execute_select(
	ds: &Datastore,
	sess: &Session,
	stmt: SelectStatement,
) -> Result<Value, GqlError> {
	let plan = LogicalPlan {
		expressions: vec![TopLevelExpr::Expr(Expr::Select(Box::new(stmt)))],
	};
	execute_plan(ds, sess, plan).await
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
			Part::Field(name) => name.as_str().to_owned(),
			_ => continue,
		};

		// Get the child field name (last part must be a Field)
		let child_name = match parts.last() {
			Some(Part::Field(name)) => name.as_str().to_owned(),
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

	// Second pass: synthesise nested-object entries from `Kind::Literal(Object)`
	// (and option/array variants). These describe the nested shape inline on the
	// parent field's type, with no matching `parent.child` DEFINE FIELD records.
	// See GitHub issue #7034.
	for fd in fds.iter() {
		if fd.name.0.len() != 1 {
			continue;
		}
		let Part::Field(name) = &fd.name.0[0] else {
			continue;
		};
		let parent_name = name.as_str().to_owned();
		if result.contains_key(&parent_name) {
			continue;
		}
		let Some(kind) = fd.field_kind.as_ref() else {
			continue;
		};

		let (literal_map, is_array, optional) = match extract_literal_object(kind) {
			Some(x) => x,
			None => continue,
		};

		let sub_fields: Vec<NestedSubField> = literal_map
			.iter()
			.map(|(k, v)| NestedSubField {
				name: k.as_str().to_owned(),
				kind: Some(v.clone()),
				comment: None,
			})
			.collect();

		if sub_fields.is_empty() {
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

/// If `kind` describes a literal-object shape — `{ … }`, `option<{ … }>`,
/// `array<{ … }>`, or `option<array<{ … }>>` — return the inner map along with
/// flags describing whether the field is array-of-object and whether it is
/// optional.
fn extract_literal_object(
	kind: &Kind,
) -> Option<(&std::collections::BTreeMap<surrealdb_strand::Strand, Kind>, bool, bool)> {
	match kind {
		Kind::Literal(KindLiteral::Object(map)) => Some((map, false, false)),
		Kind::Array(inner, _) => match inner.as_ref() {
			Kind::Literal(KindLiteral::Object(map)) => Some((map, true, false)),
			_ => None,
		},
		Kind::Either(ks) => {
			let has_none = ks.iter().any(|k| matches!(k, Kind::None | Kind::Null));
			let non_none: Vec<&Kind> =
				ks.iter().filter(|k| !matches!(k, Kind::None | Kind::Null)).collect();
			if non_none.len() != 1 {
				return None;
			}
			match non_none[0] {
				Kind::Literal(KindLiteral::Object(map)) => Some((map, false, has_none)),
				Kind::Array(inner, _) => match inner.as_ref() {
					Kind::Literal(KindLiteral::Object(map)) => Some((map, true, has_none)),
					_ => None,
				},
				_ => None,
			}
		}
		_ => None,
	}
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
		let enum_scope = format!("{type_name}_{}", sf.name);
		let fd_type = kind_to_type_with_enum_prefix(kind.clone(), types, false, Some(&enum_scope))?;
		let resolver = make_sub_field_resolver(sf.name.clone(), sf.kind.clone(), Some(enum_scope));
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
	enum_scope: Option<String>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	move |ctx: ResolverContext| {
		let field_name = field_name.clone();
		let field_kind = kind.clone();
		let enum_scope = enum_scope.clone();
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
						let gql_val = sql_value_to_gql_value_with_kind(
							v.clone(),
							field_kind.as_ref(),
							enum_scope.as_deref(),
						)
						.map_err(async_graphql::Error::from)?;
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
				Ok(vr) => (vr.rid.clone(), vr.version),
				Err(_) => {
					let rid = ctx.parent_value.try_downcast_ref::<RecordId>()?;
					(rid.clone(), None)
				}
			};

			// Build SELECT VALUE <field> FROM ONLY <record_id>
			let stmt = select_field_from_record(&rid, &fd_name, &version);
			let val = execute_select(ds, sess, stmt).await?;
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
				let out = sql_value_to_gql_value(val).map_err(async_graphql::Error::from)?;
				Ok(Some(FieldValue::value(out)))
			}
		}
	}
}

/// Derive the GraphQL filter input type name for a table (e.g. `_filter_person`).
pub(crate) fn filter_name_from_table(tb_name: impl Display) -> String {
	format!("_filter_{tb_name}")
}

// ---------------------------------------------------------------------------
// Result conversion helpers
// ---------------------------------------------------------------------------

/// Convert an array of record objects to a list of [`CachedRecord`] field values.
///
/// Each `Value::Object` in the array is wrapped in a `CachedRecord` so that
/// field resolvers can extract values directly from memory. Used by table list
/// queries, relation field resolvers, and bulk mutation results.
fn objects_to_cached_records(
	arr: SurArray,
	version: Option<Datetime>,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	let out: Result<Vec<FieldValue>, GqlError> = arr
		.0
		.into_iter()
		.map(|v| match v {
			Value::Object(obj) => {
				let rid = match obj.get("id") {
					Some(Value::RecordId(rid)) => rid.clone(),
					_ => {
						error!("Object missing 'id' field or id is not a RecordId: {obj:?}");
						return Err(internal_error("Record missing 'id' field"));
					}
				};
				Ok(FieldValue::owned_any(CachedRecord {
					rid,
					version,
					data: obj,
				}))
			}
			_ => {
				error!("Expected object in result, found: {v:?}");
				Err(internal_error("Expected object in result"))
			}
		})
		.collect();
	match out {
		Ok(l) => Ok(Some(FieldValue::list(l))),
		Err(e) => Err(e.into()),
	}
}

// ---------------------------------------------------------------------------
// Query root field builders
// ---------------------------------------------------------------------------

/// Build the query field for listing records of a table.
///
/// Creates a field like `person(limit: Int, start: Int, order: ..., filter: ...,
/// version: String): [person!]!` that returns all matching records as
/// [`CachedRecord`] instances for efficient field resolution.
fn make_table_list_field(
	tb: &TableDefinition,
	fds: Arc<[FieldDefinition]>,
	rel_filters: Arc<[RelationFieldInfo]>,
	kvs: Arc<Datastore>,
) -> Field {
	let tb_name = tb.name.clone();
	let tb_name_str = tb_name.as_str().to_string();
	let table_order_name = format!("_order_{tb_name}");
	let table_filter_name = filter_name_from_table(&tb_name);
	// Apply the naming convention (#4552) plus any explicit
	// `GRAPHQL <ident>` alias (#4537). The underlying Object type stays as
	// the source table name so cross-table `record<T>` references remain valid.
	let field_name = super::naming::list_field_name(tb);

	Field::new(field_name, TypeRef::named_nn_list_nn(&tb_name_str), move |ctx| {
		let tb_name = tb_name.clone();
		let fds = Arc::clone(&fds);
		let rel_filters = Arc::clone(&rel_filters);
		let kvs = Arc::clone(&kvs);
		FieldFuture::new(async move {
			let sess = ctx.data::<Arc<Session>>()?;
			let args = ctx.args.as_index_map();
			trace!("received request with args: {args:?}");

			let start = parse_start_arg(args);
			let limit = parse_limit_arg(args);
			let version = parse_version_arg(args)?;
			let order = parse_order_arg(args, &fds)?;
			let tb_name_str_ref = tb_name.as_str();
			let cond = parse_filter_arg(args, &fds, tb_name_str_ref, &rel_filters)?;

			trace!("parsed order: {order:?}");
			trace!("parsed filter: {cond:?}");

			let stmt =
				select_all_from_table(Expr::Table(tb_name), cond, order, limit, start, &version);
			let res = execute_select(&kvs, sess, stmt).await?;

			match res {
				Value::Array(a) => objects_to_cached_records(a, version),
				v => {
					error!("Found top level value, in result which should be array: {v:?}");
					Err(internal_error("Unexpected result type from table query").into())
				}
			}
		})
	})
	.description(
		super::naming::description_with_deprecation(
			tb.comment.as_deref(),
			tb.graphql_deprecated.as_deref(),
		)
		.unwrap_or_else(|| {
			format!("Generated from table `{}`\nallows querying a table with filters", tb.name)
		}),
	)
	.argument(InputValue::new("limit", TypeRef::named(TypeRef::INT)))
	.argument(InputValue::new("start", TypeRef::named(TypeRef::INT)))
	.argument(InputValue::new("order", TypeRef::named(&table_order_name)))
	.argument(InputValue::new("filter", TypeRef::named(&table_filter_name)))
	.argument(InputValue::new("where", TypeRef::named(&table_filter_name)))
	.argument(InputValue::new("version", TypeRef::named(TypeRef::STRING)))
}

/// Build the `_get_<table>` query field for fetching a single record by ID.
///
/// Returns the record as a [`CachedRecord`] for efficient field resolution,
/// or `null` if the record does not exist.
fn make_table_get_field(tb: &TableDefinition, kvs: Arc<Datastore>) -> Field {
	let tb_name = tb.name.clone();
	let tb_name_str = tb_name.as_str().to_string();

	let field_name = super::naming::get_field_name(tb);
	Field::new(field_name, TypeRef::named(&tb_name_str), move |ctx| {
		let tb_name = tb_name.clone();
		let kvs = Arc::clone(&kvs);
		FieldFuture::new(async move {
			let sess = ctx.data::<Arc<Session>>()?;
			let args = ctx.args.as_index_map();
			let id = match args.get("id").and_then(GqlValueUtils::as_string) {
				Some(i) => i,
				None => {
					return Err(
						internal_error("Schema validation failed: No id found in _get_").into()
					);
				}
			};
			let version = parse_version_arg(args)?;

			let rid_str = format!("{tb_name}:{id}");
			let record_id: RecordId = match crate::syn::record_id(&rid_str) {
				Ok(x) => x.into(),
				Err(_) => RecordId::new(tb_name, id),
			};

			let stmt = select_all_from_record(&record_id, &version);
			let res = execute_select(&kvs, sess, stmt).await?;

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
		})
	})
	.description(
		super::naming::description_with_deprecation(
			tb.comment.as_deref(),
			tb.graphql_deprecated.as_deref(),
		)
		.unwrap_or_else(|| {
			format!(
				"Generated from table `{}`\nallows querying a single record in a table by ID",
				tb.name
			)
		}),
	)
	.argument(InputValue::new("id", TypeRef::named_nn(TypeRef::ID)))
	.argument(InputValue::new("version", TypeRef::named(TypeRef::STRING)))
}

/// Build the generic `_get` query field for fetching any record by full ID.
///
/// Unlike `_get_<table>`, this accepts a full record ID (e.g. `"person:alice"`)
/// and returns the `record` interface type, requiring `.with_type()` to
/// indicate the concrete table type.
fn make_generic_get_field(kvs: Arc<Datastore>) -> Field {
	Field::new("_get", TypeRef::named("record"), move |ctx| {
		let kvs = Arc::clone(&kvs);
		FieldFuture::new(async move {
			let sess = ctx.data::<Arc<Session>>()?;
			let args = ctx.args.as_index_map();
			let id = match args.get("id").and_then(GqlValueUtils::as_string) {
				Some(i) => i,
				None => {
					return Err(
						internal_error("Schema validation failed: No id found in _get").into()
					);
				}
			};
			let version = parse_version_arg(args)?;

			let record_id: RecordId = match crate::syn::record_id(&id) {
				Ok(x) => x.into(),
				Err(_) => {
					return Err(resolver_error("Invalid record ID format").into());
				}
			};

			let stmt = select_all_from_record(&record_id, &version);
			let res = execute_select(&kvs, sess, stmt).await?;

			match res {
				Value::Object(obj) => {
					let rid = match obj.get("id") {
						Some(Value::RecordId(rid)) => rid.clone(),
						_ => return Ok(None),
					};
					let table_name = rid.table.clone();
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
		})
	})
	.description("Allows fetching arbitrary records".to_string())
	.argument(InputValue::new("id", TypeRef::named_nn(TypeRef::ID)))
	.argument(InputValue::new("version", TypeRef::named(TypeRef::STRING)))
}

// ---------------------------------------------------------------------------
// Table type system builders
// ---------------------------------------------------------------------------

/// The GraphQL types generated for a single table.
///
/// Returned by [`build_table_type`] for registration on the schema.
struct TableGraphQLTypes {
	/// The table's Object type (e.g., `person`).
	ty_obj: Object,
	/// Enum of fields that can be ordered by (e.g., `_orderable_person`).
	orderable: Enum,
	/// The order input object (e.g., `_order_person`).
	order: InputObject,
	/// The filter input object (e.g., `_filter_person`).
	filter: InputObject,
	/// Relation-traversal filters exposed on this table's `_filter_*`
	/// input (one per direction). Plumbed into the runtime filter parser so
	/// `count(->rel) <op> N` predicates resolve. See GitHub issue #4554.
	rel_filters: Vec<RelationFieldInfo>,
}

/// Build all GraphQL types for a single table: the Object type, orderable enum,
/// order input, and filter input.
///
/// This processes all field definitions to create typed fields, filter types,
/// and orderable items, then attaches relation fields for any relations that
/// connect to this table.
fn build_table_type(
	tb: &TableDefinition,
	fds: &[FieldDefinition],
	relations: &[RelationInfo],
	exposed_table_names: &HashSet<TableName>,
	relation_table_fds: &HashMap<TableName, Arc<[FieldDefinition]>>,
	types: &mut Vec<Type>,
) -> Result<TableGraphQLTypes, GqlError> {
	let tb_name = &tb.name;
	let tb_name_str = tb_name.as_str().to_string();

	// --- Create initial types ---

	let table_orderable_name = format!("_orderable_{tb_name}");
	let table_order_name = format!("_order_{tb_name}");
	let table_filter_name = filter_name_from_table(tb_name);

	let mut orderable = Enum::new(&table_orderable_name).item("id").description(format!(
		"Generated from `{tb_name}` the fields which a query can be ordered by"
	));

	let order = InputObject::new(&table_order_name)
		.description(format!("Generated from `{tb_name}` an object representing a query ordering"))
		.field(InputValue::new("asc", TypeRef::named(&table_orderable_name)))
		.field(InputValue::new("desc", TypeRef::named(&table_orderable_name)))
		.field(InputValue::new("then", TypeRef::named(&table_order_name)));

	let mut filter = InputObject::new(&table_filter_name)
		.field(InputValue::new("id", TypeRef::named("_filter_id")))
		.field(InputValue::new("and", TypeRef::named_nn_list(&table_filter_name)))
		.field(InputValue::new("or", TypeRef::named_nn_list(&table_filter_name)))
		.field(InputValue::new("not", TypeRef::named(&table_filter_name)));

	// `_filter_id` is registered once globally in `register_filter_helper_types`,
	// not per-table — it's the same shape for every table.

	let mut ty_obj = Object::new(&tb_name_str)
		.field(Field::new(
			"id",
			TypeRef::named_nn(TypeRef::ID),
			make_table_field_resolver("id", Some(Kind::Record(vec![tb_name.clone()])), None),
		))
		.implement("record");

	let mut existing_field_names: HashSet<String> = HashSet::new();
	existing_field_names.insert("id".to_string());

	// --- Process field definitions ---

	let nested_objects = detect_nested_objects(&tb_name_str, fds);

	for fd in fds.iter() {
		let Some(ref kind) = fd.field_kind else {
			continue;
		};
		if fd.name.is_id() {
			continue;
		}
		if fd.name.0.len() > 1 {
			continue;
		}

		// `lookup_name` is the key used to extract values from the cached
		// `SurObject` (the SurrealQL field name, GraphQL-safe-encoded).
		// `fd_name` is the GraphQL-facing name — overridden by an explicit
		// `GRAPHQL <ident>` alias (#4537) or the active naming convention
		// (#4552).
		let lookup_name = idiom_to_gql_name(&fd.name);
		let fd_name = Name::new(super::naming::field_gql_name(fd));
		existing_field_names.insert(fd_name.to_string());

		// Nested-object detection still keys on the lookup name (matches the
		// storage layout). The generated nested type name is derived from the
		// alias, so two columns aliased the same way would conflict — that's
		// caught at schema build time by async-graphql.
		if let Some(nested) = nested_objects.get(lookup_name.as_str()) {
			let nested_type =
				make_nested_object_type(&nested.gql_type_name, &nested.sub_fields, types)?;
			types.push(Type::Object(nested_type));

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

			orderable = orderable.item(fd_name.to_string());
			let mut field = Field::new(
				fd_name.as_str(),
				fd_type,
				make_nested_object_field_resolver(lookup_name.clone(), nested.is_array),
			);
			field = field.description(if let Some(ref c) = fd.comment {
				c.clone()
			} else {
				format!("Nested object field `{}`", fd_name.as_str())
			});
			ty_obj = ty_obj.field(field);
			continue;
		}

		// Handle regular fields
		let enum_scope = format!("{}_{}", tb_name_str, fd_name);
		let fd_type = kind_to_type_with_enum_prefix(kind.clone(), types, false, Some(&enum_scope))?;
		orderable = orderable.item(fd_name.to_string());

		let type_filter_name = format!("_filter_{}", filter_type_name(&fd_type));
		let filter_already_exists = types.iter().any(|t| match t {
			Type::InputObject(io) => io.type_name() == type_filter_name,
			_ => false,
		});
		if !filter_already_exists {
			let type_filter = Type::InputObject(filter_from_type(
				kind,
				type_filter_name.clone(),
				types,
				Some(&enum_scope),
			)?);
			trace!("\n{type_filter:?}\n");
			types.push(type_filter);
		}

		filter = filter.field(InputValue::new(fd_name.as_str(), TypeRef::named(type_filter_name)));
		let mut field = Field::new(
			fd_name.as_str(),
			fd_type,
			make_table_field_resolver(&lookup_name, fd.field_kind.clone(), Some(enum_scope)),
		);
		if let Some(desc) = super::naming::description_with_deprecation(
			fd.comment.as_deref(),
			fd.graphql_deprecated.as_deref(),
		) {
			field = field.description(desc);
		}
		ty_obj = ty_obj.field(field);
	}

	// --- Add relation fields ---

	let mut rel_filters: Vec<RelationFieldInfo> = Vec::new();

	for rel in relations.iter() {
		if !exposed_table_names.contains(&rel.table_name) {
			continue;
		}
		// Only allocate the `String` form once we know we'll use
		// it for the GraphQL field/type names below.
		let rel_table_str = rel.table_name.as_str().to_owned();

		let rel_fds = relation_table_fds.get(&rel.table_name).cloned();

		// Outgoing: this table is in the FROM list
		if rel.from_tables.iter().any(|n| n.as_str() == tb_name_str.as_str()) {
			let field_name = rel_table_str.clone();
			if !existing_field_names.contains(&field_name) {
				existing_field_names.insert(field_name.clone());
				ty_obj = ty_obj.field(make_relation_field(
					&field_name,
					&rel_table_str,
					rel.table_name.clone(),
					RelationDirection::Outgoing,
					rel_fds.clone(),
				));
				let rel_filter_name =
					register_relation_filter(&tb_name_str, &field_name, "out", types);
				filter = filter
					.field(InputValue::new(field_name.clone(), TypeRef::named(rel_filter_name)));
				rel_filters.push(RelationFieldInfo {
					field_name,
					relation_table: rel.table_name.clone(),
					dir: Dir::Out,
				});
			} else {
				trace!(
					"Skipping outgoing relation field '{}' on table '{}': \
					 conflicts with existing field",
					field_name, tb_name_str
				);
			}
		}

		// Incoming: this table is in the TO list
		if rel.to_tables.iter().any(|n| n.as_str() == tb_name_str.as_str()) {
			let field_name = format!("{}_in", rel_table_str);
			if !existing_field_names.contains(&field_name) {
				existing_field_names.insert(field_name.clone());
				ty_obj = ty_obj.field(make_relation_field(
					&field_name,
					&rel_table_str,
					rel.table_name.clone(),
					RelationDirection::Incoming,
					rel_fds.clone(),
				));
				let rel_filter_name =
					register_relation_filter(&tb_name_str, &field_name, "in", types);
				filter = filter
					.field(InputValue::new(field_name.clone(), TypeRef::named(rel_filter_name)));
				rel_filters.push(RelationFieldInfo {
					field_name,
					relation_table: rel.table_name.clone(),
					dir: Dir::In,
				});
			} else {
				trace!(
					"Skipping incoming relation field '{}' on table '{}': \
					 conflicts with existing field",
					field_name, tb_name_str
				);
			}
		}
	}

	Ok(TableGraphQLTypes {
		ty_obj,
		orderable,
		order,
		filter,
		rel_filters,
	})
}

/// Register the per-relation filter input object `_relation_filter_<tbl>_<field>_<dir>`.
fn register_relation_filter(
	tb_name: &str,
	field_name: &str,
	dir_token: &str,
	types: &mut Vec<Type>,
) -> String {
	let name = format!("_relation_filter_{tb_name}_{field_name}_{dir_token}");
	// Guard against duplicate registrations — async-graphql errors out on type
	// redefinition.
	if types.iter().any(|t| match t {
		Type::InputObject(io) => io.type_name() == name,
		_ => false,
	}) {
		return name;
	}
	let io = InputObject::new(&name)
		.description(format!(
			"Filter predicates evaluated against the `{field_name}` relation traversal of `{tb_name}`. \
		 Only `count` is supported."
		))
		.field(InputValue::new("count", TypeRef::named(COUNT_FILTER_INPUT)));
	types.push(Type::InputObject(io));
	name
}

// ---------------------------------------------------------------------------
// Top-level table processing
// ---------------------------------------------------------------------------

pub async fn process_tbs(
	tbs: Arc<[TableDefinition]>,
	mut query: Object,
	types: &mut Vec<Type>,
	ctx: &SchemaContext<'_>,
	relations: &[RelationInfo],
	table_fields: &mut HashMap<TableName, Arc<[FieldDefinition]>>,
) -> Result<Object, GqlError> {
	// Pre-fetch field definitions for relation tables (needed for filter support
	// in relation field resolvers). These are captured by the resolver closures.
	// Keyed by `TableName` (Strand wrapper) so insert/lookup avoid the
	// per-key `String` allocation the previous `HashMap<String, _>`
	// form forced on every hit.
	let mut relation_table_fds: HashMap<TableName, Arc<[FieldDefinition]>> = HashMap::new();
	for rel in relations.iter() {
		if let std::collections::hash_map::Entry::Vacant(e) =
			relation_table_fds.entry(rel.table_name.clone())
		{
			let fds = ctx.tx.all_tb_fields(ctx.ns, ctx.db, &rel.table_name, None).await?;
			e.insert(fds);
		}
	}

	// Set of exposed table names for checking that relation targets are
	// visible. `TableName::clone` is a cheap `Strand` clone (inline
	// copy or `Arc` refcount bump), so building the set is free vs.
	// the previous `.into_string()` path that allocated a `String`
	// per table.
	let exposed_table_names: HashSet<TableName> = tbs.iter().map(|t| t.name.clone()).collect();

	// Collision check: the (always-Apollo) naming convention plus any
	// explicit `GRAPHQL <ident>` aliases (#4537) must produce unique Query
	// field names for both the list and single-fetch queries. Otherwise
	// async-graphql rejects the whole schema with an opaque error at build
	// time.
	{
		// Sentinel used when the prior owner of a name is a built-in type or
		// reserved query field, so the error message reads naturally.
		const BUILTIN: &str = "<built-in>";
		let mut seen: HashMap<String, String> = HashMap::new();
		let mut seen_types: HashMap<String, String> = HashMap::new();
		// Reserved query field names — introspection plus any helper queries
		// added in this module. Aliasing a table to one of these would shadow
		// the introspection surface.
		for reserved in ["__schema", "__type", "__typename"] {
			seen.insert(reserved.to_owned(), BUILTIN.to_owned());
		}
		// Reserved GraphQL Object / InputObject / Enum names registered
		// unconditionally by this module. Aliasing a table to one of these
		// produces an opaque async-graphql error instead of our helpful one.
		for reserved in [
			"Query",
			"Mutation",
			"Subscription",
			PAGE_INFO_TYPE,
			ID_RANGE_INPUT,
			COUNT_FILTER_INPUT,
			VECTOR_DISTANCE_ENUM,
			NUM_OP_ENUM,
			KNN_INPUT,
			SIMILARITY_INPUT,
			MATCHES_INPUT,
			CALL_INPUT,
			"_filter_id",
		] {
			seen_types.insert(reserved.to_owned(), BUILTIN.to_owned());
		}
		for tb in tbs.iter() {
			let list = super::naming::list_field_name(tb);
			let get = super::naming::get_field_name(tb);
			let conn_field = format!("{list}Connection");
			let aggregate = format!("{}_aggregate", tb.name.as_str());
			for name in [list, get, conn_field, aggregate] {
				if let Some(prior) = seen.get(&name) {
					let prior_desc = if prior == BUILTIN {
						format!("built-in query field `{name}`")
					} else {
						format!("table `{prior}`")
					};
					return Err(super::error::schema_error(format!(
						"GraphQL naming collision on `{}` — {} and table `{}` produce the \
						 same query field. Set an explicit `GRAPHQL_ALIAS` on the table.",
						name, prior_desc, tb.name
					)));
				}
				seen.insert(name, tb.name.as_str().to_owned());
			}
			// Connection / Edge type-name collisions surface as opaque
			// `async-graphql` schema errors otherwise. `GRAPHQL_ALIAS "Foo"` on
			// two tables both produces `FooConnection` / `FooEdge`. The
			// table's own Object type also collides if aliased to a built-in
			// like `PageInfo`.
			// The table's own Object type uses the raw table name (see
			// `Object::new(&tb_name_str)` in `build_table_type`), so the
			// collision surface for the Object name is the raw `tb.name`.
			let tb_ty = tb.name.as_str().to_owned();
			let conn_ty = connection_type_name(tb);
			let edge_ty = edge_type_name(tb);
			for ty_name in [tb_ty, conn_ty, edge_ty] {
				if let Some(prior) = seen_types.get(&ty_name) {
					let prior_desc = if prior == BUILTIN {
						format!("built-in type `{ty_name}`")
					} else {
						format!("table `{prior}`")
					};
					return Err(super::error::schema_error(format!(
						"GraphQL naming collision on type `{}` — {} and table `{}` produce \
						 the same type. Set an explicit `GRAPHQL_ALIAS` on the table.",
						ty_name, prior_desc, tb.name
					)));
				}
				seen_types.insert(ty_name, tb.name.as_str().to_owned());
			}
		}
	}

	for tb in tbs.iter() {
		trace!("Adding table: {}", tb.name);
		let fds = ctx.tx.all_tb_fields(ctx.ns, ctx.db, &tb.name, None).await?;
		table_fields.insert(tb.name.clone(), Arc::clone(&fds));

		// Build and register the table's type system. We need the relation
		// filter list before constructing the runtime list/aggregate fields
		// because they capture it.
		let tt = build_table_type(
			tb,
			&fds,
			relations,
			&exposed_table_names,
			&relation_table_fds,
			types,
		)?;
		let rel_filters: Arc<[RelationFieldInfo]> = tt.rel_filters.into();
		types.push(Type::Object(tt.ty_obj));
		types.push(tt.order.into());
		types.push(Type::Enum(tt.orderable));
		types.push(Type::InputObject(tt.filter));

		// Add query root fields for this table
		query = query.field(make_table_list_field(
			tb,
			Arc::clone(&fds),
			Arc::clone(&rel_filters),
			Arc::clone(ctx.datastore),
		));
		query = query.field(make_table_get_field(tb, Arc::clone(ctx.datastore)));

		// Aggregation query field — `<table_plural>_aggregate` returns
		// `[{Table}AggregateRow!]!` with count + per-numeric-field stats and
		// optional groupBy.  See `register_filter_helper_types` / Stage E.
		let (agg_obj, agg_enum) = build_aggregate_type(tb.name.as_str(), &fds, types);
		types.push(Type::Object(agg_obj));
		types.push(Type::Enum(agg_enum));
		query = query.field(make_table_aggregate_field(
			tb,
			Arc::clone(&fds),
			Arc::clone(&rel_filters),
			Arc::clone(ctx.datastore),
		));

		// Cursor-paginated connection query — `<plural>Connection(first, after)`.
		build_connection_types(tb, types);
		query = query.field(make_table_connection_field(
			tb,
			Arc::clone(&fds),
			Arc::clone(&rel_filters),
			Arc::clone(ctx.datastore),
		));
	}

	// Add generic _get query field for fetching any record by full ID
	query = query.field(make_generic_get_field(Arc::clone(ctx.datastore)));

	Ok(query)
}

/// Create a field resolver for a column on a table Object type.
///
/// The resolver has two execution paths:
///
/// 1. **Fast path** -- if the parent value is a [`CachedRecord`] (the common case for list queries,
///    `_get_` fetches, and mutations), the field value is extracted directly from the in-memory
///    record data.
/// 2. **Slow path** -- if the parent is a [`VersionedRecord`] or plain `RecordId` (e.g. from a
///    custom function return), the resolver issues a `SELECT VALUE <field> FROM ONLY <record_id>`
///    query.
///
/// Record-link fields (`TYPE record<target>`) are dereferenced: the resolver
/// fetches the target record's full data and wraps it in a new `CachedRecord`
/// so the target's own field resolvers also benefit from caching.
fn make_table_field_resolver(
	fd_name: impl Into<String>,
	kind: Option<Kind>,
	enum_scope: Option<String>,
) -> impl for<'a> Fn(ResolverContext<'a>) -> FieldFuture<'a> + Send + Sync + 'static {
	let fd_name = fd_name.into();
	move |ctx: ResolverContext| {
		let fd_name = fd_name.clone();
		let field_kind = kind.clone();
		let enum_scope = enum_scope.clone();
		FieldFuture::new({
			async move {
				// ── Fast path: extract field from CachedRecord ──
				//
				// When the parent is a CachedRecord (from a list query, _get_,
				// relation, or mutation), the full record data is already in
				// memory. Extract the requested field directly instead of
				// issuing a separate database query.
				if let Ok(cached) = ctx.parent_value.try_downcast_ref::<CachedRecord>() {
					return resolve_field_from_cached_record(
						&ctx,
						cached,
						&fd_name,
						&field_kind,
						enum_scope.as_deref(),
					)
					.await;
				}

				// ── Slow path: fetch field via database query ──
				//
				// Fallback for VersionedRecord (no cached data) or plain
				// RecordId (from custom functions, etc.).
				let ds = ctx.data::<Arc<Datastore>>()?;
				let sess = ctx.data::<Arc<Session>>()?;

				let (rid, version) = match ctx.parent_value.try_downcast_ref::<VersionedRecord>() {
					Ok(vr) => (vr.rid.clone(), vr.version),
					Err(_) => {
						let rid = ctx.parent_value.try_downcast_ref::<RecordId>()?;
						(rid.clone(), None)
					}
				};

				// Build SELECT VALUE <field> FROM ONLY <record_id>
				let stmt = select_field_from_record(&rid, &fd_name, &version);
				let val = execute_select(ds, sess, stmt).await?;
				resolve_field_value(
					&ctx,
					val,
					&fd_name,
					&field_kind,
					&version,
					enum_scope.as_deref(),
				)
				.await
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
	enum_scope: Option<&str>,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	match val {
		Value::RecordId(target_rid) if fd_name != "id" => {
			// Record-link dereferencing: fetch the full target record and
			// wrap it as CachedRecord so the target's field resolvers can
			// also benefit from caching.
			let ds = ctx.data::<Arc<Datastore>>()?;
			let sess = ctx.data::<Arc<Session>>()?;

			let stmt = select_all_from_record(&target_rid, version);
			let target_val = execute_select(ds, sess, stmt).await?;

			match target_val {
				Value::Object(obj) => {
					let field_val = FieldValue::owned_any(CachedRecord {
						rid: target_rid.clone(),
						version: *version,
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
			let out = sql_value_to_gql_value_with_kind(v, field_kind.as_ref(), enum_scope)
				.map_err(async_graphql::Error::from)?;
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
	enum_scope: Option<&str>,
) -> Result<Option<FieldValue<'static>>, async_graphql::Error> {
	let val = cached.data.get(fd_name).cloned().unwrap_or(Value::None);
	resolve_field_value(ctx, val, fd_name, field_kind, &cached.version, enum_scope).await
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
					(cached.rid.clone(), cached.version)
				} else if let Ok(vr) = ctx.parent_value.try_downcast_ref::<VersionedRecord>() {
					(vr.rid.clone(), vr.version)
				} else {
					let rid = ctx.parent_value.try_downcast_ref::<RecordId>()?;
					(rid.clone(), None)
				};
			let args = ctx.args.as_index_map();

			let start = parse_start_arg(args);
			let limit = parse_limit_arg(args);
			let order = parse_order_arg(args, fds.as_deref().unwrap_or(&[]))?;

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

			// Parse and combine user-supplied filter
			if let Some(ref fds) = fds
				&& let Some(user_cond) = parse_filter_arg(args, fds, relation_table.as_str(), &[])?
			{
				base_cond = Expr::Binary {
					left: Box::new(base_cond),
					op: BinaryOperator::And,
					right: Box::new(user_cond.0),
				};
			}

			let cond = Some(Cond(base_cond));

			// Build SELECT * FROM <relation_table> WHERE ...
			// Propagate version from parent for consistent temporal queries
			let stmt = select_all_from_table(
				Expr::Table(relation_table),
				cond,
				order,
				limit,
				start,
				&version,
			);

			let res = execute_select(ds, sess, stmt).await?;

			match res {
				Value::Array(a) => objects_to_cached_records(a, version),
				v => {
					error!("Expected array result for relation query, found: {v:?}");
					Err(internal_error("Unexpected result type for relation query").into())
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
	filter_impl!(filter, ty, "gt");
	filter_impl!(filter, ty, "gte");
	filter_impl!(filter, ty, "lt");
	filter_impl!(filter, ty, "lte");
	// `in` accepts a list of IDs
	let list_ty = TypeRef::named_nn_list(TypeRef::ID);
	filter_impl!(filter, list_ty, "in");
	// `range` accepts a `{ from, to, inclusive }` input — see GitHub issue #4555.
	let range_ty = TypeRef::named(ID_RANGE_INPUT);
	filter_impl!(filter, range_ty, "range");
	filter
}

/// Generate a filter InputObject for a field's type.
///
/// All types get `eq` and `ne` operators.  Additional operators are added
/// based on the kind:
/// - **String** -- `contains`, `startsWith`, `endsWith`, `regex`, `in`
/// - **Numeric** (Int, Float, Number, Decimal) -- `gt`, `gte`, `lt`, `lte`, `in`
/// - **Datetime** -- `gt`, `gte`, `lt`, `lte`
/// - **Record** -- `in` (list of IDs)
///
/// `option<record<T>>` is normalised to the inner record kind so filters
/// use the target table's filter type rather than a plain ID filter.
fn filter_from_type(
	kind: &Kind,
	filter_name: String,
	types: &mut Vec<Type>,
	enum_scope: Option<&str>,
) -> Result<InputObject, GqlError> {
	// Normalise `option<record<T>>` (Kind::Either([None, Record([T])])) down to the
	// inner record kind so filters are generated correctly with ID-based filtering.
	let effective_kind = match kind {
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

	// Determine the input type used for eq/ne. For record-typed fields we use
	// ID-based filtering, never the record's output type (which is an Object
	// type and not valid as an input). For arrays of records we drop eq/ne
	// entirely — comparing full record arrays by content rarely makes sense and
	// would otherwise pull an output Object type into input position.
	let (eq_ne_ty, supports_eq_ne) = match &effective_kind {
		Kind::Record(ts) => match ts.len() {
			1 => (
				TypeRef::named(filter_name_from_table(
					ts.first().expect("ts should have exactly one element").as_str(),
				)),
				true,
			),
			_ => (TypeRef::named(TypeRef::ID), true),
		},
		// `array<record<T>>` — leave eq/ne off; expose `contains` further below.
		Kind::Array(inner, _) if matches!(**inner, Kind::Record(_)) => {
			(TypeRef::named(TypeRef::ID), false)
		}
		k => {
			(unwrap_type(kind_to_type_with_enum_prefix(k.clone(), types, true, enum_scope)?), true)
		}
	};

	let mut filter = InputObject::new(filter_name);
	if supports_eq_ne {
		filter_impl!(filter, eq_ne_ty, "eq");
		filter_impl!(filter, eq_ne_ty, "ne");
	}

	// Every field gets a generic `call` predicate (function-based filter).
	let call_ty = TypeRef::named(CALL_INPUT);
	filter_impl!(filter, call_ty, "call");

	// Numeric-array fields get `nearest` (KNN) and `similarity` predicates,
	// regardless of the outer field kind being plain `array<T>` or
	// `option<array<T>>`.
	if numeric_array_inner(&effective_kind).is_some() {
		let knn_ty = TypeRef::named(KNN_INPUT);
		filter_impl!(filter, knn_ty, "nearest");
		let sim_ty = TypeRef::named(SIMILARITY_INPUT);
		filter_impl!(filter, sim_ty, "similarity");
	}

	match effective_kind {
		// String: contains, startsWith, endsWith, regex, in, matches
		Kind::String => {
			let str_ty = TypeRef::named(TypeRef::STRING);
			filter_impl!(filter, str_ty, "contains");
			filter_impl!(filter, str_ty, "startsWith");
			filter_impl!(filter, str_ty, "endsWith");
			filter_impl!(filter, str_ty, "regex");
			let list_ty = TypeRef::named_nn_list(TypeRef::STRING);
			filter_impl!(filter, list_ty, "in");
			let matches_ty = TypeRef::named(MATCHES_INPUT);
			filter_impl!(filter, matches_ty, "matches");
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
		// Array of records: `contains: ID` lets callers ask whether the array
		// holds a specific record link. Per-element field filtering is out of
		// scope here.
		Kind::Array(ref inner, _) if matches!(**inner, Kind::Record(_)) => {
			let id_ty = TypeRef::named(TypeRef::ID);
			filter_impl!(filter, id_ty, "contains");
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

/// Convert a GraphQL filter input object into a SurrealQL `WHERE` condition.
///
/// The filter object may contain field-level comparison operators (`eq`, `gt`,
/// etc.), logical combinators (`and`, `or`, `not`), or a mix of both.
/// Multiple top-level keys are combined with implicit AND.
pub(super) fn cond_from_filter(
	filter: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
	tb_name: &str,
	relations: &[RelationFieldInfo],
) -> Result<Cond, GqlError> {
	val_from_filter(filter, fds, tb_name, relations).map(Cond)
}

/// Recursive filter-to-expression converter.
///
/// Single-key filters dispatch directly to the appropriate handler (field
/// comparison, AND/OR aggregation, or NOT negation).  Multi-key filters are
/// treated as implicit AND across all entries.
fn val_from_filter(
	filter: &IndexMap<Name, GqlValue>,
	fds: &[FieldDefinition],
	tb_name: &str,
	relations: &[RelationFieldInfo],
) -> Result<Expr, GqlError> {
	if filter.is_empty() {
		return Err(resolver_error("Table filter must have at least one item"));
	}

	// If there is exactly one key, use the original dispatch logic
	if filter.len() == 1 {
		let (k, v) = filter.iter().next().expect("filter has exactly one item");

		return match k.as_str().to_lowercase().as_str() {
			"or" => aggregate(v, AggregateOp::Or, fds, tb_name, relations),
			"and" => aggregate(v, AggregateOp::And, fds, tb_name, relations),
			"not" => negate(v, fds, tb_name, relations),
			_ => binop(k.as_str(), v, fds, tb_name, relations),
		};
	}

	// Multiple fields: implicit AND across all entries.
	// Separate logical operators (and/or/not) from field conditions.
	let mut exprs = Vec::with_capacity(filter.len());

	for (k, v) in filter.iter() {
		let expr = match k.as_str().to_lowercase().as_str() {
			"or" => aggregate(v, AggregateOp::Or, fds, tb_name, relations)?,
			"and" => aggregate(v, AggregateOp::And, fds, tb_name, relations)?,
			"not" => negate(v, fds, tb_name, relations)?,
			_ => binop(k.as_str(), v, fds, tb_name, relations)?,
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

fn negate(
	filter: &GqlValue,
	fds: &[FieldDefinition],
	tb_name: &str,
	relations: &[RelationFieldInfo],
) -> Result<Expr, GqlError> {
	let obj = filter.as_object().ok_or(resolver_error("Value of NOT must be object"))?;
	let inner_cond = val_from_filter(obj, fds, tb_name, relations)?;

	Ok(Expr::Prefix {
		op: expr::PrefixOperator::Not,
		expr: Box::new(inner_cond),
	})
}

#[derive(Clone, Copy)]
enum AggregateOp {
	And,
	Or,
}

fn aggregate(
	filter: &GqlValue,
	op: AggregateOp,
	fds: &[FieldDefinition],
	tb_name: &str,
	relations: &[RelationFieldInfo],
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
		.map(|v| v.as_object().map(|o| val_from_filter(o, fds, tb_name, relations)))
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

/// Convert a single field's filter object to a SurrealQL expression.
///
/// The filter object maps operator names (`eq`, `gt`, `contains`, etc.) to
/// values.  Binary operators produce `field <op> value` expressions; function
/// operators produce `fn(field, value)` calls.  Multiple operators on the
/// same field are combined with AND.
fn binop(
	field_name: &str,
	val: &GqlValue,
	fds: &[FieldDefinition],
	tb_name: &str,
	relations: &[RelationFieldInfo],
) -> Result<Expr, GqlError> {
	let obj = val.as_object().ok_or(resolver_error("Field filter should be object"))?;

	// Match by GraphQL alias first (when a `GRAPHQL <ident>` clause is set on
	// the field), otherwise by the sanitised idiom name.
	let Some(fd) = fds.iter().find(|fd| field_graphql_name(fd) == field_name) else {
		if field_name == "id" {
			return binop_for_id(obj);
		}
		if let Some(rel) = relations.iter().find(|r| r.field_name == field_name) {
			return binop_for_relation(rel, obj);
		}
		return Err(resolver_error(format!("Field `{field_name}` not found")));
	};

	if obj.is_empty() {
		return Err(resolver_error("Field filter must have at least one operator"));
	}

	// The SurrealQL WHERE clause must use the original storage field name —
	// the alias only exists in the GraphQL surface.
	let lookup_name = idiom_to_gql_name(&fd.name);
	// Fields without an explicit type accept any kind for filtering.
	let field_kind = fd.field_kind.clone().unwrap_or(Kind::Any);
	let enum_scope = format!("{tb_name}_{field_name}");
	let mut exprs = Vec::with_capacity(obj.len());

	for (k, v) in obj.iter() {
		let op_name = k.as_str();
		let lhs = Expr::Idiom(Idiom::field(lookup_name.clone()));

		if let Some(binary_op) = parse_binary_op(op_name) {
			let rhs_kind = if op_name == "in" {
				Kind::Array(Box::new(field_kind.clone()), None)
			} else {
				field_kind.clone()
			};
			let rhs = gql_to_sql_kind_with_scope(v, rhs_kind, Some(&enum_scope))?;
			exprs.push(Expr::Binary {
				left: Box::new(lhs),
				op: binary_op,
				right: Box::new(rhs.into_literal()),
			});
		} else if op_name == "contains"
			&& matches!(strip_option(&field_kind), Kind::Array(inner, _) if matches!(*inner, Kind::Record(_)))
		{
			// `contains` on an `array<record<T>>` field uses the SurrealQL
			// `CONTAINS` binary operator, with the RHS coerced to a record ID.
			let rhs = gql_to_sql_kind(v, Kind::Record(vec![]))?;
			exprs.push(Expr::Binary {
				left: Box::new(lhs),
				op: BinaryOperator::Contain,
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
			match op_name {
				"nearest" => exprs.push(translate_nearest(lhs, v)?),
				"similarity" => exprs.push(translate_similarity(lhs, v)?),
				"matches" => exprs.push(translate_matches(lhs, v)?),
				"call" => exprs.push(translate_call(lhs, v)?),
				_ => {
					return Err(resolver_error(format!("Unsupported filter operator: {op_name}")));
				}
			}
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

/// Translate a relation-field filter (e.g. `{ sent: { count: { gt: 5 } } }`)
/// into a SurrealQL `WHERE` expression like `count(->sent) > 5`.
///
/// Only the `count` operator is supported. Multiple count predicates combine
/// with AND. See GitHub issue #4554.
fn binop_for_relation(
	rel: &RelationFieldInfo,
	obj: &IndexMap<Name, GqlValue>,
) -> Result<Expr, GqlError> {
	if obj.is_empty() {
		return Err(resolver_error("Relation filter must have at least one operator"));
	}

	let lookup = Lookup {
		kind: LookupKind::Graph(rel.dir),
		what: vec![LookupSubject::Table {
			table: rel.relation_table.clone(),
			referencing_field: None,
		}],
		..Default::default()
	};
	let count_lhs = Expr::FunctionCall(Box::new(FunctionCall {
		receiver: Function::Normal("count".to_string()),
		arguments: vec![Expr::Idiom(Idiom(vec![Part::Lookup(Box::new(lookup))]))],
	}));

	let mut exprs: Vec<Expr> = Vec::new();

	for (k, v) in obj.iter() {
		match k.as_str() {
			"count" => {
				let count_obj = v
					.as_object()
					.ok_or(resolver_error("`count` must be an object of operators"))?;
				if count_obj.is_empty() {
					return Err(resolver_error("`count` filter must have at least one operator"));
				}
				for (op_name, op_val) in count_obj.iter() {
					let Some(binary_op) = parse_binary_op(op_name.as_str()) else {
						return Err(resolver_error(format!(
							"Unsupported count operator: {op_name}"
						)));
					};
					if matches!(binary_op, BinaryOperator::Inside) {
						return Err(resolver_error(
							"`in` is not supported on a relation `count` filter",
						));
					}
					let rhs = gql_to_sql_kind(op_val, Kind::Int)?;
					exprs.push(Expr::Binary {
						left: Box::new(count_lhs.clone()),
						op: binary_op,
						right: Box::new(rhs.into_literal()),
					});
				}
			}
			other => {
				return Err(resolver_error(format!(
					"Unsupported relation filter operator: {other}"
				)));
			}
		}
	}

	let mut iter = exprs.into_iter();
	let mut combined = iter.next().expect("at least one relation operator");
	for next in iter {
		combined = Expr::Binary {
			left: Box::new(combined),
			op: BinaryOperator::And,
			right: Box::new(next),
		};
	}
	Ok(combined)
}

/// Translate a `range: { from, to, inclusive }` filter on the `id` field into
/// a SurrealQL `WHERE` expression.
///
/// `from`/`to` may be omitted independently; `inclusive` defaults to `false`
/// (matching the SurrealQL `..` vs `..=` distinction). Unbounded on both sides
/// is rejected because it carries no useful selectivity.
fn translate_id_range(lhs: Expr, val: &GqlValue) -> Result<Expr, GqlError> {
	let obj = val.as_object().ok_or(resolver_error("Value of `range` must be an object"))?;

	let from = obj.get("from").filter(|v| !matches!(v, GqlValue::Null));
	let to = obj.get("to").filter(|v| !matches!(v, GqlValue::Null));
	let inclusive = obj
		.get("inclusive")
		.and_then(|v| match v {
			GqlValue::Boolean(b) => Some(*b),
			_ => None,
		})
		.unwrap_or(false);

	if from.is_none() && to.is_none() {
		return Err(resolver_error("`range` requires at least one of `from` or `to`"));
	}

	let mut clauses: Vec<Expr> = Vec::with_capacity(2);

	if let Some(f) = from {
		let rhs = gql_to_sql_kind(f, Kind::Record(vec![]))?;
		clauses.push(Expr::Binary {
			left: Box::new(lhs.clone()),
			op: BinaryOperator::MoreThanEqual,
			right: Box::new(rhs.into_literal()),
		});
	}

	if let Some(t) = to {
		let rhs = gql_to_sql_kind(t, Kind::Record(vec![]))?;
		let op = if inclusive {
			BinaryOperator::LessThanEqual
		} else {
			BinaryOperator::LessThan
		};
		clauses.push(Expr::Binary {
			left: Box::new(lhs),
			op,
			right: Box::new(rhs.into_literal()),
		});
	}

	let mut iter = clauses.into_iter();
	let mut combined = iter.next().expect("at least one range bound");
	for next in iter {
		combined = Expr::Binary {
			left: Box::new(combined),
			op: BinaryOperator::And,
			right: Box::new(next),
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

		if op_name == "in" {
			// SurrealQL's `INSIDE` operator doesn't accept a record ID on the
			// left side; expand `id: { in: [a, b, c] }` into
			// `id = a OR id = b OR id = c`.
			let rhs = gql_to_sql_kind(v, Kind::Array(Box::new(Kind::Record(vec![])), None))?;
			let ids = match rhs {
				Value::Array(arr) => arr.0,
				_ => return Err(resolver_error("`id.in` expected an array of IDs")),
			};
			if ids.is_empty() {
				return Err(resolver_error("`id.in` must contain at least one ID"));
			}
			// Cap list size so a request can't synthesise an unbounded `OR` chain.
			if ids.len() > MAX_ID_IN_LIST {
				return Err(resolver_error(format!(
					"`id.in` accepts at most {MAX_ID_IN_LIST} IDs (got {})",
					ids.len()
				)));
			}
			let mut iter = ids.into_iter();
			let first = iter.next().expect("non-empty");
			let mut combined = Expr::Binary {
				left: Box::new(lhs.clone()),
				op: BinaryOperator::Equal,
				right: Box::new(first.into_literal()),
			};
			for next in iter {
				combined = Expr::Binary {
					left: Box::new(combined),
					op: BinaryOperator::Or,
					right: Box::new(Expr::Binary {
						left: Box::new(lhs.clone()),
						op: BinaryOperator::Equal,
						right: Box::new(next.into_literal()),
					}),
				};
			}
			exprs.push(combined);
		} else if let Some(binary_op) = parse_binary_op(op_name) {
			let rhs = gql_to_sql_kind(v, Kind::Record(vec![]))?;
			exprs.push(Expr::Binary {
				left: Box::new(lhs),
				op: binary_op,
				right: Box::new(rhs.into_literal()),
			});
		} else if op_name == "range" {
			exprs.push(translate_id_range(lhs, v)?);
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

// ---------------------------------------------------------------------------
// Advanced filter operators (vector similarity / KNN, full-text matches,
// function-call predicates) — see GitHub issue #7312.
//
// The shared input types registered here are independent of any particular
// table, so they live in the global `types` vector and are reused across every
// `_filter_*` InputObject that opts in.
// ---------------------------------------------------------------------------

const VECTOR_DISTANCE_ENUM: &str = "_VectorDistance";
const NUM_OP_ENUM: &str = "_NumOp";
const KNN_INPUT: &str = "_KnnInput";
const SIMILARITY_INPUT: &str = "_SimilarityInput";
const MATCHES_INPUT: &str = "_MatchesInput";
const CALL_INPUT: &str = "_CallInput";
const ID_RANGE_INPUT: &str = "_IdRangeInput";
const COUNT_FILTER_INPUT: &str = "_CountFilterInput";
const PAGE_INFO_TYPE: &str = "PageInfo";

/// Register the shared InputObject / Enum types used by the advanced filter
/// operators (`nearest`, `similarity`, `matches`, `call`).
///
/// Registered unconditionally — async-graphql tolerates unused types, and the
/// schema is built once per `DEFINE CONFIG GRAPHQL`. Keeping registration
/// unconditional avoids drift between the filter generator and the type
/// registration step.
pub(crate) fn register_filter_helper_types(types: &mut Vec<Type>) {
	// `Minkowski(Number)` is intentionally omitted — it takes an extra param
	// that doesn't fit a flat enum; use the `call` operator with
	// `vector::distance::minkowski` if it's needed.
	types.push(Type::Enum(
		Enum::new(VECTOR_DISTANCE_ENUM)
			.description(
				"Vector distance / similarity metric for the `nearest` and `similarity` filter \
				 operators.",
			)
			.item("COSINE")
			.item("EUCLIDEAN")
			.item("MANHATTAN")
			.item("HAMMING")
			.item("JACCARD")
			.item("CHEBYSHEV")
			.item("PEARSON"),
	));

	types.push(Type::Enum(
		Enum::new(NUM_OP_ENUM)
			.description("Comparison operator for the `similarity` and `call` filter operators.")
			.item("eq")
			.item("ne")
			.item("gt")
			.item("gte")
			.item("lt")
			.item("lte"),
	));

	// `nearest` — SurrealQL `<|K, distance|>` operator.
	types.push(Type::InputObject(
		InputObject::new(KNN_INPUT)
			.description(
				"K-nearest-neighbour predicate.  Translates to SurrealQL `field <|k,distance|> to`.",
			)
			.field(InputValue::new("to", TypeRef::named_nn_list_nn(TypeRef::FLOAT)))
			.field(InputValue::new("k", TypeRef::named_nn(TypeRef::INT)))
			.field(InputValue::new("distance", TypeRef::named_nn(VECTOR_DISTANCE_ENUM))),
	));

	// `similarity` — calls `vector::similarity::*` or `vector::distance::*` on
	// the field and target, then compares the result against `value`.
	types.push(Type::InputObject(
		InputObject::new(SIMILARITY_INPUT)
			.description(
				"Vector similarity / distance predicate.  Calls the matching `vector::*` function \
				 on the field and `to`, then compares the result against `value` using `op`.",
			)
			.field(InputValue::new("to", TypeRef::named_nn_list_nn(TypeRef::FLOAT)))
			.field(InputValue::new("distance", TypeRef::named_nn(VECTOR_DISTANCE_ENUM)))
			.field(InputValue::new("op", TypeRef::named_nn(NUM_OP_ENUM)))
			.field(InputValue::new("value", TypeRef::named_nn(TypeRef::FLOAT))),
	));

	// `matches` — SurrealQL `@@` full-text-search operator.
	types.push(Type::InputObject(
		InputObject::new(MATCHES_INPUT)
			.description(
				"Full-text-search predicate.  Translates to SurrealQL `field @@ query`.  Requires \
				 a `DEFINE INDEX … SEARCH ANALYZER …` on the field.",
			)
			.field(InputValue::new("query", TypeRef::named_nn(TypeRef::STRING))),
	));

	// `call` — generic function-call predicate.
	types.push(Type::InputObject(
		InputObject::new(CALL_INPUT)
			.description(
				"Generic function-call predicate.  Translates to SurrealQL `fn(field, ...args) op \
				 value`.  Function permissions are enforced at execution time.",
			)
			.field(InputValue::new("fn", TypeRef::named_nn(TypeRef::STRING)))
			.field(InputValue::new("args", TypeRef::named_list("JSON")))
			.field(InputValue::new("op", TypeRef::named_nn(NUM_OP_ENUM)))
			.field(InputValue::new("value", TypeRef::named_nn("JSON"))),
	));

	// `range` — record ID range predicate. See GitHub issue #4555.
	types.push(Type::InputObject(
		InputObject::new(ID_RANGE_INPUT)
			.description(
				"Record ID range predicate. Omitted bounds are unbounded. `inclusive` selects \
				 `..=` (inclusive end) vs the default `..` (exclusive end). At least one of \
				 `from` or `to` must be supplied.",
			)
			.field(InputValue::new("from", TypeRef::named(TypeRef::ID)))
			.field(InputValue::new("to", TypeRef::named(TypeRef::ID)))
			.field(InputValue::new("inclusive", TypeRef::named(TypeRef::BOOLEAN))),
	));

	// `_filter_id` — id comparison input shared by every table filter.
	// Registered once globally; the per-table filter just references it by name.
	types.push(Type::InputObject(filter_id()));

	// `_CountFilterInput` — numeric comparison input used by relation `count`
	// predicates. See GitHub issue #4554.
	types.push(Type::InputObject(
		InputObject::new(COUNT_FILTER_INPUT)
			.description(
				"Numeric comparison applied to the count of a relation (graph) traversal in a \
				 WHERE clause. Multiple operators in one object combine with implicit AND.",
			)
			.field(InputValue::new("eq", TypeRef::named(TypeRef::INT)))
			.field(InputValue::new("ne", TypeRef::named(TypeRef::INT)))
			.field(InputValue::new("gt", TypeRef::named(TypeRef::INT)))
			.field(InputValue::new("gte", TypeRef::named(TypeRef::INT)))
			.field(InputValue::new("lt", TypeRef::named(TypeRef::INT)))
			.field(InputValue::new("lte", TypeRef::named(TypeRef::INT))),
	));

	// `PageInfo` — Relay-style cursor pagination metadata.
	// Per-table `<Table>Connection` / `<Table>Edge` types reference this.
	// The connection resolver passes a `PageInfoValue` as the parent value of
	// this object.
	types.push(Type::Object(
		Object::new(PAGE_INFO_TYPE)
			.description(
				"Cursor pagination metadata. `hasNextPage` / `hasPreviousPage` are computed \
				 from the over-fetch on the requested direction; the opposite-direction flag \
				 runs a small probe query when actually selected by the client.",
			)
			.field(Field::new("hasNextPage", TypeRef::named_nn(TypeRef::BOOLEAN), |ctx| {
				FieldFuture::new(async move {
					let p = ctx.parent_value.try_downcast_ref::<PageInfoValue>()?;
					Ok(Some(FieldValue::value(GqlValue::Boolean(p.has_next_page))))
				})
			}))
			.field(Field::new("hasPreviousPage", TypeRef::named_nn(TypeRef::BOOLEAN), |ctx| {
				FieldFuture::new(async move {
					let p = ctx.parent_value.try_downcast_ref::<PageInfoValue>()?;
					Ok(Some(FieldValue::value(GqlValue::Boolean(p.has_previous_page))))
				})
			}))
			.field(Field::new("startCursor", TypeRef::named(TypeRef::STRING), |ctx| {
				FieldFuture::new(async move {
					let p = ctx.parent_value.try_downcast_ref::<PageInfoValue>()?;
					Ok(Some(match p.start_cursor.as_deref() {
						Some(s) => FieldValue::value(GqlValue::String(s.to_owned())),
						None => FieldValue::value(GqlValue::Null),
					}))
				})
			}))
			.field(Field::new("endCursor", TypeRef::named(TypeRef::STRING), |ctx| {
				FieldFuture::new(async move {
					let p = ctx.parent_value.try_downcast_ref::<PageInfoValue>()?;
					Ok(Some(match p.end_cursor.as_deref() {
						Some(s) => FieldValue::value(GqlValue::String(s.to_owned())),
						None => FieldValue::value(GqlValue::Null),
					}))
				})
			})),
	));
}

/// Parent value attached to a `PageInfo` Object in connection responses.
#[derive(Clone, Debug, Default)]
struct PageInfoValue {
	has_next_page: bool,
	has_previous_page: bool,
	start_cursor: Option<String>,
	end_cursor: Option<String>,
}

/// Unwrap `Kind::Either([None|Null, T])` to `T`, leaving other kinds unchanged.
fn strip_option(kind: &Kind) -> Kind {
	match kind {
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
	}
}

/// If `kind` is an `array<T>` (recursing through `Either`/`Option`) whose
/// element kind is numeric, return the inner numeric kind.
fn numeric_array_inner(kind: &Kind) -> Option<Kind> {
	match kind {
		Kind::Array(inner, _) => match inner.as_ref() {
			Kind::Float | Kind::Int | Kind::Number | Kind::Decimal => Some(*inner.clone()),
			Kind::Either(ks) => {
				let non_none: Vec<&Kind> =
					ks.iter().filter(|k| !matches!(k, Kind::None | Kind::Null)).collect();
				if non_none.len() == 1
					&& matches!(non_none[0], Kind::Float | Kind::Int | Kind::Number | Kind::Decimal)
				{
					Some(non_none[0].clone())
				} else {
					None
				}
			}
			_ => None,
		},
		Kind::Either(ks) => {
			let non_none: Vec<&Kind> =
				ks.iter().filter(|k| !matches!(k, Kind::None | Kind::Null)).collect();
			if non_none.len() == 1 {
				numeric_array_inner(non_none[0])
			} else {
				None
			}
		}
		_ => None,
	}
}

fn distance_variant(name: &str) -> Option<crate::catalog::Distance> {
	use crate::catalog::Distance;
	match name {
		"COSINE" => Some(Distance::Cosine),
		"EUCLIDEAN" => Some(Distance::Euclidean),
		"MANHATTAN" => Some(Distance::Manhattan),
		"HAMMING" => Some(Distance::Hamming),
		"JACCARD" => Some(Distance::Jaccard),
		"CHEBYSHEV" => Some(Distance::Chebyshev),
		"PEARSON" => Some(Distance::Pearson),
		_ => None,
	}
}

/// Cosine/Jaccard/Pearson have dedicated *similarity* functions
/// (higher = closer); the others fall back to the corresponding *distance*
/// function (lower = closer).
fn distance_function_name(name: &str) -> Option<&'static str> {
	match name {
		"COSINE" => Some("vector::similarity::cosine"),
		"JACCARD" => Some("vector::similarity::jaccard"),
		"PEARSON" => Some("vector::similarity::pearson"),
		"EUCLIDEAN" => Some("vector::distance::euclidean"),
		"MANHATTAN" => Some("vector::distance::manhattan"),
		"HAMMING" => Some("vector::distance::hamming"),
		"CHEBYSHEV" => Some("vector::distance::chebyshev"),
		_ => None,
	}
}

fn num_op_to_binop(name: &str) -> Option<BinaryOperator> {
	match name {
		"eq" => Some(BinaryOperator::Equal),
		"ne" => Some(BinaryOperator::NotEqual),
		"gt" => Some(BinaryOperator::MoreThan),
		"gte" => Some(BinaryOperator::MoreThanEqual),
		"lt" => Some(BinaryOperator::LessThan),
		"lte" => Some(BinaryOperator::LessThanEqual),
		_ => None,
	}
}

/// Extract an enum token (`GqlValue::Enum` or `GqlValue::String`) by name from
/// an InputObject, returning a resolver error if missing or wrong-typed.
fn take_enum<'a>(obj: &'a IndexMap<Name, GqlValue>, key: &str) -> Result<&'a str, GqlError> {
	let Some(v) = obj.get(key) else {
		return Err(resolver_error(format!("missing `{key}` in filter operator input")));
	};
	match v {
		GqlValue::Enum(n) => Ok(n.as_str()),
		GqlValue::String(s) => Ok(s.as_str()),
		_ => Err(resolver_error(format!("`{key}` must be an enum or string"))),
	}
}

/// Translate a `_KnnInput` GqlValue object into
/// `Expr::Binary { left: field, op: NearestNeighbor::K(k, dist), right: to }`.
fn translate_nearest(field: Expr, val: &GqlValue) -> Result<Expr, GqlError> {
	use crate::expr::operator::NearestNeighbor;
	let obj = val.as_object().ok_or(resolver_error("`nearest` value must be an object"))?;

	let k = obj
		.get("k")
		.and_then(|v| v.as_i64())
		.ok_or(resolver_error("`nearest.k` must be an integer"))?;
	let k: u32 =
		u32::try_from(k.max(0)).map_err(|_| resolver_error("`nearest.k` does not fit in a u32"))?;

	let dist_name = take_enum(obj, "distance")?;
	let dist = distance_variant(dist_name)
		.ok_or_else(|| resolver_error(format!("Unknown distance metric: {dist_name}")))?;

	let to_val = obj.get("to").ok_or(resolver_error("`nearest.to` is required"))?;
	let to = gql_to_sql_kind(to_val, Kind::Array(Box::new(Kind::Float), None))?;

	Ok(Expr::Binary {
		left: Box::new(field),
		op: BinaryOperator::NearestNeighbor(Box::new(NearestNeighbor::K(k, dist))),
		right: Box::new(to.into_literal()),
	})
}

/// Translate a `_SimilarityInput` GqlValue into `vector::*(field, to) <op> value`.
fn translate_similarity(field: Expr, val: &GqlValue) -> Result<Expr, GqlError> {
	let obj = val.as_object().ok_or(resolver_error("`similarity` value must be an object"))?;

	let dist_name = take_enum(obj, "distance")?;
	let fn_name = distance_function_name(dist_name)
		.ok_or_else(|| resolver_error(format!("Unknown distance metric: {dist_name}")))?;

	let op_name = take_enum(obj, "op")?;
	let op = num_op_to_binop(op_name)
		.ok_or_else(|| resolver_error(format!("Unknown comparison op: {op_name}")))?;

	let value = obj.get("value").ok_or(resolver_error("`similarity.value` is required"))?;
	let value = gql_to_sql_kind(value, Kind::Float)?;

	let to_val = obj.get("to").ok_or(resolver_error("`similarity.to` is required"))?;
	let to = gql_to_sql_kind(to_val, Kind::Array(Box::new(Kind::Float), None))?;

	let call = Expr::FunctionCall(Box::new(FunctionCall {
		receiver: Function::Normal(fn_name.to_string()),
		arguments: vec![field, to.into_literal()],
	}));

	Ok(Expr::Binary {
		left: Box::new(call),
		op,
		right: Box::new(value.into_literal()),
	})
}

/// Translate a `_MatchesInput` GqlValue into `field @@ query`.
fn translate_matches(field: Expr, val: &GqlValue) -> Result<Expr, GqlError> {
	use crate::expr::operator::{BooleanOperator, MatchesOperator};
	let obj = val.as_object().ok_or(resolver_error("`matches` value must be an object"))?;
	let q = obj
		.get("query")
		.and_then(|v| match v {
			GqlValue::String(s) => Some(s.as_str()),
			_ => None,
		})
		.ok_or(resolver_error("`matches.query` must be a string"))?;
	let query = gql_to_sql_kind(&GqlValue::String(q.to_string()), Kind::String)?;
	Ok(Expr::Binary {
		left: Box::new(field),
		op: BinaryOperator::Matches(MatchesOperator {
			rf: None,
			operator: BooleanOperator::And,
		}),
		right: Box::new(query.into_literal()),
	})
}

/// Translate a `_CallInput` GqlValue into `fn(field, ...args) <op> value`.
fn translate_call(field: Expr, val: &GqlValue) -> Result<Expr, GqlError> {
	let obj = val.as_object().ok_or(resolver_error("`call` value must be an object"))?;

	let fn_name = obj
		.get("fn")
		.and_then(|v| match v {
			GqlValue::String(s) => Some(s.as_str()),
			_ => None,
		})
		.ok_or(resolver_error("`call.fn` must be a string"))?;

	let mut args: Vec<Expr> = vec![field];
	if let Some(args_val) = obj.get("args")
		&& !matches!(args_val, GqlValue::Null)
	{
		let list = args_val.as_list().ok_or(resolver_error("`call.args` must be a list"))?;
		for a in list {
			let v = gql_to_sql_kind(a, Kind::Any)?;
			args.push(v.into_literal());
		}
	}

	let op_name = take_enum(obj, "op")?;
	let op = num_op_to_binop(op_name)
		.ok_or_else(|| resolver_error(format!("Unknown comparison op: {op_name}")))?;

	let value = obj.get("value").ok_or(resolver_error("`call.value` is required"))?;
	let value = gql_to_sql_kind(value, Kind::Any)?;

	// User-defined functions (`fn::name`) dispatch via `Function::Custom`; the
	// engine prepends the `fn::` prefix itself. Built-in functions
	// (`string::len`, `vector::*`, …) dispatch via `Function::Normal`.
	let receiver = if let Some(custom) = fn_name.strip_prefix("fn::") {
		Function::Custom(custom.to_string())
	} else {
		Function::Normal(fn_name.to_string())
	};

	let call = Expr::FunctionCall(Box::new(FunctionCall {
		receiver,
		arguments: args,
	}));

	Ok(Expr::Binary {
		left: Box::new(call),
		op,
		right: Box::new(value.into_literal()),
	})
}

// ---------------------------------------------------------------------------
// Aggregations — `{table}_aggregate(filter, groupBy): [{Table}Aggregate!]!`
// See GitHub issue #7312.
// ---------------------------------------------------------------------------

/// Wrapper for a single row of the aggregate result.  Field resolvers on the
/// generated `{Table}Aggregate` Object downcast `parent_value` to this type
/// and pull the corresponding key (count, `<field>_min`, …, or a group key).
#[derive(Clone)]
struct AggregateRow(SurObject);

/// Return `true` if `kind` is numeric (Float/Int/Number/Decimal), recursing
/// through `Either`/`Option` once.
fn is_numeric_kind(kind: &Kind) -> bool {
	match kind {
		Kind::Float | Kind::Int | Kind::Number | Kind::Decimal => true,
		Kind::Either(ks) => {
			let non_none: Vec<&Kind> =
				ks.iter().filter(|k| !matches!(k, Kind::None | Kind::Null)).collect();
			non_none.len() == 1
				&& matches!(non_none[0], Kind::Float | Kind::Int | Kind::Number | Kind::Decimal)
		}
		_ => false,
	}
}

/// Return the field's effective numeric kind (stripping `option<…>`), or
/// `Kind::Number` as a fallback.
fn numeric_kind(kind: &Kind) -> Kind {
	match kind {
		Kind::Float | Kind::Int | Kind::Number | Kind::Decimal => kind.clone(),
		Kind::Either(ks) => {
			let non_none: Vec<Kind> =
				ks.iter().filter(|k| !matches!(k, Kind::None | Kind::Null)).cloned().collect();
			if non_none.len() == 1 {
				non_none.into_iter().next().expect("len == 1")
			} else {
				Kind::Number
			}
		}
		_ => Kind::Number,
	}
}

fn aggregate_object_type_name(tb: &str) -> String {
	format!("{tb}_aggregate_row")
}

fn aggregate_field_name(tb: &str) -> String {
	format!("{tb}_aggregate")
}

fn aggregate_groupable_enum_name(tb: &str) -> String {
	format!("_groupable_{tb}")
}

/// Build the `{T}_aggregate_row` Object type along with the
/// `_groupable_{T}` enum.  Returns `None` if the table has no numeric fields
/// AND no groupable fields (in which case `count` alone is exposed via a
/// degenerate type — still useful, so always Some).
fn build_aggregate_type(
	tb_name: &str,
	fds: &[FieldDefinition],
	types: &mut Vec<Type>,
) -> (Object, Enum) {
	let obj_name = aggregate_object_type_name(tb_name);
	let mut obj = Object::new(&obj_name)
		.description(format!("Aggregation row for `{tb_name}`. `count` is always set; numeric `{{field}}_min/max/sum/avg` are filled per numeric field; group-key columns hold the value of each requested `groupBy` field for the row."));

	// count: Int!
	obj = obj.field(Field::new("count", TypeRef::named_nn(TypeRef::INT), |ctx| {
		FieldFuture::new(async move {
			let row = ctx.parent_value.try_downcast_ref::<AggregateRow>()?;
			let v = row.0.get("count").cloned().unwrap_or(Value::None);
			Ok(Some(FieldValue::value(sql_value_to_gql_value(v)?)))
		})
	}));

	let mut groupable_items: Vec<String> = Vec::new();

	for fd in fds {
		// Use the GraphQL-safe form of the field name as both the alias prefix
		// and the resolver lookup key. The underlying SurrealQL `Idiom` still
		// quotes reserved-word identifiers when serialised, so the SELECT
		// statement that the resolver builds remains valid.
		let fname = idiom_to_gql_name(&fd.name);
		// Fields without an explicit type fall back to `Any` so the aggregate
		// type-check below treats them as non-numeric.
		let kind = fd.field_kind.clone().unwrap_or(Kind::Any);
		if is_numeric_kind(&kind) {
			let inner = numeric_kind(&kind);
			let ty_min_max = match kind_to_type(inner.clone(), types, false) {
				Ok(t) => unwrap_type(t),
				Err(_) => TypeRef::named("number"),
			};
			// sum/avg use the same numeric kind for min/max, but `math::mean`
			// always returns a Number, and `math::sum` widens — keep the field
			// type tolerant (`Number`) so Decimal sums don't get truncated.
			let ty_sum = TypeRef::named("number");
			let ty_avg = TypeRef::named("number");

			for (suffix, ty) in
				[("min", ty_min_max.clone()), ("max", ty_min_max), ("sum", ty_sum), ("avg", ty_avg)]
			{
				let key = format!("{fname}_{suffix}");
				let key_for_resolver = key.clone();
				obj = obj.field(Field::new(key, ty, move |ctx| {
					let k = key_for_resolver.clone();
					FieldFuture::new(async move {
						let row = ctx.parent_value.try_downcast_ref::<AggregateRow>()?;
						let v = row.0.get(k.as_str()).cloned().unwrap_or(Value::None);
						if matches!(v, Value::None | Value::Null) {
							Ok(None)
						} else {
							Ok(Some(FieldValue::value(sql_value_to_gql_value(v)?)))
						}
					})
				}));
			}
		} else {
			// Non-numeric: expose as a group-key column on the row, and add to
			// the groupable enum (so the caller can ask for it via `groupBy`).
			let ty = match kind_to_type_with_enum_prefix(
				kind.clone(),
				types,
				false,
				Some(&format!("{tb_name}_{fname}")),
			) {
				Ok(t) => unwrap_type(t),
				Err(_) => TypeRef::named("any"),
			};
			let key_for_resolver = fname.clone();
			obj = obj.field(Field::new(fname.clone(), ty, move |ctx| {
				let k = key_for_resolver.clone();
				FieldFuture::new(async move {
					let row = ctx.parent_value.try_downcast_ref::<AggregateRow>()?;
					let v = row.0.get(k.as_str()).cloned().unwrap_or(Value::None);
					if matches!(v, Value::None | Value::Null) {
						Ok(None)
					} else {
						Ok(Some(FieldValue::value(sql_value_to_gql_value(v)?)))
					}
				})
			}));
			groupable_items.push(fname);
		}
	}

	let enum_name = aggregate_groupable_enum_name(tb_name);
	let mut groupable = Enum::new(&enum_name).description(format!(
		"Fields of `{tb_name}` that can be used as `groupBy` keys in the aggregate query."
	));
	if groupable_items.is_empty() {
		// async-graphql disallows empty enums — emit a single placeholder so
		// the type stays valid even for tables with only numeric fields.
		groupable = groupable.item("_NONE");
	} else {
		for item in &groupable_items {
			groupable = groupable.item(item);
		}
	}

	(obj, groupable)
}

/// Build the `{table}_aggregate(filter, groupBy): [{Table}AggregateRow!]!`
/// Query field.
fn make_table_aggregate_field(
	tb: &TableDefinition,
	fds: Arc<[FieldDefinition]>,
	rel_filters: Arc<[RelationFieldInfo]>,
	kvs: Arc<Datastore>,
) -> Field {
	let tb_name = tb.name.clone();
	let tb_name_str = tb_name.as_str().to_string();
	let table_filter_name = filter_name_from_table(&tb_name);
	let aggregate_row_name = aggregate_object_type_name(&tb_name_str);
	let groupable_enum_name = aggregate_groupable_enum_name(&tb_name_str);
	// The aggregate Query field uses the active list-field name as its prefix
	// so it pluralises in Apollo mode and honours any explicit alias.
	let field_name = aggregate_field_name(&super::naming::list_field_name(tb));

	let numeric_fields: Vec<String> = fds
		.iter()
		.filter(|fd| fd.field_kind.as_ref().is_some_and(is_numeric_kind))
		.map(|fd| idiom_to_gql_name(&fd.name))
		.collect();

	Field::new(field_name, TypeRef::named_nn_list_nn(&aggregate_row_name), move |ctx| {
		let tb_name = tb_name.clone();
		let fds = Arc::clone(&fds);
		let rel_filters = Arc::clone(&rel_filters);
		let kvs = Arc::clone(&kvs);
		let numeric_fields = numeric_fields.clone();
		FieldFuture::new(async move {
			let sess = ctx.data::<Arc<Session>>()?;
			let args = ctx.args.as_index_map();

			let tb_name_str_ref = tb_name.as_str();
			let cond = parse_filter_arg(args, &fds, tb_name_str_ref, &rel_filters)?;

			// Parse groupBy: list of enum tokens identifying groupable fields.
			let mut group_keys: Vec<String> = Vec::new();
			if let Some(gb) = args.get("groupBy")
				&& !matches!(gb, GqlValue::Null)
			{
				let list = gb
					.as_list()
					.ok_or(resolver_error("`groupBy` must be a list"))?;
				for v in list {
					let name = match v {
						GqlValue::Enum(n) => n.as_str(),
						GqlValue::String(s) => s.as_str(),
						_ => {
							return Err(resolver_error(
								"`groupBy` items must be enum values",
							)
							.into());
						}
					};
					if name != "_NONE" {
						group_keys.push(name.to_string());
					}
				}
			}

			// Build SELECT fields:
			//   count() AS count, math::*(F) AS F_min/max/sum/avg, plus each groupBy field.
			let mut select_fields: Vec<SelectField> = Vec::new();
			select_fields.push(SelectField::Single(Selector {
				expr: Expr::FunctionCall(Box::new(FunctionCall {
					receiver: Function::Normal("count".to_string()),
					arguments: vec![],
				})),
				alias: Some(Idiom::field("count".to_string())),
			}));
			for fname in &numeric_fields {
				for (suffix, fn_name) in [
					("min", "math::min"),
					("max", "math::max"),
					("sum", "math::sum"),
					("avg", "math::mean"),
				] {
					select_fields.push(SelectField::Single(Selector {
						expr: Expr::FunctionCall(Box::new(FunctionCall {
							receiver: Function::Normal(fn_name.to_string()),
							arguments: vec![Expr::Idiom(Idiom::field(fname.clone()))],
						})),
						alias: Some(Idiom::field(format!("{fname}_{suffix}"))),
					}));
				}
			}
			for gk in &group_keys {
				select_fields.push(SelectField::Single(Selector {
					expr: Expr::Idiom(Idiom::field(gk.clone())),
					alias: None,
				}));
			}

			// Always set a `GROUP` clause so aggregate functions like
			// `math::mean(price)` collect values across rows.  An empty
			// `Groups(vec![])` is SurrealQL's `GROUP ALL`, which collapses
			// every row into a single aggregate result.
			let group = Some(Groups(
				group_keys.iter().map(|gk| Group(Idiom::field(gk.clone()))).collect(),
			));

			let stmt = SelectStatement {
				what: vec![Expr::Table(tb_name)],
				fields: Fields::Select(select_fields),
				cond,
				group,
				order: None,
				limit: None,
				start: None,
				version: version_to_expr(&None),
				timeout: Expr::Literal(Literal::None),
				omit: vec![],
				only: false,
				with: None,
				split: None,
				fetch: None,
				explain: None,
				tempfiles: false,
			};
			let res = execute_select(&kvs, sess, stmt).await?;

			let arr = match res {
				Value::Array(a) => a,
				v => SurArray::from(vec![v]),
			};

			let items: Vec<FieldValue> = arr
				.iter()
				.map(|v| {
					let obj = match v {
						Value::Object(o) => o.clone(),
						_ => SurObject::default(),
					};
					FieldValue::owned_any(AggregateRow(obj))
				})
				.collect();
			Ok(Some(FieldValue::list(items)))
		})
	})
	.description(format!(
		"Aggregation query over `{tb_name_str}`. Returns `count` plus per-numeric-field `min/max/sum/avg`. Group rows by one or more non-numeric fields via the `groupBy` argument."
	))
	.argument(InputValue::new("filter", TypeRef::named(&table_filter_name)))
	.argument(InputValue::new("groupBy", TypeRef::named_list(&groupable_enum_name)))
}

// ---------------------------------------------------------------------------
// Cursor pagination — Relay-style `<plural>Connection(first, after)` field.
// Forward-only: returns `{ edges: [{ cursor, node }], pageInfo: { hasNextPage,
// endCursor }, totalCount }`. Cursors are base64-encoded record IDs.
// ---------------------------------------------------------------------------

/// Parent value attached to an `<Table>Edge` Object in a connection response.
#[derive(Clone, Debug)]
struct EdgeValue {
	cursor: String,
	node: CachedRecord,
}

fn connection_type_name(tb: &TableDefinition) -> String {
	format!("{}Connection", super::naming::to_pascal_case(super::naming::table_base_name(tb)))
}

fn edge_type_name(tb: &TableDefinition) -> String {
	format!("{}Edge", super::naming::to_pascal_case(super::naming::table_base_name(tb)))
}

/// Build the per-table `<Table>Connection` and `<Table>Edge` Object types.
fn build_connection_types(tb: &TableDefinition, types: &mut Vec<Type>) {
	let tb_name_str = tb.name.as_str().to_string();
	let connection_name = connection_type_name(tb);
	let edge_name = edge_type_name(tb);
	let node_type = tb_name_str.clone();

	// <Table>Edge { cursor: String!, node: <table>! }
	let edge_obj = Object::new(&edge_name)
		.description(format!("A single edge in a `{tb_name_str}` cursor-paginated connection."))
		.field(Field::new("cursor", TypeRef::named_nn(TypeRef::STRING), |ctx| {
			FieldFuture::new(async move {
				let e = ctx.parent_value.try_downcast_ref::<EdgeValue>()?;
				Ok(Some(FieldValue::value(GqlValue::String(e.cursor.clone()))))
			})
		}))
		.field(Field::new("node", TypeRef::named_nn(node_type), move |ctx| {
			FieldFuture::new(async move {
				let e = ctx.parent_value.try_downcast_ref::<EdgeValue>()?;
				Ok(Some(FieldValue::owned_any(e.node.clone())))
			})
		}));
	types.push(Type::Object(edge_obj));

	// <Table>Connection { edges, pageInfo, totalCount }
	let conn_obj = Object::new(&connection_name)
		.description(format!(
			"Cursor-paginated `{tb_name_str}` records. Forward: pass `after` and read \
			 `pageInfo.endCursor`. Backward: pass `before` and read `pageInfo.startCursor`."
		))
		.field(Field::new("edges", TypeRef::named_nn_list_nn(&edge_name), |ctx| {
			FieldFuture::new(async move {
				let c = ctx.parent_value.try_downcast_ref::<ConnectionValue>()?;
				let items: Vec<FieldValue> =
					c.edges.iter().cloned().map(FieldValue::owned_any).collect();
				Ok(Some(FieldValue::list(items)))
			})
		}))
		.field(Field::new("pageInfo", TypeRef::named_nn(PAGE_INFO_TYPE), |ctx| {
			FieldFuture::new(async move {
				let c = ctx.parent_value.try_downcast_ref::<ConnectionValue>()?;
				Ok(Some(FieldValue::owned_any(c.page_info.clone())))
			})
		}))
		.field(Field::new("totalCount", TypeRef::named(TypeRef::INT), |ctx| {
			FieldFuture::new(async move {
				let c = ctx.parent_value.try_downcast_ref::<ConnectionValue>()?;
				// Run `SELECT count() FROM <table> WHERE <cond> GROUP ALL`
				// only when the caller actually requests this field.
				let sess = ctx.data::<Arc<Session>>()?;
				let count = run_connection_total_count(&c.total_count_query, sess).await?;
				Ok(Some(FieldValue::value(GqlValue::Number(count.into()))))
			})
		}));
	types.push(Type::Object(conn_obj));
}

/// Parent value for `<Table>Connection`.
///
/// `total_count_query` carries everything needed to run a lazy `count()`
/// when (and only when) the GraphQL query asks for `totalCount`. The count
/// is computed against the same filter used for the edge query so the
/// reported number matches what pagination would walk through.
#[derive(Clone)]
struct ConnectionValue {
	edges: Vec<EdgeValue>,
	page_info: PageInfoValue,
	total_count_query: TotalCountQuery,
}

/// Stored connection state needed to run `count()` on demand.
///
/// `Datastore` doesn't impl `Debug`, so neither do we — the wrapping
/// `ConnectionValue` is only used as a `FieldValue::owned_any` payload anyway.
#[derive(Clone)]
struct TotalCountQuery {
	kvs: Arc<Datastore>,
	tb_name: TableName,
	cond: Option<Cond>,
	version: Option<Datetime>,
}

fn encode_cursor(rid: &RecordId) -> String {
	use base64::Engine;
	base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(rid.to_sql())
}

/// Inspect the resolver's selection set to decide whether the opposite-direction
/// `pageInfo` probe should run. We only care about the field selected on the
/// *opposite* side of the requested page:
/// - forward pagination => `hasPreviousPage`
/// - backward pagination => `hasNextPage`
fn connection_selects_opposite_page_flag(
	ctx: &async_graphql::dynamic::ResolverContext<'_>,
	backward: bool,
) -> bool {
	let target = if backward {
		"hasNextPage"
	} else {
		"hasPreviousPage"
	};
	for top in ctx.field().selection_set() {
		if top.name() == "pageInfo" {
			for inner in top.selection_set() {
				if inner.name() == target {
					return true;
				}
			}
		}
	}
	false
}

fn decode_cursor(s: &str) -> Option<RecordId> {
	use base64::Engine;
	let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s).ok()?;
	let id_str = String::from_utf8(bytes).ok()?;
	crate::syn::record_id(&id_str).ok().map(Into::into)
}

/// Run `SELECT count() FROM <tb> WHERE <cond> GROUP ALL` (with optional
/// `VERSION`) for the lazy `totalCount` field on a connection. Returns 0 when
/// the table is empty under the filter.
async fn run_connection_total_count(
	q: &TotalCountQuery,
	sess: &Arc<Session>,
) -> Result<i64, async_graphql::Error> {
	let stmt = SelectStatement {
		what: vec![Expr::Table(q.tb_name.clone())],
		fields: Fields::Select(vec![SelectField::Single(Selector {
			expr: Expr::FunctionCall(Box::new(FunctionCall {
				receiver: Function::Normal("count".to_string()),
				arguments: vec![],
			})),
			alias: Some(Idiom::field("count".to_string())),
		})]),
		cond: q.cond.clone(),
		group: Some(Groups(Vec::new())),
		version: version_to_expr(&q.version),
		timeout: Expr::Literal(Literal::None),
		omit: vec![],
		only: false,
		with: None,
		split: None,
		order: None,
		limit: None,
		start: None,
		fetch: None,
		explain: None,
		tempfiles: false,
	};
	let res = execute_select(&q.kvs, sess, stmt).await?;
	let arr = match res {
		Value::Array(a) => a,
		_ => return Ok(0),
	};
	let Some(first) = arr.0.into_iter().next() else {
		return Ok(0);
	};
	let Value::Object(obj) = first else {
		return Ok(0);
	};
	match obj.get("count") {
		Some(Value::Number(n)) => Ok(n.to_int()),
		_ => Ok(0),
	}
}

/// Build the `<plural>Connection(first, after, filter, where, order, version)` Query
/// field for a table.
fn make_table_connection_field(
	tb: &TableDefinition,
	fds: Arc<[FieldDefinition]>,
	rel_filters: Arc<[RelationFieldInfo]>,
	kvs: Arc<Datastore>,
) -> Field {
	let tb_name = tb.name.clone();
	let tb_name_str = tb_name.as_str().to_string();
	let connection_name = connection_type_name(tb);
	let table_filter_name = filter_name_from_table(&tb_name);
	let field_name = format!("{}Connection", super::naming::list_field_name(tb));

	Field::new(field_name, TypeRef::named_nn(connection_name), move |ctx| {
		let tb_name = tb_name.clone();
		let fds = Arc::clone(&fds);
		let rel_filters = Arc::clone(&rel_filters);
		let kvs = Arc::clone(&kvs);
		FieldFuture::new(async move {
			let sess = ctx.data::<Arc<Session>>()?;
			let args = ctx.args.as_index_map();
			let version = parse_version_arg(args)?;
			let mut cond = parse_filter_arg(args, &fds, tb_name.as_str(), &rel_filters)?;

			let first = args.get("first").and_then(|v| v.as_i64());
			let last = args.get("last").and_then(|v| v.as_i64());
			let after = match args.get("after") {
				Some(GqlValue::String(s)) => Some(s.as_str().to_string()),
				_ => None,
			};
			let before = match args.get("before") {
				Some(GqlValue::String(s)) => Some(s.as_str().to_string()),
				_ => None,
			};

			// Mutual exclusion. `first`/`after` is forward; `last`/`before` is
			// backward. Mixing produces ambiguous results — reject it.
			if first.is_some() && last.is_some() {
				return Err(resolver_error("Pass either `first` or `last`, not both").into());
			}
			if after.is_some() && before.is_some() {
				return Err(resolver_error("Pass either `after` or `before`, not both").into());
			}
			let backward = last.is_some() || before.is_some();

			// Page size. Forward defaults to 20; backward requires `last`
			// (defaulted to 20 if omitted with `before`).
			let page_size = if backward {
				last.unwrap_or(20).clamp(1, 1000)
			} else {
				first.unwrap_or(20).clamp(1, 1000)
			};

			// Append cursor predicate. Forward = `id > after`; backward =
			// `id < before`. We keep the user's `order:` argument for forward
			// pagination but force `ORDER BY id DESC` for backward (then
			// reverse to return ascending-by-id edges).
			let cursor_str = if backward {
				before.as_deref()
			} else {
				after.as_deref()
			};
			// Save the user's cond before mixing in the cursor predicate so the
			// opposite-direction pageInfo probe below can re-apply it without
			// the cursor constraint.
			let cond_without_cursor = cond.clone();
			let decoded_cursor: Option<RecordId> = match cursor_str {
				Some(cs) => {
					let rid = decode_cursor(cs).ok_or_else(|| resolver_error("invalid cursor"))?;
					if rid.table.as_str() != tb_name.as_str() {
						return Err(resolver_error(format!(
							"cursor table mismatch: cursor decodes to table `{}`, expected `{}`",
							rid.table.as_str(),
							tb_name.as_str()
						))
						.into());
					}
					let op = if backward {
						BinaryOperator::LessThan
					} else {
						BinaryOperator::MoreThan
					};
					let extra = Expr::Binary {
						left: Box::new(Expr::Idiom(Idiom::field("id".to_string()))),
						op,
						right: Box::new(Value::RecordId(rid.clone()).into_literal()),
					};
					cond = Some(match cond {
						Some(Cond(prev)) => Cond(Expr::Binary {
							left: Box::new(prev),
							op: BinaryOperator::And,
							right: Box::new(extra),
						}),
						None => Cond(extra),
					});
					Some(rid)
				}
				None => None,
			};

			// Cache the resolved cond/version so `totalCount` can run an
			// independent `count()` query if requested.
			let total_count_query = TotalCountQuery {
				kvs: Arc::clone(&kvs),
				tb_name: tb_name.clone(),
				cond: cond.clone(),
				version,
			};

			// Cursors are id-keyed, so always order by id. Backward fetches
			// `id DESC` (then reverses the slice so edges read ascending);
			// forward fetches `id ASC`.
			let order_for_query = if backward {
				Some(Ordering::Order(OrderList(vec![expr::Order {
					value: Idiom::field("id".to_string()),
					..expr::Order::default()
				}])))
			} else {
				Some(Ordering::Order(OrderList(vec![order_asc("id".to_string())])))
			};

			// Fetch `page_size + 1` to detect the "more results" boundary
			// without a second round-trip.
			let limit = Some(Limit(Expr::Literal(Literal::Integer(page_size.saturating_add(1)))));
			let stmt = select_all_from_table(
				Expr::Table(tb_name.clone()),
				cond,
				order_for_query,
				limit,
				None,
				&version,
			);
			let res = execute_select(&kvs, sess, stmt).await?;
			let arr = match res {
				Value::Array(a) => a.0,
				v => {
					error!("connection query returned non-array: {v:?}");
					return Err(internal_error("connection query result not an array").into());
				}
			};

			let returned = arr.len() as i64;
			let has_more = returned > page_size;
			let mut edges: Vec<EdgeValue> = Vec::with_capacity(page_size as usize);
			for v in arr.into_iter().take(page_size as usize) {
				let Value::Object(obj) = v else {
					continue;
				};
				let rid = match obj.get("id") {
					Some(Value::RecordId(rid)) => rid.clone(),
					_ => continue,
				};
				let cursor = encode_cursor(&rid);
				edges.push(EdgeValue {
					cursor,
					node: CachedRecord {
						rid,
						version,
						data: obj,
					},
				});
			}

			// Backward pagination fetched in descending order; reverse for
			// the user so edges read ascending by id (matching forward).
			if backward {
				edges.reverse();
			}

			let start_cursor = edges.first().map(|e| e.cursor.clone());
			let end_cursor = edges.last().map(|e| e.cursor.clone());
			// Relay semantics: the over-fetch-by-one on the main query gives us
			// the "more results in the pagination direction" flag for free.
			// For the opposite direction we issue a tiny LIMIT 1 probe against
			// the supplied cursor so Apollo / Relay clients see correct
			// `hasPreviousPage` / `hasNextPage` (best-effort logic of "cursor
			// was supplied" was wrong on the actual last/first page). The probe
			// is skipped when no opposite-side cursor was supplied — that
			// implies a true boundary.
			// The opposite-direction probe issues a second `LIMIT 1` SELECT to
			// answer "are there records on the other side of the cursor?" for
			// `hasPreviousPage` (forward) / `hasNextPage` (backward). Skip it
			// entirely when no cursor was supplied (boundary is implicit), or
			// when the client did not select the corresponding pageInfo field
			// — the probe's only consumer.
			let needs_opposite_probe =
				decoded_cursor.is_some() && connection_selects_opposite_page_flag(&ctx, backward);
			let opposite_has_records = if needs_opposite_probe {
				let rid = decoded_cursor.expect("guarded by needs_opposite_probe");
				let probe_op = if backward {
					// backward query asked `id < before`; opposite side is
					// `id >= before` (records forward of the cursor).
					BinaryOperator::MoreThanEqual
				} else {
					// forward query asked `id > after`; opposite side is
					// `id <= after` (records backward of the cursor).
					BinaryOperator::LessThanEqual
				};
				let probe_extra = Expr::Binary {
					left: Box::new(Expr::Idiom(Idiom::field("id".to_string()))),
					op: probe_op,
					right: Box::new(Value::RecordId(rid).into_literal()),
				};
				let probe_cond = Some(match cond_without_cursor {
					Some(Cond(prev)) => Cond(Expr::Binary {
						left: Box::new(prev),
						op: BinaryOperator::And,
						right: Box::new(probe_extra),
					}),
					None => Cond(probe_extra),
				});
				let probe_limit = Some(Limit(Expr::Literal(Literal::Integer(1))));
				let probe_stmt = select_all_from_table(
					Expr::Table(tb_name.clone()),
					probe_cond,
					None,
					probe_limit,
					None,
					&version,
				);
				let probe_res = execute_select(&kvs, sess, probe_stmt).await?;
				matches!(&probe_res, Value::Array(a) if !a.0.is_empty())
			} else {
				false
			};
			let (has_next_page, has_previous_page) = if backward {
				(opposite_has_records, has_more)
			} else {
				(has_more, opposite_has_records)
			};
			let conn = ConnectionValue {
				edges,
				page_info: PageInfoValue {
					has_next_page,
					has_previous_page,
					start_cursor,
					end_cursor,
				},
				total_count_query,
			};
			Ok(Some(FieldValue::owned_any(conn)))
		})
	})
	.description(format!(
		"Cursor-paginated `{tb_name_str}` list. Forward: pass `first` and `after` (from \
		 `pageInfo.endCursor`). Backward: pass `last` and `before` (from `pageInfo.startCursor`). \
		 Iterates by record id; use the non-connection list query for a custom sort key. \
		 `totalCount` runs a separate `count()` query on demand."
	))
	.argument(InputValue::new("first", TypeRef::named(TypeRef::INT)))
	.argument(InputValue::new("after", TypeRef::named(TypeRef::STRING)))
	.argument(InputValue::new("last", TypeRef::named(TypeRef::INT)))
	.argument(InputValue::new("before", TypeRef::named(TypeRef::STRING)))
	.argument(InputValue::new("filter", TypeRef::named(&table_filter_name)))
	.argument(InputValue::new("where", TypeRef::named(&table_filter_name)))
	.argument(InputValue::new("version", TypeRef::named(TypeRef::STRING)))
}
