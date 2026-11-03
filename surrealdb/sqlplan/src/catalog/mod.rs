//! Narrow catalog abstraction for the SQL binder.

use std::collections::HashMap;

use surrealdb_expr::expr::{Expr, Kind};

use crate::declared_type::DeclaredSqlType;

/// Information about a single column in a table schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
	/// Column name.
	pub name: String,
	/// 0-based ordinal index of column within table.
	pub ordinal: usize,
	/// Optional type name/kind description.
	pub data_type: Option<String>,
	/// Whether column is nullable.
	pub is_nullable: bool,
	/// The sequence that numbers the column when a write gives it no value, as `pg_sequences`
	/// lists it.
	pub sequence: Option<String>,
	/// The enum whose labels the column holds, or a set or an array of them, which the system
	/// views name as the column's type.
	pub enum_held: Option<EnumColumn>,
	/// The user-defined domain the column is declared with. `data_type` then names the base type
	/// of the domain, which is the type a client sees for the column.
	pub domain: Option<String>,
	/// The user-defined composite type the column is declared with, when it is one.
	pub composite: Option<String>,
	/// The SQL type the column is declared with (`SQL TYPE`), which the system views report in
	/// place of the type its kind maps to.
	pub declared: Option<DeclaredSqlType>,
}

impl ColumnInfo {
	pub fn new(name: impl Into<String>, ordinal: usize) -> Self {
		Self {
			name: name.into(),
			ordinal,
			data_type: None,
			is_nullable: true,
			sequence: None,
			enum_held: None,
			domain: None,
			composite: None,
			declared: None,
		}
	}

	/// This column, declared with the SQL type `declared`.
	pub fn with_declared(mut self, declared: DeclaredSqlType) -> Self {
		self.declared = Some(declared);
		self
	}

	/// This column, declared with the user-defined domain `name`.
	pub fn with_domain(mut self, name: impl Into<String>) -> Self {
		self.domain = Some(name.into());
		self
	}

	/// This column, declared with the user-defined composite type `name`.
	pub fn with_composite(mut self, name: impl Into<String>) -> Self {
		self.composite = Some(name.into());
		self
	}

	/// This column, holding the labels of the enum `held`.
	pub fn with_enum(mut self, held: EnumColumn) -> Self {
		self.enum_held = Some(held);
		self
	}

	pub fn with_sequence(mut self, sequence: impl Into<String>) -> Self {
		self.sequence = Some(sequence.into());
		self
	}

	pub fn with_type(mut self, data_type: impl Into<String>) -> Self {
		self.data_type = Some(data_type.into());
		self
	}
}

/// An index of a table, as the system views list it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInfo {
	/// Index name.
	pub name: String,
	/// The indexed columns, in index order.
	pub columns: Vec<String>,
	/// Whether the index rejects duplicate entries.
	pub unique: bool,
}

/// What happens to a referencing row when the row it references is deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferentialAction {
	/// The delete is refused.
	NoAction,
	/// The referencing row is deleted too.
	Cascade,
	/// The referencing column is set to `NONE`.
	SetNull,
}

impl ReferentialAction {
	/// The `pg_constraint.confdeltype` code of the action.
	pub const fn pg_code(self) -> char {
		match self {
			Self::NoAction => 'a',
			Self::Cascade => 'c',
			Self::SetNull => 'n',
		}
	}

	/// The action as `pg_get_constraintdef` words it.
	pub const fn sql(self) -> &'static str {
		match self {
			Self::NoAction => "NO ACTION",
			Self::Cascade => "CASCADE",
			Self::SetNull => "SET NULL",
		}
	}
}

/// A column that holds record links to the primary key (`id`) of another table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyInfo {
	/// The referencing column.
	pub column: String,
	/// The referenced table.
	pub target_table: String,
	/// What deleting the referenced row does to this one.
	pub on_delete: ReferentialAction,
}

/// Metadata about a table or view schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
	/// Table name.
	pub name: String,
	/// Optional schema namespace (e.g. `public`).
	pub schema: Option<String>,
	/// Ordered list of columns.
	pub columns: Vec<ColumnInfo>,
	/// The indexes the system views list for the table.
	pub indexes: Vec<IndexInfo>,
	/// The columns that reference another table, listed as foreign keys.
	pub foreign_keys: Vec<ForeignKeyInfo>,
	/// Whether `columns` is every column the table has, so that a name outside it is an error.
	/// A table whose list is a subset of what a client may read (a system catalog table) or that
	/// declares no columns (a schemaless table) is not closed.
	pub closed: bool,
	/// The tables a view reads, empty for a table that is not a view. A table cannot be removed
	/// while a view reads it.
	pub view_over: Vec<String>,
}

impl TableSchema {
	pub fn new(name: impl Into<String>, columns: Vec<ColumnInfo>) -> Self {
		Self {
			name: name.into(),
			schema: None,
			columns,
			indexes: Vec::new(),
			foreign_keys: Vec::new(),
			closed: false,
			view_over: Vec::new(),
		}
	}

	pub fn with_view_over(mut self, tables: Vec<String>) -> Self {
		self.view_over = tables;
		self
	}

	pub fn with_indexes(mut self, indexes: Vec<IndexInfo>) -> Self {
		self.indexes = indexes;
		self
	}

	pub fn with_foreign_keys(mut self, foreign_keys: Vec<ForeignKeyInfo>) -> Self {
		self.foreign_keys = foreign_keys;
		self
	}

	/// Declare `columns` to be every column the table has.
	pub fn as_closed(mut self) -> Self {
		self.closed = true;
		self
	}

	pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
		self.schema = Some(schema.into());
		self
	}

	/// Find a column by case-insensitive or exact name.
	pub fn find_column(&self, name: &str, case_insensitive: bool) -> Option<&ColumnInfo> {
		if case_insensitive {
			self.columns.iter().find(|c| c.name.eq_ignore_ascii_case(name))
		} else {
			self.columns.iter().find(|c| c.name == name)
		}
	}

	/// Get column names in ordinal order.
	pub fn column_names(&self) -> Vec<String> {
		self.columns.iter().map(|c| c.name.clone()).collect()
	}
}

pub mod relations;
pub mod staged;
pub mod system;

/// The assertion of a column, as far as a statement that changes the column's type needs to read
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnAssert {
	/// The column has no assertion.
	None,
	/// The assertion is the `NOT NULL` one the SQL DDL defines (`$value != NONE`).
	NotNull,
	/// Any other assertion, written in SurrealQL.
	Other,
}

/// How a column is declared: what `ALTER TABLE` must know to give the column a new type.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnShape {
	/// The declared type, `None` for a column that declares none.
	pub kind: Option<Kind>,
	/// The assertion of the column.
	pub assert: ColumnAssert,
	/// Whether the column has a `DEFAULT`.
	pub has_default: bool,
}

impl ColumnShape {
	/// The shape of a column declared as `kind` with `assert` and a `DEFAULT` when `has_default`.
	pub fn new(kind: Option<Kind>, assert: Option<&Expr>, has_default: bool) -> Self {
		let assert = match assert {
			None => ColumnAssert::None,
			Some(expr) if crate::lower::ddl::is_not_null_assertion(expr) => ColumnAssert::NotNull,
			Some(_) => ColumnAssert::Other,
		};
		Self {
			kind,
			assert,
			has_default,
		}
	}
}

/// The table that a field of this type holds links to, when the type names exactly one table and
/// admits nothing but a link besides `NONE` and `NULL`: `record<t>`, `option<record<t>>` and
/// `record<t> | none`.
pub fn link_target(kind: &Kind) -> Option<&str> {
	match kind {
		Kind::Record(tables) => match tables.as_slice() {
			[table] => Some(table.as_str()),
			_ => None,
		},
		Kind::Either(kinds) => {
			let mut target = None;
			for kind in kinds {
				if matches!(kind, Kind::None | Kind::Null) {
					continue;
				}
				if target.is_some() {
					return None;
				}
				target = Some(link_target(kind)?);
			}
			target
		}
		Kind::Any
		| Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Duration
		| Kind::Float
		| Kind::F16
		| Kind::F32
		| Kind::Int
		| Kind::I8
		| Kind::I16
		| Kind::I32
		| Kind::U8
		| Kind::U16
		| Kind::U32
		| Kind::U64
		| Kind::Number
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Geometry(_)
		| Kind::Set(..)
		| Kind::Array(..)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_)
		| Kind::Enum(_)
		| Kind::Named(_) => None,
	}
}

/// The `information_schema.columns.data_type` of a column declared as `kind` where it has one
/// wire type: `None` for a type with no single one (a record link, a mixed union, `any`).
/// Optionality does not change it, and an array is its element's name followed by `[]`.
pub(crate) fn kind_type_name(kind: &Kind) -> Option<String> {
	let name = match kind {
		Kind::Bool => "boolean",
		Kind::Bytes => "bytea",
		Kind::Datetime => "timestamp with time zone",
		Kind::Date => "date",
		Kind::Time => "time without time zone",
		Kind::Timestamp => "timestamp without time zone",
		Kind::Interval => "interval",
		Kind::Decimal | Kind::BigDecimal | Kind::U64 => "numeric",
		Kind::Duration => "interval",
		Kind::Float => "double precision",
		Kind::F16 | Kind::F32 => "real",
		Kind::Int | Kind::U32 => "bigint",
		Kind::I8 | Kind::I16 | Kind::U8 => "smallint",
		Kind::I32 | Kind::U16 => "integer",
		Kind::String => "text",
		Kind::Uuid => "uuid",
		Kind::Object => "jsonb",
		Kind::Named(_) => "USER-DEFINED",
		Kind::Enum(_) => "enum",
		// Every dimension of an array is named alike, as PostgreSQL names them.
		Kind::Array(element, _) => {
			let mut scalar = element.as_ref();
			while let Kind::Array(inner, _) = scalar {
				scalar = inner.as_ref();
			}
			return kind_type_name(scalar).map(|name| format!("{name}[]"));
		}
		Kind::Either(kinds) => {
			let mut names = kinds
				.iter()
				.filter(|kind| !matches!(kind, Kind::None | Kind::Null))
				.map(kind_type_name);
			let first = names.next()??;
			return names.all(|name| name.as_deref() == Some(first.as_str())).then_some(first);
		}
		Kind::Any
		| Kind::None
		| Kind::Null
		| Kind::Number
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Set(..)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_) => return None,
	};
	Some(name.to_owned())
}

/// Whether a column of `kind` can hold `NONE`.
pub fn kind_admits_none(kind: &Kind) -> bool {
	if let Kind::Either(kinds) = kind {
		kinds.iter().any(kind_admits_none)
	} else {
		matches!(kind, Kind::Any | Kind::None)
	}
}

/// How a column holds the labels of an enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EnumShape {
	/// One label (`enum<..>`, optionally `NONE`).
	Scalar,
	/// A set of labels (`set<enum<..>>`), which MySQL `SET` columns are.
	Set,
	/// A list of labels (`array<enum<..>>`).
	Array,
}

/// The enum whose labels a column holds: a type of the database whose body is a union of labels,
/// by name, or a column-scoped enum, its labels inline.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EnumRef {
	/// A type of the database whose body is a union of labels.
	Named(String),
	/// A column-scoped enum, its labels in declaration order.
	Inline(Vec<String>),
}

/// A column that holds the labels of an enum, as its declared kind says.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EnumColumn {
	/// The enum the labels belong to.
	pub reference: EnumRef,
	/// Whether the column holds one label, a set of them or a list.
	pub shape: EnumShape,
}

impl EnumColumn {
	/// The enum a field of `kind` holds labels of: the enum itself, a set or an array of it, any
	/// of them besides `NONE` and `NULL`. A named type is an enum when `is_enum` says it is one.
	/// `None` for every other kind, including a union of several enums.
	pub fn of_kind(kind: &Kind, is_enum: &dyn Fn(&str) -> bool) -> Option<Self> {
		let reference = |kind: &Kind| {
			if let Kind::Enum(labels) = kind {
				Some(EnumRef::Inline(labels.clone()))
			} else if let Kind::Named(name) = kind
				&& is_enum(name)
			{
				Some(EnumRef::Named(name.clone()))
			} else {
				None
			}
		};
		match kind {
			Kind::Enum(_) | Kind::Named(_) => Some(Self {
				reference: reference(kind)?,
				shape: EnumShape::Scalar,
			}),
			Kind::Set(element, _) | Kind::Array(element, _) => {
				let shape = if matches!(kind, Kind::Set(..)) {
					EnumShape::Set
				} else {
					EnumShape::Array
				};
				Some(Self {
					reference: reference(element)?,
					shape,
				})
			}
			Kind::Either(kinds) => {
				let mut found = None;
				for kind in kinds {
					if matches!(kind, Kind::None | Kind::Null) {
						continue;
					}
					if found.is_some() {
						return None;
					}
					found = Some(Self::of_kind(kind, is_enum)?);
				}
				found
			}
			Kind::Any
			| Kind::None
			| Kind::Null
			| Kind::Bool
			| Kind::Bytes
			| Kind::Datetime
			| Kind::Date
			| Kind::Time
			| Kind::Timestamp
			| Kind::Interval
			| Kind::Decimal
			| Kind::BigDecimal
			| Kind::Duration
			| Kind::Float
			| Kind::F16
			| Kind::F32
			| Kind::Int
			| Kind::I8
			| Kind::I16
			| Kind::I32
			| Kind::U8
			| Kind::U16
			| Kind::U32
			| Kind::U64
			| Kind::Number
			| Kind::Object
			| Kind::String
			| Kind::Uuid
			| Kind::Regex
			| Kind::Table(_)
			| Kind::Record(_)
			| Kind::Geometry(_)
			| Kind::Function(..)
			| Kind::Range
			| Kind::Literal(_)
			| Kind::File(_) => None,
		}
	}
}

/// A user-defined enum type: its name, its database-scoped identity, and its labels with the sort
/// key that places each in declaration order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumType {
	/// The type's name.
	pub name: String,
	/// The id the type's OID is allocated from.
	pub id: u32,
	/// The labels in declaration order.
	pub labels: Vec<EnumTypeLabel>,
}

/// A label of an [`EnumType`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumTypeLabel {
	/// The label's text.
	pub label: String,
	/// The sort key that places it in declaration order; a re-spacing rewrites it.
	pub order: u64,
	/// The identity the label keeps through a rename or a re-spacing, which its `pg_enum` OID is
	/// derived from. Labels declared together take 1, 2, ...; an added label takes one past the
	/// greatest, as the catalog does.
	pub ident: u32,
}

impl EnumType {
	/// The gap between the sort keys of labels declared together.
	const ORDER_STRIDE: u64 = 1 << 32;

	/// An enum of `labels`, declared together, with the id `id`.
	pub fn declared(name: &str, id: u32, labels: &[String]) -> Self {
		Self {
			name: name.to_owned(),
			id,
			labels: labels
				.iter()
				.zip(1..)
				.map(|(label, ident)| EnumTypeLabel {
					label: label.clone(),
					order: u64::from(ident) * Self::ORDER_STRIDE,
					ident,
				})
				.collect(),
		}
	}

	/// Places `label` before or after `anchor` (last when there is none), taking the midpoint of
	/// its neighbours' sort keys, as the catalog does. Does nothing for a label the enum holds or
	/// an anchor it does not.
	pub fn add_label(&mut self, label: &str, anchor: Option<(&str, bool)>) {
		if self.labels.iter().any(|l| l.label == label) {
			return;
		}
		let index = match anchor {
			None => self.labels.len(),
			Some((anchor, after)) => match self.labels.iter().position(|l| l.label == anchor) {
				Some(at) => at + usize::from(after),
				None => return,
			},
		};
		let ident = self.labels.iter().map(|l| l.ident).max().unwrap_or(0).saturating_add(1);
		let below = index.checked_sub(1).map(|i| self.labels[i].order).unwrap_or(0);
		let order = match self.labels.get(index) {
			Some(above) if above.order - below >= 2 => below + (above.order - below) / 2,
			Some(_) => {
				for (position, existing) in self.labels.iter_mut().enumerate() {
					existing.order = (position as u64 + 1) * Self::ORDER_STRIDE;
				}
				let below = index.checked_sub(1).map(|i| self.labels[i].order).unwrap_or(0);
				match self.labels.get(index) {
					Some(above) => below + (above.order - below) / 2,
					None => below + Self::ORDER_STRIDE,
				}
			}
			None => below + Self::ORDER_STRIDE,
		};
		self.labels.insert(
			index,
			EnumTypeLabel {
				label: label.to_owned(),
				order,
				ident,
			},
		);
	}

	/// Renames `from` to `to`, keeping its place and its identity.
	pub fn rename_label(&mut self, from: &str, to: &str) {
		if self.labels.iter().any(|l| l.label == to) {
			return;
		}
		if let Some(existing) = self.labels.iter_mut().find(|l| l.label == from) {
			to.clone_into(&mut existing.label);
		}
	}
}

/// A user-defined domain: a base type with the `NOT NULL` and `DEFAULT` a column of the domain
/// takes, and the checks every value it holds must pass.
#[derive(Debug, Clone, PartialEq)]
pub struct DomainType {
	/// The domain's name.
	pub name: String,
	/// The id the type's OID is allocated from, below [`crate::descriptor::USER_TYPE_ID_LIMIT`].
	pub id: u32,
	/// The kind of the base type. A column of the domain holds values of this kind.
	pub base: Kind,
	/// The base type as the domain declares it, in the canonical text of a PostgreSQL declared
	/// type.
	pub base_type: String,
	/// Whether a column of the domain refuses NULL.
	pub not_null: bool,
	/// The value a column of the domain takes when a write names none.
	pub default: Option<Expr>,
	/// The checks, in declaration order. Each reads the value under test as `$value`.
	pub checks: Vec<DomainCheck>,
}

/// One `CHECK` of a domain, with the name given by `CONSTRAINT`, when one was.
#[derive(Debug, Clone, PartialEq)]
pub struct DomainCheck {
	/// The constraint name, when declared.
	pub name: Option<String>,
	/// The check, as a SurrealQL expression over `$value`. It holds when it evaluates to true.
	pub check: Expr,
}

/// A user-defined composite type: its attributes in declaration order, each with its kind.
#[derive(Debug, Clone, PartialEq)]
pub struct CompositeType {
	/// The type's name.
	pub name: String,
	/// The id the type's OIDs are allocated from, below [`crate::descriptor::USER_TYPE_ID_LIMIT`].
	pub id: u32,
	/// The attributes in declaration order, with the kind each holds.
	pub fields: Vec<(String, Kind)>,
	/// The SQL type each attribute was declared with, for the attributes declared with a type the
	/// front end reads (an enum attribute is read through its kind).
	pub declared: Vec<(String, DeclaredSqlType)>,
}

impl CompositeType {
	/// The SQL type `attribute` was declared with, if it was declared with one.
	pub fn declared_type(&self, attribute: &str) -> Option<&DeclaredSqlType> {
		self.declared.iter().find(|(name, _)| name == attribute).map(|(_, declared)| declared)
	}

	/// The SQL type the values of `attribute`, of `kind`, travel as: the wire type of its
	/// declared type ([`crate::reported_type::pg_wire_type`]), or the type its kind reads as.
	pub fn attribute_sql_type(&self, attribute: &str, kind: &Kind) -> crate::SqlType {
		if let Some(sql_type) =
			self.declared_type(attribute).and_then(crate::reported_type::pg_wire_type)
		{
			return sql_type;
		}
		crate::SqlType::from_name(&kind_type_name(kind).unwrap_or_else(|| "text".to_owned()))
	}
}

/// What the PostgreSQL front end reads a type of the database as.
#[derive(Debug, Clone, PartialEq)]
pub enum UserType {
	/// A type whose body is a union of string labels.
	Enum(EnumType),
	/// A type whose body is an object literal: the shape of a composite type.
	Composite(CompositeType),
	/// Any other type, read as a domain over its body.
	Domain(DomainType),
}

impl UserType {
	/// The type `name`, with the id `id`, defined with `body`, `asserts`, `default` and
	/// `sql_type`. An enum's labels are numbered as labels declared together are.
	pub fn of_definition(
		name: &str,
		id: u32,
		body: &Kind,
		asserts: &[surrealdb_expr::expr::statements::define::TypeAssert],
		default: Option<&Expr>,
		sql_type: Option<&str>,
	) -> Self {
		use surrealdb_expr::expr::KindLiteral;
		if let Some(labels) = surrealdb_expr::expr::statements::define::union_labels(body) {
			return UserType::Enum(EnumType::declared(name, id, &labels));
		}
		if let Kind::Literal(KindLiteral::Object(fields)) = body {
			let declared = sql_type.map(composite_attributes).unwrap_or_default();
			let mut ordered: Vec<(String, Kind)> = declared
				.iter()
				.filter_map(|(attribute, _)| {
					fields.get(attribute.as_str()).map(|kind| (attribute.clone(), kind.clone()))
				})
				.collect();
			for (field, kind) in fields {
				if !ordered.iter().any(|(name, _)| name == field.as_str()) {
					ordered.push((field.as_str().to_owned(), kind.clone()));
				}
			}
			let declared = declared
				.into_iter()
				.filter_map(|(attribute, text)| {
					DeclaredSqlType::parse(crate::SqlTypeDialect::PostgreSql, &text)
						.ok()
						.map(|declared| (attribute, declared))
				})
				.collect();
			return UserType::Composite(CompositeType {
				name: name.to_owned(),
				id,
				fields: ordered,
				declared,
			});
		}
		let members = if let Kind::Either(members) = body {
			members.clone()
		} else {
			vec![body.clone()]
		};
		// A domain whose base admits NULL states `NOT NULL` as an assertion.
		let asserts_not_null =
			asserts.iter().any(|assert| crate::lower::ddl::is_not_null_assertion(&assert.check));
		let not_null = asserts_not_null
			|| !members.iter().any(|kind| matches!(kind, Kind::None | Kind::Null | Kind::Any));
		let base = Kind::either(
			members.into_iter().filter(|kind| !matches!(kind, Kind::None | Kind::Null)).collect(),
		);
		let base_type = sql_type
			.map(str::to_owned)
			.or_else(|| kind_type_name(&base))
			.unwrap_or_else(|| "text".to_owned());
		UserType::Domain(DomainType {
			name: name.to_owned(),
			id,
			base,
			base_type,
			not_null,
			// A domain stated with no default holds the NULL PostgreSQL gives an omitted column.
			default: default
				.filter(|expr| !matches!(expr, Expr::Literal(surrealdb_expr::expr::Literal::Null)))
				.cloned(),
			checks: asserts
				.iter()
				.filter(|assert| !crate::lower::ddl::is_not_null_assertion(&assert.check))
				.map(|assert| DomainCheck {
					name: assert.name.clone(),
					check: domain_check_of_assert(&assert.check),
				})
				.collect(),
		})
	}
}

/// The check a domain's `ASSERT` states: `check` of `check != false`, or the assertion itself
/// when it is not of that shape.
pub fn domain_check_of_assert(assert: &Expr) -> Expr {
	use surrealdb_expr::expr::{BinaryOperator, Literal};
	if let Expr::Binary {
		left,
		op: BinaryOperator::NotEqual,
		right,
	} = assert
		&& matches!(**right, Expr::Literal(Literal::Bool(false)))
	{
		return (**left).clone();
	}
	assert.clone()
}

/// The attributes of a composite type in declaration order, each with its declared type, read
/// from the `SQL TYPE POSTGRESQL '(attr type, …)'` its definition carries. An attribute name holds
/// no space, and a comma inside parentheses belongs to the type it is part of.
pub fn composite_attributes(sql_type: &str) -> Vec<(String, String)> {
	let Some(inner) = sql_type.trim().strip_prefix('(').and_then(|s| s.strip_suffix(')')) else {
		return Vec::new();
	};
	let mut attributes = Vec::new();
	let mut depth = 0usize;
	let mut start = 0usize;
	let mut push = |part: &str| {
		if let Some((name, ty)) = part.trim().split_once(char::is_whitespace) {
			attributes.push((name.to_owned(), ty.trim().to_owned()));
		}
	};
	for (i, c) in inner.char_indices() {
		match c {
			'(' => depth += 1,
			')' => depth = depth.saturating_sub(1),
			',' if depth == 0 => {
				push(&inner[start..i]);
				start = i + 1;
			}
			_ => {}
		}
	}
	push(&inner[start..]);
	attributes
}

/// The user-defined types that the catalog views list: enums, domains and composite types.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct UserTypes {
	/// The enum types, with the sort key of each label.
	pub enums: Vec<EnumType>,
	/// The domains.
	pub domains: Vec<DomainType>,
	/// The composite types.
	pub composites: Vec<CompositeType>,
}

impl UserTypes {
	/// Every user-defined type `catalog` describes.
	pub fn of_catalog(catalog: &dyn BinderCatalog) -> Self {
		Self {
			enums: catalog.enum_types(),
			domains: catalog.domain_types(),
			composites: catalog.composite_types(),
		}
	}
}

/// Whether a column of `kind` can hold `NULL`: its type is `any`, `null`, or has a member that is.
pub fn kind_admits_null(kind: &Kind) -> bool {
	if let Kind::Either(kinds) = kind {
		kinds.iter().any(kind_admits_null)
	} else {
		matches!(kind, Kind::Any | Kind::Null)
	}
}

/// Narrow catalog trait required by the binder to resolve table and column names.
pub trait BinderCatalog: Send + Sync {
	/// Look up a table schema by optional namespace and table name.
	fn get_table(&self, schema: Option<&str>, name: &str) -> Option<TableSchema>;

	/// The user-defined enum type `name`, for a column declared with it.
	fn enum_type(&self, _name: &str) -> Option<EnumType> {
		None
	}

	/// Every user-defined enum type, for the system views that list them.
	fn enum_types(&self) -> Vec<EnumType> {
		Vec::new()
	}

	/// The user-defined domain `name`, for a column declared with it.
	fn domain_type(&self, _name: &str) -> Option<DomainType> {
		None
	}

	/// The user-defined composite type `name`, for a column declared with it.
	fn composite_type(&self, _name: &str) -> Option<CompositeType> {
		None
	}

	/// Every user-defined domain, for the system views that list them.
	fn domain_types(&self) -> Vec<DomainType> {
		Vec::new()
	}

	/// Every user-defined composite type, for the system views that list them.
	fn composite_types(&self) -> Vec<CompositeType> {
		Vec::new()
	}

	/// Check if a table exists.
	fn table_exists(&self, schema: Option<&str>, name: &str) -> bool {
		self.get_table(schema, name).is_some()
	}

	/// Current active database name, if known.
	fn current_database(&self) -> Option<&str> {
		None
	}

	/// Return all user-defined table schemas in the catalog.
	fn all_tables(&self) -> Vec<TableSchema> {
		Vec::new()
	}

	/// The tables a statement resolves constraint names and serial columns against: `ALTER TABLE`
	/// constraints, `pg_get_constraintdef` and `pg_get_serial_sequence`. The same as
	/// [`BinderCatalog::all_tables`] unless the catalog says otherwise, so that a catalog that
	/// records what is asked of it can tell this question from the binder's.
	fn constraint_tables(&self) -> Vec<TableSchema> {
		self.all_tables()
	}

	/// Every column of `table` whose type is declared, in no particular order. A schemaless
	/// table may declare only some of its columns, so this is not the table's column list.
	fn declared_column_types(
		&self,
		_schema: Option<&str>,
		_table: &str,
	) -> Vec<(String, crate::SqlType)> {
		Vec::new()
	}

	/// The SQL type column `column` of table `table` is declared with (`SQL TYPE`), which the wire
	/// reports for the column in place of the type its kind maps to. `None` for a column declared
	/// without one, and for a table or column the catalog does not describe. Answered from the
	/// column's description in [`BinderCatalog::get_table`] by default.
	fn column_sql_type(
		&self,
		schema: Option<&str>,
		table: &str,
		column: &str,
	) -> Option<DeclaredSqlType> {
		self.get_table(schema, table)
			.and_then(|table| table.find_column(column, false).and_then(|c| c.declared.clone()))
	}

	/// Every column of `table` declared with a SQL type, in no particular order. As for
	/// [`BinderCatalog::declared_column_types`], this is not the table's column list.
	fn declared_sql_types(
		&self,
		schema: Option<&str>,
		table: &str,
	) -> Vec<(String, DeclaredSqlType)> {
		self.get_table(schema, table)
			.map(|table| {
				table
					.columns
					.into_iter()
					.filter_map(|column| column.declared.map(|declared| (column.name, declared)))
					.collect()
			})
			.unwrap_or_default()
	}

	/// The declared type of column `column` of table `table` when a text value written to it is
	/// read as that type: the integer, float, `numeric`, boolean, timestamp, `uuid` and `json`
	/// types. [`crate::SqlType::Unknown`] for every other column, and for a column the catalog does
	/// not describe.
	///
	/// The table and the column are named as the lowering names them, and are compared exactly.
	fn write_type(&self, _schema: Option<&str>, _table: &str, _column: &str) -> crate::SqlType {
		crate::SqlType::Unknown
	}

	/// A declared column type, independent of whether the table's columns can be enumerated.
	fn column_type(&self, schema: Option<&str>, table: &str, column: &str) -> crate::SqlType {
		self.get_table(schema, table)
			.and_then(|table| {
				table.find_column(column, false).and_then(|column| column.data_type.clone())
			})
			.map(|name| crate::SqlType::from_name(&name))
			.unwrap_or(crate::SqlType::Unknown)
	}

	/// Whether this catalog only records what a lowering asks of it and describes no table, so
	/// that its silence says nothing about the schema. A lowering asks this exactly where it would
	/// refuse a statement for what the schema lacks (a column that is not a link); on `true` it
	/// does not refuse and returns the statements it would run if the schema allowed them, and
	/// the caller lowers again against the schema.
	fn defers_schema_checks(&self) -> bool {
		false
	}

	/// The table that column `column` of table `table` holds record links to.
	///
	/// A column holds links to one table when its declared type names exactly that table:
	/// `record<t>`, `option<record<t>>` or `record<t> | none`. The answer is `None` for a table or
	/// a column the catalog does not describe, for a `record` of several tables or of any table,
	/// and for every other type, including a collection of links.
	///
	/// The table and the column are named as the lowering names them, which is the name of the
	/// stored field, and are compared exactly: SurrealDB field names are case-sensitive, so a field
	/// that differs from a link column only in case is not a link.
	///
	/// The binder and the write lowering ask for each column they resolve against a table (a column
	/// may be asked about more than once), never for the columns of a table as a whole, so an
	/// implementation may answer from the definition of the one column. The answer does not change
	/// what [`BinderCatalog::get_table`] returns: a table that only has link columns described is
	/// still an undescribed table.
	fn link_table(&self, _schema: Option<&str>, _table: &str, _column: &str) -> Option<String> {
		None
	}

	/// Whether the link column `column` of table `table` admits `NONE` (`option<record<t>>`, or a
	/// union with `none`), so that SQL `NULL` written to it is stored as `NONE`. A link column that
	/// does not refuses `NULL`. `false` where the catalog does not say.
	fn link_admits_none(&self, _schema: Option<&str>, _table: &str, _column: &str) -> bool {
		false
	}

	/// The enum that column `column` of table `table` holds labels of, when its declared type is
	/// an enum, or a set or an array of one. `None` for every other column and for a table or a
	/// column the catalog does not describe. The names are compared exactly, as for
	/// [`BinderCatalog::link_table`].
	fn column_enum(
		&self,
		_schema: Option<&str>,
		_table: &str,
		_column: &str,
	) -> Option<EnumColumn> {
		None
	}

	/// The user-defined composite type that column `column` of table `table` is declared with, by
	/// name, when it is one. Answered from the column's description in [`BinderCatalog::get_table`]
	/// by default.
	fn column_composite(&self, schema: Option<&str>, table: &str, column: &str) -> Option<String> {
		self.get_table(schema, table)
			.and_then(|table| table.find_column(column, false).and_then(|c| c.composite.clone()))
	}

	/// The columns of table `table` that hold NULL in a row written without a value for them: the
	/// columns the table declares whose type admits NULL and that have none of a `DEFAULT`, a
	/// `VALUE` and a `COMPUTED` clause. A column left out of an `INSERT` is stored as `NONE`
	/// unless the write names it, and `NONE` and `NULL` are two values to a grouping, a
	/// `DISTINCT` and a set operation, so an `INSERT` names each of these as `NULL`.
	///
	/// Empty for a table the catalog does not describe and for one that does not declare all the
	/// columns it has, where the columns that are left out are not known. The table is named as the
	/// lowering names it and compared exactly.
	fn null_columns(&self, _schema: Option<&str>, _table: &str) -> Vec<String> {
		Vec::new()
	}

	/// The columns of table `table` that take a default in a row written without a value for them,
	/// with the expression that gives it: the columns the table declares with a `DEFAULT` clause
	/// and without a `VALUE` or a `COMPUTED` one. A `REPLACE` over an existing row writes the row
	/// as a new one, so each of these is set to its default, as each of
	/// [`null_columns`](Self::null_columns) is set to `NULL`, where an `INSERT` of a new row
	/// leaves the defaults to the table.
	///
	/// Empty where [`null_columns`](Self::null_columns) is, and compared as it is.
	fn default_columns(&self, _schema: Option<&str>, _table: &str) -> Vec<(String, Expr)> {
		Vec::new()
	}

	/// How column `column` of table `table` is declared, for the statements that give a column a
	/// new type (`ALTER TABLE ... ADD FOREIGN KEY` over a scalar column). The answer is `None` for
	/// a table or column the catalog does not describe.
	///
	/// The names are compared exactly, as for [`BinderCatalog::link_table`].
	fn column_shape(
		&self,
		_schema: Option<&str>,
		_table: &str,
		_column: &str,
	) -> Option<ColumnShape> {
		None
	}

	/// The number of dimensions of column `column` of table `table` when it holds vectors
	/// (`vector(n)`, which the engine stores as `array<float, n>`), and `None` for a table or
	/// column the catalog does not describe and for every other type.
	///
	/// The table and the column are named as the lowering names them, and are compared exactly.
	fn vector_dimension(
		&self,
		_schema: Option<&str>,
		_table: &str,
		_column: &str,
	) -> Option<usize> {
		None
	}

	/// The HNSW indexes of table `table`, as the column each covers and the metric it ranks by.
	/// An index that is being removed is not one.
	fn hnsw_indexes(
		&self,
		_schema: Option<&str>,
		_table: &str,
	) -> Vec<(String, crate::vector::VectorMetric)> {
		Vec::new()
	}
}

/// An empty or schemaless catalog.
///
/// In SurrealDB, tables can be schemaless, where columns are not predefined.
/// Returning `None` signals to the binder that column resolution should proceed
/// flexibly without failing early on non-predefined column names.
/// System catalog tables are still recognized.
#[derive(Debug, Default, Clone)]
pub struct EmptyCatalog;

impl BinderCatalog for EmptyCatalog {
	fn get_table(&self, schema: Option<&str>, name: &str) -> Option<TableSchema> {
		system::get_system_table_schema(schema, name)
	}
}

/// An in-memory catalog used for tests and static schema definitions.
#[derive(Debug, Default, Clone)]
pub struct MemoryCatalog {
	tables: HashMap<(Option<String>, String), TableSchema>,
	/// Tables that [`BinderCatalog::all_tables`] lists in the system views and
	/// [`BinderCatalog::get_table`] does not resolve, so a statement naming one binds against it
	/// as against an undescribed table.
	listed: Vec<TableSchema>,
	/// The link columns, by (schema, table, column), and the table each holds links to. Kept apart
	/// from `tables`: describing a link column does not describe its table.
	links: HashMap<(Option<String>, String, String), String>,
	current_db: Option<String>,
	column_types: HashMap<(String, String), crate::SqlType>,
	/// How columns are declared, by (table, column).
	shapes: HashMap<(String, String), ColumnShape>,
	/// The link columns that admit `NONE`, by (table, column).
	nullable_links: std::collections::HashSet<(String, String)>,
	/// The columns that hold NULL in a row written without them, by table.
	null_columns: HashMap<String, Vec<String>>,
	default_columns: HashMap<String, Vec<(String, Expr)>>,
	/// The dimensions of the vector columns, by (table, column).
	vectors: HashMap<(String, String), usize>,
	/// The types text is read as when written to a column, by (table, column).
	write_types: HashMap<(String, String), crate::SqlType>,
	/// The HNSW indexes, by table, as the column each covers and its metric.
	hnsw: HashMap<String, Vec<(String, crate::vector::VectorMetric)>>,
	/// The user-defined enum types.
	enums: Vec<EnumType>,
	/// The columns that hold enum labels, by (table, column).
	enum_columns: HashMap<(String, String), EnumColumn>,
	/// The user-defined domains.
	domains: Vec<DomainType>,
	/// The user-defined composite types.
	composites: Vec<CompositeType>,
	/// The SQL types columns are declared with, by (table, column).
	sql_types: HashMap<(String, String), DeclaredSqlType>,
}

impl MemoryCatalog {
	/// Declare that column `column` of table `table` holds the labels of an enum; see
	/// [`BinderCatalog::column_enum`].
	pub fn with_enum_column(mut self, table: &str, column: &str, held: EnumColumn) -> Self {
		self.enum_columns.insert((table.to_owned(), column.to_owned()), held);
		self
	}

	/// This catalog with the user-defined enum types `enums`.
	pub fn with_enums(mut self, enums: Vec<EnumType>) -> Self {
		self.enums = enums;
		self
	}

	/// This catalog with the user-defined domains `domains`.
	pub fn with_domains(mut self, domains: Vec<DomainType>) -> Self {
		self.domains = domains;
		self
	}

	/// This catalog with the user-defined composite types `composites`.
	pub fn with_composites(mut self, composites: Vec<CompositeType>) -> Self {
		self.composites = composites;
		self
	}

	pub fn new() -> Self {
		Self {
			tables: HashMap::new(),
			listed: Vec::new(),
			links: HashMap::new(),
			current_db: None,
			column_types: HashMap::new(),
			shapes: HashMap::new(),
			nullable_links: std::collections::HashSet::new(),
			null_columns: HashMap::new(),
			default_columns: HashMap::new(),
			vectors: HashMap::new(),
			write_types: HashMap::new(),
			hnsw: HashMap::new(),
			enums: Vec::new(),
			enum_columns: HashMap::new(),
			domains: Vec::new(),
			composites: Vec::new(),
			sql_types: HashMap::new(),
		}
	}

	/// Declare that column `column` of table `table` is declared with the SQL type `declared`; see
	/// [`BinderCatalog::column_sql_type`].
	pub fn with_sql_type(mut self, table: &str, column: &str, declared: DeclaredSqlType) -> Self {
		self.add_sql_type(table, column, declared);
		self
	}

	/// Declare a column's SQL type, as [`Self::with_sql_type`] does.
	pub fn add_sql_type(
		&mut self,
		table: &str,
		column: &str,
		declared: DeclaredSqlType,
	) -> &mut Self {
		self.sql_types.insert((table.to_owned(), column.to_owned()), declared);
		self
	}

	/// Describe the vector columns that `other` does, besides what this catalog describes.
	pub fn with_vectors_of(mut self, other: &MemoryCatalog) -> Self {
		self.vectors.extend(other.vectors.iter().map(|(key, count)| (key.clone(), *count)));
		self
	}

	/// Forget the HNSW indexes, as a catalog does that cannot say which indexes a request leaves.
	pub fn clear_hnsw_indexes(&mut self) {
		self.hnsw.clear();
	}

	/// Declare that the link column `column` of table `table` admits `NONE`; see
	/// [`BinderCatalog::link_admits_none`].
	pub fn with_nullable_link(mut self, table: &str, column: &str) -> Self {
		self.nullable_links.insert((table.to_owned(), column.to_owned()));
		self
	}

	pub fn add_nullable_link(&mut self, table: &str, column: &str) -> &mut Self {
		self.nullable_links.insert((table.to_owned(), column.to_owned()));
		self
	}

	/// Declare the columns of table `table` that hold NULL in a row written without them; see
	/// [`BinderCatalog::null_columns`].
	pub fn add_null_columns(&mut self, table: &str, columns: Vec<String>) -> &mut Self {
		self.null_columns.insert(table.to_owned(), columns);
		self
	}

	/// Declare the NULL-holding columns of a table, as [`Self::add_null_columns`] does.
	pub fn with_null_columns(mut self, table: &str, columns: &[&str]) -> Self {
		self.add_null_columns(table, columns.iter().map(|column| (*column).to_owned()).collect());
		self
	}

	/// Declare the columns of table `table` that take a default in a row written without them,
	/// with their default expressions; see [`BinderCatalog::default_columns`].
	pub fn add_default_columns(&mut self, table: &str, columns: Vec<(String, Expr)>) -> &mut Self {
		self.default_columns.insert(table.to_owned(), columns);
		self
	}

	/// Declare the defaulted columns of a table, as [`Self::add_default_columns`] does.
	pub fn with_default_columns(mut self, table: &str, columns: &[(&str, Expr)]) -> Self {
		self.add_default_columns(
			table,
			columns
				.iter()
				.map(|(column, default)| ((*column).to_owned(), default.clone()))
				.collect(),
		);
		self
	}

	/// Declare how column `column` of table `table` is declared; see
	/// [`BinderCatalog::column_shape`].
	pub fn with_column_shape(mut self, table: &str, column: &str, shape: ColumnShape) -> Self {
		self.shapes.insert((table.to_owned(), column.to_owned()), shape);
		self
	}

	/// Declare an HNSW index of table `table` over column `column`, ranking by `metric`.
	pub fn add_hnsw_index(
		&mut self,
		table: &str,
		column: &str,
		metric: crate::vector::VectorMetric,
	) -> &mut Self {
		self.hnsw.entry(table.to_owned()).or_default().push((column.to_owned(), metric));
		self
	}

	/// Declare that text written to column `column` of table `table` is read as `ty`.
	pub fn add_write_type(&mut self, table: &str, column: &str, ty: crate::SqlType) -> &mut Self {
		self.write_types.insert((table.to_owned(), column.to_owned()), ty);
		self
	}

	/// Declare that column `column` of table `table` holds vectors of `dimensions` dimensions.
	pub fn add_vector(&mut self, table: &str, column: &str, dimensions: usize) -> &mut Self {
		self.vectors.insert((table.to_owned(), column.to_owned()), dimensions);
		self
	}

	/// Declare a vector column, as [`Self::add_vector`] does.
	pub fn with_vector(mut self, table: &str, column: &str, dimensions: usize) -> Self {
		self.add_vector(table, column, dimensions);
		self
	}

	/// Supply a field's declared type without treating a schemaless table as a closed schema.
	pub fn with_column_type(mut self, table: &str, column: &str, ty: crate::SqlType) -> Self {
		self.add_column_type(table, column, ty);
		self
	}

	/// Supply a field's declared type, as [`Self::with_column_type`] does.
	pub fn add_column_type(&mut self, table: &str, column: &str, ty: crate::SqlType) -> &mut Self {
		self.column_types.insert((table.to_owned(), column.to_owned()), ty);
		self
	}

	/// List `tables` in the system views (`information_schema.tables`, `pg_class`, ...) without
	/// describing them to the binder: a table named in a statement stays as described, or
	/// undescribed, as it was.
	pub fn with_listed_tables(mut self, tables: Vec<TableSchema>) -> Self {
		self.listed = tables;
		self
	}

	/// Set the active database name.
	pub fn with_database(mut self, db: impl Into<String>) -> Self {
		self.current_db = Some(db.into());
		self
	}

	/// Set the active database name mutably.
	pub fn set_database(&mut self, db: Option<String>) {
		self.current_db = db;
	}

	/// Register a table schema.
	pub fn add_table(&mut self, schema: TableSchema) {
		let key = (schema.schema.clone(), schema.name.clone());
		self.tables.insert(key, schema);
	}

	/// Declare that column `column` of the schema-less table `table` holds record links to the
	/// table `target`, whether the table is named bare or qualified. The table itself stays as
	/// described (or undescribed) as it was.
	pub fn with_link(mut self, table: &str, column: &str, target: &str) -> Self {
		self.add_link(None, table, column, target);
		self
	}

	/// Declare that column `column` of table `table` in `schema` holds record links to the table
	/// `target`.
	pub fn add_link(
		&mut self,
		schema: Option<&str>,
		table: &str,
		column: &str,
		target: &str,
	) -> &mut Self {
		self.links.insert(
			(schema.map(str::to_string), table.to_string(), column.to_string()),
			target.to_string(),
		);
		self
	}

	/// Helper to define a table quickly.
	pub fn define_table(
		&mut self,
		schema: Option<&str>,
		name: &str,
		columns: &[&str],
	) -> &mut Self {
		let cols = columns.iter().enumerate().map(|(i, &c)| ColumnInfo::new(c, i)).collect();
		let mut table = TableSchema::new(name, cols);
		if let Some(s) = schema {
			table = table.with_schema(s);
		}
		self.add_table(table);
		self
	}
}

impl BinderCatalog for MemoryCatalog {
	fn current_database(&self) -> Option<&str> {
		self.current_db.as_deref()
	}

	fn enum_type(&self, name: &str) -> Option<EnumType> {
		self.enums.iter().find(|e| e.name == name).cloned()
	}

	fn enum_types(&self) -> Vec<EnumType> {
		self.enums.clone()
	}

	fn domain_type(&self, name: &str) -> Option<DomainType> {
		self.domains.iter().find(|d| d.name == name).cloned()
	}

	fn composite_type(&self, name: &str) -> Option<CompositeType> {
		self.composites.iter().find(|c| c.name == name).cloned()
	}

	fn domain_types(&self) -> Vec<DomainType> {
		self.domains.clone()
	}

	fn composite_types(&self) -> Vec<CompositeType> {
		self.composites.clone()
	}

	fn all_tables(&self) -> Vec<TableSchema> {
		let mut seen =
			std::collections::HashSet::with_capacity(self.tables.len() + self.listed.len());
		self.tables
			.values()
			.chain(&self.listed)
			.filter(|t| seen.insert((t.schema.as_deref().unwrap_or("public"), t.name.as_str())))
			.cloned()
			.collect()
	}

	fn get_table(&self, schema: Option<&str>, name: &str) -> Option<TableSchema> {
		if let Some(sys_tbl) = system::get_system_table_schema(schema, name) {
			return Some(sys_tbl);
		}
		let key = (schema.map(|s| s.to_string()), name.to_string());
		if let Some(tbl) = self.tables.get(&key) {
			return Some(tbl.clone());
		}
		// A closed table is registered without a schema: the datastore holds one namespace of
		// tables per database, and a qualifier (`public`, the database name) names that same
		// table.
		if schema.is_some()
			&& let Some(tbl) = self.tables.get(&(None, name.to_string()))
			&& tbl.closed
		{
			return Some(tbl.clone());
		}
		// If schema was not specified, try with None. A closed table is found under the exact
		// name only: table names the datastore holds are case-sensitive.
		if schema.is_none() {
			for ((s, n), tbl) in &self.tables {
				if (s.is_none() || s.as_deref() == Some("public"))
					&& !tbl.closed && n.eq_ignore_ascii_case(name)
				{
					return Some(tbl.clone());
				}
			}
		}
		None
	}

	fn column_type(&self, schema: Option<&str>, table: &str, column: &str) -> crate::SqlType {
		if let Some(ty) = self.column_types.get(&(table.to_owned(), column.to_owned())) {
			return *ty;
		}
		// A closed table is typed by `column_types` alone, so looking it up would copy its columns
		// to find none.
		if self.tables.get(&(None, table.to_owned())).is_some_and(|table| table.closed) {
			return crate::SqlType::Unknown;
		}
		self.get_table(schema, table)
			.and_then(|table| {
				table.find_column(column, false).and_then(|column| column.data_type.clone())
			})
			.map(|name| crate::SqlType::from_name(&name))
			.unwrap_or(crate::SqlType::Unknown)
	}

	fn link_admits_none(&self, _schema: Option<&str>, table: &str, column: &str) -> bool {
		self.nullable_links.contains(&(table.to_owned(), column.to_owned()))
	}

	fn column_enum(&self, schema: Option<&str>, table: &str, column: &str) -> Option<EnumColumn> {
		if schema.is_some_and(|schema| schema != "public") {
			return None;
		}
		self.enum_columns.get(&(table.to_owned(), column.to_owned())).cloned()
	}

	fn null_columns(&self, _schema: Option<&str>, table: &str) -> Vec<String> {
		self.null_columns.get(table).cloned().unwrap_or_default()
	}

	fn default_columns(&self, _schema: Option<&str>, table: &str) -> Vec<(String, Expr)> {
		self.default_columns.get(table).cloned().unwrap_or_default()
	}

	fn column_shape(&self, schema: Option<&str>, table: &str, column: &str) -> Option<ColumnShape> {
		if schema.is_some_and(|schema| schema != "public") {
			return None;
		}
		self.shapes.get(&(table.to_owned(), column.to_owned())).cloned()
	}

	fn declared_column_types(
		&self,
		_schema: Option<&str>,
		table: &str,
	) -> Vec<(String, crate::SqlType)> {
		self.column_types
			.iter()
			.filter(|((name, _), _)| name == table)
			.map(|((_, column), ty)| (column.clone(), *ty))
			.collect()
	}

	fn column_sql_type(
		&self,
		schema: Option<&str>,
		table: &str,
		column: &str,
	) -> Option<DeclaredSqlType> {
		if let Some(declared) = self.sql_types.get(&(table.to_owned(), column.to_owned())) {
			return Some(declared.clone());
		}
		self.get_table(schema, table)
			.and_then(|table| table.find_column(column, false).and_then(|c| c.declared.clone()))
	}

	fn declared_sql_types(
		&self,
		schema: Option<&str>,
		table: &str,
	) -> Vec<(String, DeclaredSqlType)> {
		let mut declared: Vec<(String, DeclaredSqlType)> = self
			.sql_types
			.iter()
			.filter(|((name, _), _)| name == table)
			.map(|((_, column), declared)| (column.clone(), declared.clone()))
			.collect();
		if let Some(table) = self.get_table(schema, table) {
			for column in table.columns {
				if let Some(sql_type) = column.declared
					&& !declared.iter().any(|(name, _)| *name == column.name)
				{
					declared.push((column.name, sql_type));
				}
			}
		}
		declared
	}

	fn vector_dimension(&self, _schema: Option<&str>, table: &str, column: &str) -> Option<usize> {
		self.vectors.get(&(table.to_owned(), column.to_owned())).copied()
	}

	fn write_type(&self, _schema: Option<&str>, table: &str, column: &str) -> crate::SqlType {
		self.write_types
			.get(&(table.to_owned(), column.to_owned()))
			.copied()
			.unwrap_or(crate::SqlType::Unknown)
	}

	fn hnsw_indexes(
		&self,
		_schema: Option<&str>,
		table: &str,
	) -> Vec<(String, crate::vector::VectorMetric)> {
		self.hnsw.get(table).cloned().unwrap_or_default()
	}

	fn link_table(&self, schema: Option<&str>, table: &str, column: &str) -> Option<String> {
		let key = (schema.map(str::to_string), table.to_string(), column.to_string());
		if let Some(target) = self.links.get(&key) {
			return Some(target.clone());
		}
		// A link declared without a schema belongs to the table whatever qualifier names it: a
		// qualified name (`public.emp`, `db.emp`) reads the same table of the current database.
		if schema.is_some()
			&& let Some(target) = self.links.get(&(None, table.to_string(), column.to_string()))
		{
			return Some(target.clone());
		}
		// A bare table is the table of the `public` schema, as `get_table` reads it.
		if schema.is_none() {
			return self
				.links
				.get(&(Some("public".to_string()), table.to_string(), column.to_string()))
				.cloned();
		}
		None
	}
}
