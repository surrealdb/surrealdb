//! The types of a database, declared by `DEFINE TYPE`.
//!
//! A type is stored under one key per name within its database. Its body is either a kind, or a
//! union of string labels kept with the sort key that places each in declaration order and the
//! identity each keeps across a rename or a re-spacing. The PostgreSQL front end allocates a
//! type's wire OIDs from its id and a label's `pg_enum` OID from its identity.

use anyhow::Context as _;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{ExprText, FromStored, KindText, Permission, StoredPermission};
use crate::expr::statements::define::{
	DefineKind, DefineTypeStatement, TypeAssert, union_kind, union_labels,
};
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, Kind, Literal};
use crate::stored::Stored;
use crate::val::Value;
use crate::{expr as runtime, impl_kv_value_catalog};

/// The gap left between the sort keys of consecutive labels when a union is declared, so that
/// inserting a label between two others takes a midpoint without renumbering.
pub const ENUM_ORDER_STRIDE: u64 = 1 << 32;

/// The label identities a union type draws from are below this, so a union holds at most this
/// many less one labels over its life (identities start at 1 and are not reused). The PostgreSQL
/// front end derives a label's `pg_enum` OID from its type's id and its identity, a fixed stride
/// of this many apart, which keeps every label's OID distinct.
pub const ENUM_LABEL_IDENT_LIMIT: u32 = 1 << 10;

/// How many types [`TypeDefinition::union_behind`] follows. A type whose body leads back to itself
/// is refused when it is defined, so only a long chain of aliases reaches the bound.
pub const UNION_ALIAS_DEPTH: usize = 64;

/// The one type `kind` names: `kind` is that named type, or a union of it with `NONE` or `NULL`.
fn single_named_type(kind: &Kind) -> Option<&str> {
	if let Kind::Named(name) = kind {
		return Some(name.as_str());
	}
	let Kind::Either(kinds) = kind else {
		return None;
	};
	let mut named = None;
	for kind in kinds {
		if matches!(kind, Kind::None | Kind::Null) {
			continue;
		}
		let Kind::Named(name) = kind else {
			return None;
		};
		if named.is_some() {
			return None;
		}
		named = Some(name.as_str());
	}
	named
}

/// The database-scoped identity of a type, stable across redefinitions and label changes.
#[revisioned(revision = 1)]
#[derive(
	Debug,
	Clone,
	Copy,
	Default,
	PartialEq,
	Eq,
	PartialOrd,
	Ord,
	Hash,
	Serialize,
	Deserialize,
	Stored,
)]
#[stored(since = "4.0", transparent)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct TypeId(pub u32);

impl TypeId {
	/// The ids a database's types are drawn from are below this. The PostgreSQL front end derives
	/// a type's wire OID and its array type's OID from the id, a fixed distance of this many
	/// apart, so an id at or past it would give a type the OID of another type's array type.
	pub const LIMIT: u32 = 1 << 20;
}

/// One label of a stored union type, the sort key that places it in declaration order, and the
/// identity it keeps while its sort key and its text change.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct StoredEnumLabel {
	#[stored(n = 2)]
	pub label: Strand,
	#[stored(n = 4)]
	pub order: u64,
	#[stored(n = 6)]
	pub ident: u32,
}

/// The body of a stored type.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub enum StoredTypeBody {
	/// Canonical SurrealQL kind text of a body that is not a union of string labels.
	#[stored(n = 1)]
	Kind(KindText),
	/// The labels of a union of string literals.
	#[stored(n = 2)]
	Union(Vec<StoredEnumLabel>),
}

/// One check of a stored type.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct StoredTypeAssert {
	/// The constraint name the check was declared with.
	#[stored(n = 2)]
	pub name: Option<String>,
	/// Canonical SurrealQL text of the check, an expression over `$value`.
	#[stored(n = 4)]
	pub check: ExprText,
}

/// A stored type, keyed by name within its database.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct StoredTypeDefinition {
	#[stored(n = 2)]
	pub id: TypeId,
	#[stored(n = 4)]
	pub name: Strand,
	#[stored(n = 6)]
	pub body: StoredTypeBody,
	#[stored(n = 8)]
	pub asserts: Vec<StoredTypeAssert>,
	/// Canonical SurrealQL text of the `DEFAULT` expression.
	#[stored(n = 10)]
	pub default: Option<ExprText>,
	/// The type as the PostgreSQL front end declares it.
	#[stored(n = 12)]
	pub sql_type: Option<String>,
	#[stored(n = 14)]
	pub permissions: StoredPermission,
	#[stored(
		n = 1,
		ignorable,
		independent = "a comment is text for people and no other field reads it"
	)]
	pub comment: Option<String>,
}
impl_kv_value_catalog!(StoredTypeDefinition);

/// Where a label is placed among the existing ones.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnumPlacement<'a> {
	/// After every existing label.
	Last,
	/// Immediately before the named label.
	Before(&'a str),
	/// Immediately after the named label.
	After(&'a str),
}

/// Why a label edit was refused.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EnumEditError {
	/// The label is already a member.
	Exists(String),
	/// The named label is not a member.
	Missing(String),
	/// The union has used every label identity below [`ENUM_LABEL_IDENT_LIMIT`].
	Full,
}

/// A label of a union type with its sort key and its identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnumLabel {
	pub label: String,
	/// The sort key: a re-spacing of the union rewrites it, keeping declaration order.
	pub order: u64,
	/// Distinct among the union's labels and never changed by a rename or a re-spacing, so what
	/// is derived from it (the label's PostgreSQL `pg_enum` OID) stays the label's own. Labels
	/// declared together take 1, 2, ...; an added label takes one past the greatest.
	pub ident: u32,
}

/// The labels of a union type, kept in ascending sort-key order, which is declaration order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EnumLabels {
	pub labels: Vec<EnumLabel>,
}

impl EnumLabels {
	/// The labels `labels` declared together, in order.
	pub fn declared<'a>(labels: impl IntoIterator<Item = &'a str>) -> Self {
		let labels: Vec<&str> = labels.into_iter().collect();
		let count = labels.len();
		EnumLabels {
			labels: labels
				.into_iter()
				.zip(Self::initial_orders(count))
				.zip(1..)
				.map(|((label, order), ident)| EnumLabel {
					label: label.to_owned(),
					order,
					ident,
				})
				.collect(),
		}
	}

	/// The sort keys of `count` labels declared together.
	pub fn initial_orders(count: usize) -> impl Iterator<Item = u64> {
		(1..=count as u64).map(|i| i.saturating_mul(ENUM_ORDER_STRIDE))
	}

	/// The identity a label added now takes: one past the greatest the union holds.
	pub fn next_ident(&self) -> u32 {
		self.labels.iter().map(|l| l.ident).max().unwrap_or(0).saturating_add(1)
	}

	/// The position of `label` in declaration order, from zero.
	pub fn rank(&self, label: &str) -> Option<usize> {
		self.labels.iter().position(|l| l.label == label)
	}

	/// Whether `label` is a member.
	pub fn contains(&self, label: &str) -> bool {
		self.rank(label).is_some()
	}

	/// The labels in declaration order.
	pub fn label_names(&self) -> impl Iterator<Item = &str> {
		self.labels.iter().map(|l| l.label.as_str())
	}

	/// The kind the labels are: a union of string literal kinds, in declaration order.
	pub fn kind(&self) -> Kind {
		union_kind(self.label_names())
	}

	/// Re-spaces every sort key evenly, keeping declaration order.
	pub fn rebalance(&mut self) {
		let count = self.labels.len();
		for (label, order) in self.labels.iter_mut().zip(Self::initial_orders(count)) {
			label.order = order;
		}
	}

	/// Inserts `label` at `placement`, taking the midpoint of its neighbours' sort keys and
	/// re-spacing the whole union first when no gap is left.
	pub fn add_label(
		&mut self,
		label: &str,
		placement: EnumPlacement<'_>,
	) -> Result<(), EnumEditError> {
		if self.contains(label) {
			return Err(EnumEditError::Exists(label.to_owned()));
		}
		let index = match placement {
			EnumPlacement::Last => self.labels.len(),
			EnumPlacement::Before(anchor) => {
				self.rank(anchor).ok_or_else(|| EnumEditError::Missing(anchor.to_owned()))?
			}
			EnumPlacement::After(anchor) => {
				self.rank(anchor).ok_or_else(|| EnumEditError::Missing(anchor.to_owned()))? + 1
			}
		};
		let ident = self.next_ident();
		if ident >= ENUM_LABEL_IDENT_LIMIT {
			return Err(EnumEditError::Full);
		}
		let order = match self.order_at(index) {
			Some(order) => order,
			None => {
				self.rebalance();
				self.order_at(index).unwrap_or(ENUM_ORDER_STRIDE)
			}
		};
		self.labels.insert(
			index,
			EnumLabel {
				label: label.to_owned(),
				order,
				ident,
			},
		);
		Ok(())
	}

	/// The sort key a label inserted at `index` takes, or `None` when the neighbours leave no gap.
	fn order_at(&self, index: usize) -> Option<u64> {
		let below = index.checked_sub(1).map(|i| self.labels[i].order).unwrap_or(0);
		match self.labels.get(index) {
			Some(above) => {
				let gap = above.order - below;
				(gap >= 2).then(|| below + gap / 2)
			}
			None => below.checked_add(ENUM_ORDER_STRIDE),
		}
	}

	/// Renames `from` to `to`, keeping its place in declaration order.
	pub fn rename_label(&mut self, from: &str, to: &str) -> Result<(), EnumEditError> {
		if from != to && self.contains(to) {
			return Err(EnumEditError::Exists(to.to_owned()));
		}
		let index = self.rank(from).ok_or_else(|| EnumEditError::Missing(from.to_owned()))?;
		to.clone_into(&mut self.labels[index].label);
		Ok(())
	}
}

/// What a type is.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TypeBody {
	/// A kind that is not a union of string labels.
	Kind(Kind),
	/// A union of string labels.
	Union(EnumLabels),
}

impl TypeBody {
	/// The body a declared kind is: a union of string literals is a union of labels declared
	/// together, any other kind is itself.
	pub fn declared(kind: Kind) -> Self {
		match union_labels(&kind) {
			Some(labels) => {
				TypeBody::Union(EnumLabels::declared(labels.iter().map(String::as_str)))
			}
			None => TypeBody::Kind(kind),
		}
	}

	/// The kind a value of the type has.
	pub fn kind(&self) -> Kind {
		match self {
			TypeBody::Kind(kind) => kind.clone(),
			TypeBody::Union(labels) => labels.kind(),
		}
	}
}

/// Runtime form of [`StoredTypeDefinition`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypeDefinition {
	pub id: TypeId,
	pub name: Strand,
	pub body: TypeBody,
	pub asserts: Vec<TypeAssert>,
	pub default: Option<Expr>,
	pub sql_type: Option<String>,
	pub permissions: Permission,
	pub comment: Option<String>,
}

impl TypeDefinition {
	/// The labels of the type, when its body is a union of string labels.
	pub fn union(&self) -> Option<&EnumLabels> {
		match &self.body {
			TypeBody::Union(labels) => Some(labels),
			TypeBody::Kind(_) => None,
		}
	}

	/// The labels of the type, to edit, when its body is a union of string labels.
	pub fn union_mut(&mut self) -> Option<&mut EnumLabels> {
		match &mut self.body {
			TypeBody::Union(labels) => Some(labels),
			TypeBody::Kind(_) => None,
		}
	}

	/// The kind a value of the type has.
	pub fn kind(&self) -> Kind {
		self.body.kind()
	}

	/// The type whose union of string labels a value of this type holds: this type when its body
	/// is a union, or the one the type its body names holds, where the body names one type alone
	/// or beside `NONE` and `NULL` (an alias, or a domain that may be empty). `types` answers the
	/// other types of the database by name. `None` for any other body, and for a chain of more
	/// than [`UNION_ALIAS_DEPTH`] types.
	pub fn union_behind<'a>(
		&'a self,
		types: &dyn Fn(&str) -> Option<&'a TypeDefinition>,
	) -> Option<&'a TypeDefinition> {
		let mut current = self;
		for _ in 0..UNION_ALIAS_DEPTH {
			match &current.body {
				TypeBody::Union(_) => return Some(current),
				TypeBody::Kind(kind) => current = types(single_named_type(kind)?)?,
			}
		}
		None
	}

	/// Whether the body is an object literal kind: the shape of a PostgreSQL composite type.
	pub fn is_object(&self) -> bool {
		matches!(&self.body, TypeBody::Kind(Kind::Literal(crate::expr::KindLiteral::Object(_))))
	}

	/// Whether the type is what PostgreSQL calls a domain: neither a union of string labels (an
	/// enum) nor an object literal kind (a composite type).
	pub fn is_domain(&self) -> bool {
		self.union().is_none() && !self.is_object()
	}

	/// The names of the types the definition names: in its body, at any depth, and in the
	/// types written in its assertions and its default.
	pub fn referenced_types(&self) -> Vec<String> {
		let mut names: Vec<String> = Vec::new();
		let mut note = |name: &str| {
			if !names.iter().any(|n| n == name) {
				names.push(name.to_owned());
			}
		};
		if let TypeBody::Kind(kind) = &self.body {
			kind.each_named_type(&mut note);
		}
		for assert in &self.asserts {
			runtime::visit::each_named_type_in_expr(&assert.check, &mut note);
		}
		if let Some(default) = &self.default {
			runtime::visit::each_named_type_in_expr(default, &mut note);
		}
		names
	}

	fn to_runtime_definition(&self) -> DefineTypeStatement {
		DefineTypeStatement {
			kind: DefineKind::Default,
			name: self.name.clone(),
			body: self.kind(),
			asserts: self.asserts.clone(),
			default: self.default.clone(),
			sql_type: self.sql_type.clone(),
			comment: self
				.comment
				.clone()
				.map(|v| runtime::Expr::Literal(runtime::Literal::String(v.into())))
				.unwrap_or(runtime::Expr::Literal(Literal::None)),
			permissions: self.permissions.clone(),
		}
	}

	pub fn to_stored(&self) -> StoredTypeDefinition {
		StoredTypeDefinition {
			id: self.id,
			name: self.name.clone(),
			body: match &self.body {
				TypeBody::Kind(kind) => StoredTypeBody::Kind(KindText::new(kind)),
				TypeBody::Union(labels) => StoredTypeBody::Union(
					labels
						.labels
						.iter()
						.map(|l| StoredEnumLabel {
							label: Strand::new(l.label.clone()),
							order: l.order,
							ident: l.ident,
						})
						.collect(),
				),
			},
			asserts: self
				.asserts
				.iter()
				.map(|assert| StoredTypeAssert {
					name: assert.name.clone(),
					check: ExprText::new(&assert.check),
				})
				.collect(),
			default: self.default.as_ref().map(ExprText::new),
			sql_type: self.sql_type.clone(),
			permissions: StoredPermission::from_runtime(&self.permissions),
			comment: self.comment.clone(),
		}
	}
}

impl ToSql for TypeDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_runtime_definition().fmt_sql(f, fmt)
	}
}

impl InfoStructure for TypeDefinition {
	fn structure(self) -> Value {
		let labels = self
			.union()
			.map(|labels| Value::from(labels.label_names().map(Value::from).collect::<Vec<_>>()));
		Value::from(map! {
			"name" => self.name.clone().into(),
			"kind" => Value::from(self.kind().to_sql()),
			"asserts" => Value::from(
				self.asserts
					.into_iter()
					.map(|assert| Value::from(map! {
						"name", if let Some(name) = assert.name => Value::from(name),
						"check" => Value::from(assert.check.to_sql()),
					}))
					.collect::<Vec<_>>()
			),
			"default", if let Some(default) = self.default => Value::from(default.to_sql()),
			"sql_type", if let Some(sql_type) = self.sql_type => Value::from(sql_type),
			"labels", if let Some(labels) = labels => labels,
			"permissions" => self.permissions.structure(),
			"comment", if let Some(v) = self.comment => v.into(),
		})
	}
}

impl FromStored for TypeDefinition {
	type Stored = StoredTypeDefinition;

	fn from_stored(stored: &StoredTypeDefinition) -> anyhow::Result<TypeDefinition> {
		fn build(stored: &StoredTypeDefinition) -> anyhow::Result<TypeDefinition> {
			let body = match &stored.body {
				StoredTypeBody::Kind(kind) => TypeBody::Kind(kind.compile()?),
				StoredTypeBody::Union(labels) => {
					let mut labels: Vec<EnumLabel> = labels
						.iter()
						.map(|l| EnumLabel {
							label: l.label.to_string(),
							order: l.order,
							ident: l.ident,
						})
						.collect();
					labels.sort_by_key(|l| l.order);
					TypeBody::Union(EnumLabels {
						labels,
					})
				}
			};
			Ok(TypeDefinition {
				id: stored.id,
				name: stored.name.clone(),
				body,
				asserts: stored
					.asserts
					.iter()
					.map(|assert| {
						Ok(TypeAssert {
							name: assert.name.clone(),
							check: assert.check.compile()?,
						})
					})
					.collect::<anyhow::Result<Vec<_>>>()?,
				default: stored.default.as_ref().map(ExprText::compile).transpose()?,
				sql_type: stored.sql_type.clone(),
				permissions: Permission::from_stored(&stored.permissions)?,
				comment: stored.comment.clone(),
			})
		}
		build(stored).with_context(|| {
			format!("the stored definition of the type `{}` no longer compiles", stored.name)
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::{BinaryOperator, KindLiteral, Param};

	fn round_trip(definition: &TypeDefinition) -> TypeDefinition {
		let stored = definition.to_stored();
		let bytes = revision::to_vec(&stored).expect("the definition encodes");
		let decoded: StoredTypeDefinition =
			revision::from_slice(&bytes).expect("the definition decodes");
		assert_eq!(decoded, stored);
		TypeDefinition::from_stored(&decoded).expect("the stored definition compiles")
	}

	fn definition(body: TypeBody) -> TypeDefinition {
		TypeDefinition {
			id: TypeId(3),
			name: Strand::new("t"),
			body,
			asserts: Vec::new(),
			default: None,
			sql_type: None,
			permissions: Permission::default(),
			comment: None,
		}
	}

	fn union(labels: &[&str]) -> EnumLabels {
		EnumLabels::declared(labels.iter().copied())
	}

	fn names(d: &EnumLabels) -> Vec<&str> {
		d.label_names().collect()
	}

	#[test]
	fn a_refined_scalar_round_trips_through_its_stored_form() {
		let mut scalar = definition(TypeBody::Kind(Kind::Int));
		scalar.asserts = vec![
			TypeAssert {
				name: Some("positive".to_owned()),
				check: Expr::Binary {
					left: Box::new(Expr::Param(Param::from("value".to_owned()))),
					op: BinaryOperator::MoreThan,
					right: Box::new(Expr::Literal(Literal::Integer(0))),
				},
			},
			TypeAssert {
				name: None,
				check: Expr::Literal(Literal::Bool(true)),
			},
		];
		scalar.default = Some(Expr::Literal(Literal::Integer(1)));
		scalar.sql_type = Some("integer".to_owned());
		scalar.comment = Some("a count".to_owned());
		assert_eq!(round_trip(&scalar), scalar);
		assert!(scalar.union().is_none() && !scalar.is_object());
	}

	#[test]
	fn an_object_shape_round_trips_through_its_stored_form() {
		let mut fields = std::collections::BTreeMap::new();
		fields.insert(Strand::new("street"), Kind::String);
		fields.insert(Strand::new("zip"), Kind::option(Kind::Named("zip_code".to_owned())));
		let shape = definition(TypeBody::Kind(Kind::Literal(KindLiteral::Object(fields))));
		assert_eq!(round_trip(&shape), shape);
		assert!(shape.is_object());
		assert_eq!(shape.referenced_types(), ["zip_code"]);
	}

	#[test]
	fn a_union_round_trips_through_its_stored_form() {
		let mut mood = definition(TypeBody::declared(union_kind(["sad", "ok", "happy"])));
		assert_eq!(names(mood.union().expect("a union")), ["sad", "ok", "happy"]);
		mood.union_mut().expect("a union").add_label("meh", EnumPlacement::After("sad")).unwrap();
		let decoded = round_trip(&mood);
		assert_eq!(decoded, mood);
		assert_eq!(names(decoded.union().expect("a union")), ["sad", "meh", "ok", "happy"]);
		assert_eq!(decoded.kind(), union_kind(["sad", "meh", "ok", "happy"]));
	}

	#[test]
	fn places_labels_by_midpoint() {
		let mut d = union(&["a", "c"]);
		d.add_label("b", EnumPlacement::After("a")).unwrap();
		d.add_label("z", EnumPlacement::Last).unwrap();
		d.add_label("_", EnumPlacement::Before("a")).unwrap();
		assert_eq!(names(&d), ["_", "a", "b", "c", "z"]);
		assert!(d.labels.windows(2).all(|w| w[0].order < w[1].order));
		// A new label takes a midpoint: existing sort keys are untouched.
		assert_eq!(d.labels[1].order, ENUM_ORDER_STRIDE);
	}

	#[test]
	fn rebalances_when_no_gap_is_left() {
		let mut d = union(&["a", "b"]);
		d.labels[0].order = 10;
		d.labels[1].order = 11;
		d.add_label("x", EnumPlacement::After("a")).unwrap();
		assert_eq!(names(&d), ["a", "x", "b"]);
		assert!(d.labels.windows(2).all(|w| w[0].order < w[1].order));
		// The re-spacing rewrote every sort key, and no label's identity.
		let idents: Vec<u32> = d.labels.iter().map(|l| l.ident).collect();
		assert_eq!(idents, [1, 3, 2]);
		d.rename_label("x", "y").unwrap();
		assert_eq!(d.labels[1].ident, 3);
	}

	#[test]
	fn refuses_a_label_past_the_identity_limit() {
		let mut d = union(&["a"]);
		d.labels[0].ident = ENUM_LABEL_IDENT_LIMIT - 2;
		d.add_label("b", EnumPlacement::Last).unwrap();
		assert_eq!(d.labels[1].ident, ENUM_LABEL_IDENT_LIMIT - 1);
		assert_eq!(d.add_label("c", EnumPlacement::Last), Err(EnumEditError::Full));
	}

	#[test]
	fn refuses_duplicates_and_unknown_anchors() {
		let mut d = union(&["a", "b"]);
		assert_eq!(d.add_label("a", EnumPlacement::Last), Err(EnumEditError::Exists("a".into())));
		assert_eq!(
			d.add_label("c", EnumPlacement::Before("q")),
			Err(EnumEditError::Missing("q".into()))
		);
		assert_eq!(d.rename_label("a", "b"), Err(EnumEditError::Exists("b".into())));
		d.rename_label("a", "A").unwrap();
		assert_eq!(names(&d), ["A", "b"]);
		assert_eq!(d.rank("b"), Some(1));
	}
}
