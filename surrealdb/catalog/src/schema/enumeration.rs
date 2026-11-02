use anyhow::Context as _;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{FromStored, Permission, StoredPermission};
use crate::expr::statements::define::{DefineEnumStatement, DefineKind};
use crate::expr::statements::info::InfoStructure;
use crate::stored::Stored;
use crate::val::Value;
use crate::{expr as runtime, impl_kv_value_catalog};

/// The gap left between the sort keys of consecutive labels when an enum is declared, so that
/// inserting a label between two others takes a midpoint without renumbering.
pub const ENUM_ORDER_STRIDE: u64 = 1 << 32;

/// The database-scoped identity of an enum, stable across label changes.
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
pub struct EnumId(pub u32);

impl EnumId {
	/// The ids a database's enums and user-defined types are drawn from are below this. The
	/// PostgreSQL front end derives a type's wire OID and its array type's OID from the id, a
	/// fixed distance of this many apart, so an id at or past it would give an enum the OID of
	/// another enum's array type.
	pub const LIMIT: u32 = 1 << 20;
}

/// The label identities an enum draws from are below this, so an enum holds at most this many
/// less one labels over its life (identities start at 1 and are not reused). The PostgreSQL
/// front end derives a label's `pg_enum` OID from its enum's id and its identity, a fixed
/// stride of this many apart, which keeps every label's OID distinct.
pub const ENUM_LABEL_IDENT_LIMIT: u32 = 1 << 10;

/// One label of a stored enum, the sort key that places it in declaration order, and the
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

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct StoredEnumDefinition {
	#[stored(n = 2)]
	pub id: EnumId,
	#[stored(n = 4)]
	pub name: Strand,
	#[stored(n = 6)]
	pub labels: Vec<StoredEnumLabel>,
	#[stored(n = 8)]
	pub permissions: StoredPermission,
	#[stored(
		n = 1,
		ignorable,
		independent = "a comment is text for people and no other field reads it"
	)]
	pub comment: Option<String>,
}
impl_kv_value_catalog!(StoredEnumDefinition);

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
	/// The enum has used every label identity below [`ENUM_LABEL_IDENT_LIMIT`].
	Full,
}

/// A label of an enum with its sort key and its identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnumLabel {
	pub label: String,
	/// The sort key: a re-spacing of the enum rewrites it, keeping declaration order.
	pub order: u64,
	/// Distinct among the enum's labels and never changed by a rename or a re-spacing, so what is
	/// derived from it (the label's PostgreSQL `pg_enum` OID) stays the label's own. Labels
	/// declared together take 1, 2, ...; an added label takes one past the greatest.
	pub ident: u32,
}

/// Runtime form of [`StoredEnumDefinition`]. `labels` is kept in ascending sort-key order, which
/// is declaration order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnumDefinition {
	pub id: EnumId,
	pub name: Strand,
	pub labels: Vec<EnumLabel>,
	pub permissions: Permission,
	pub comment: Option<String>,
}

impl EnumDefinition {
	/// The sort keys of `count` labels declared together.
	pub fn initial_orders(count: usize) -> impl Iterator<Item = u64> {
		(1..=count as u64).map(|i| i.saturating_mul(ENUM_ORDER_STRIDE))
	}

	/// The identity a label added now takes: one past the greatest the enum holds.
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

	/// Re-spaces every sort key evenly, keeping declaration order.
	pub fn rebalance(&mut self) {
		let count = self.labels.len();
		for (label, order) in self.labels.iter_mut().zip(Self::initial_orders(count)) {
			label.order = order;
		}
	}

	/// Inserts `label` at `placement`, taking the midpoint of its neighbours' sort keys and
	/// re-spacing the whole enum first when no gap is left.
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

	fn to_runtime_definition(&self) -> DefineEnumStatement {
		DefineEnumStatement {
			kind: DefineKind::Default,
			name: self.name.clone(),
			labels: self.labels.iter().map(|l| l.label.clone()).collect(),
			comment: self
				.comment
				.clone()
				.map(|v| runtime::Expr::Literal(runtime::Literal::String(v.into())))
				.unwrap_or(runtime::Expr::Literal(runtime::Literal::None)),
			permissions: self.permissions.clone(),
		}
	}

	pub fn to_stored(&self) -> StoredEnumDefinition {
		StoredEnumDefinition {
			id: self.id,
			name: self.name.clone(),
			labels: self
				.labels
				.iter()
				.map(|l| StoredEnumLabel {
					label: Strand::new(l.label.clone()),
					order: l.order,
					ident: l.ident,
				})
				.collect(),
			permissions: StoredPermission::from_runtime(&self.permissions),
			comment: self.comment.clone(),
		}
	}
}

impl ToSql for EnumDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_runtime_definition().fmt_sql(f, fmt)
	}
}

impl InfoStructure for EnumDefinition {
	fn structure(self) -> Value {
		Value::from(map! {
			"name" => self.name.into(),
			"values" => Value::from(
				self.labels.into_iter().map(|l| Value::from(l.label)).collect::<Vec<_>>()
			),
			"permissions" => self.permissions.structure(),
			"comment", if let Some(v) = self.comment => v.into(),
		})
	}
}

impl FromStored for EnumDefinition {
	type Stored = StoredEnumDefinition;

	fn from_stored(stored: &StoredEnumDefinition) -> anyhow::Result<EnumDefinition> {
		fn build(stored: &StoredEnumDefinition) -> anyhow::Result<EnumDefinition> {
			let mut labels: Vec<EnumLabel> = stored
				.labels
				.iter()
				.map(|l| EnumLabel {
					label: l.label.to_string(),
					order: l.order,
					ident: l.ident,
				})
				.collect();
			labels.sort_by_key(|l| l.order);
			Ok(EnumDefinition {
				id: stored.id,
				name: stored.name.clone(),
				labels,
				permissions: Permission::from_stored(&stored.permissions)?,
				comment: stored.comment.clone(),
			})
		}
		build(stored).with_context(|| {
			format!("the stored definition of enum `{}` no longer compiles", stored.name)
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn definition(labels: &[&str]) -> EnumDefinition {
		EnumDefinition {
			id: EnumId(1),
			name: Strand::new("mood"),
			labels: labels
				.iter()
				.zip(EnumDefinition::initial_orders(labels.len()))
				.zip(1..)
				.map(|((label, order), ident)| EnumLabel {
					label: (*label).to_owned(),
					order,
					ident,
				})
				.collect(),
			permissions: Permission::default(),
			comment: None,
		}
	}

	fn names(d: &EnumDefinition) -> Vec<&str> {
		d.label_names().collect()
	}

	#[test]
	fn places_labels_by_midpoint() {
		let mut d = definition(&["a", "c"]);
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
		let mut d = definition(&["a", "b"]);
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
		let mut d = definition(&["a"]);
		d.labels[0].ident = ENUM_LABEL_IDENT_LIMIT - 2;
		d.add_label("b", EnumPlacement::Last).unwrap();
		assert_eq!(d.labels[1].ident, ENUM_LABEL_IDENT_LIMIT - 1);
		assert_eq!(d.add_label("c", EnumPlacement::Last), Err(EnumEditError::Full));
	}

	#[test]
	fn refuses_duplicates_and_unknown_anchors() {
		let mut d = definition(&["a", "b"]);
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
