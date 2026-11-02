//! User-defined SQL types of a database: PostgreSQL domains and composite types.
//!
//! A database's domains and composite types share one name space, as PostgreSQL's do, and are
//! stored under one key per name. A type is defined by the SQL front ends only; a column declared
//! with one is a field whose kind, `ASSERT` and `DEFAULT` the front end derives from the type and
//! whose `SQL TYPE` names it. The entity is what reads the type by name: the lowering of a later
//! column, the catalog views, and the wire types, which allocate the type's OIDs from its id.

use anyhow::Context as _;
use revision::revisioned;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::{ExprText, FromStored, KindText};
use crate::expr::statements::define::{
	CompositeBody, CompositeField, DefineKind, DefineUserTypeStatement, DomainBody, DomainCheck,
	UserTypeBody,
};
use crate::expr::statements::info::InfoStructure;
use crate::impl_kv_value_catalog;
use crate::stored::Stored;
use crate::val::Value;

/// The database-scoped identity of a domain or composite type. It is drawn from the sequence that
/// numbers the database's enums, so no two user-defined types of a database share one.
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
	serde::Serialize,
	serde::Deserialize,
	Stored,
)]
#[stored(since = "4.0", transparent)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct UserTypeId(pub u32);

/// One check of a stored domain.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct StoredDomainCheck {
	/// The constraint name the check was declared with.
	#[stored(n = 2)]
	pub name: Option<String>,
	/// Canonical SurrealQL text of the check, an expression over `$value`.
	#[stored(n = 4)]
	pub check: ExprText,
}

/// A stored domain.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct StoredDomain {
	/// Canonical SurrealQL kind text of the base type.
	#[stored(n = 2)]
	pub base: KindText,
	/// The base type as the PostgreSQL front end declares it.
	#[stored(n = 4)]
	pub base_type: String,
	#[stored(n = 6)]
	pub not_null: bool,
	/// Canonical SurrealQL text of the `DEFAULT` expression.
	#[stored(n = 8)]
	pub default: Option<ExprText>,
	#[stored(n = 10)]
	pub checks: Vec<StoredDomainCheck>,
}

/// One attribute of a stored composite type.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct StoredCompositeField {
	#[stored(n = 2)]
	pub name: String,
	/// Canonical SurrealQL kind text of the attribute's kind.
	#[stored(n = 4)]
	pub kind: KindText,
	/// The attribute's type as the PostgreSQL front end declares it.
	#[stored(n = 6)]
	pub sql_type: String,
}

/// A stored composite type.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct StoredComposite {
	#[stored(n = 2)]
	pub fields: Vec<StoredCompositeField>,
}

/// What a stored user-defined type is.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub enum StoredUserTypeBody {
	#[stored(n = 1)]
	Domain(StoredDomain),
	#[stored(n = 2)]
	Composite(StoredComposite),
}

/// A stored domain or composite type, keyed by name within its database.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct StoredUserTypeDefinition {
	#[stored(n = 2)]
	pub id: UserTypeId,
	#[stored(n = 4)]
	pub name: Strand,
	#[stored(n = 6)]
	pub body: StoredUserTypeBody,
}
impl_kv_value_catalog!(StoredUserTypeDefinition);

/// Runtime form of [`StoredUserTypeDefinition`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserTypeDefinition {
	pub id: UserTypeId,
	pub name: Strand,
	pub body: UserTypeBody,
}

impl UserTypeDefinition {
	/// The domain this type is, when it is one.
	pub fn domain(&self) -> Option<&DomainBody> {
		match &self.body {
			UserTypeBody::Domain(domain) => Some(domain),
			UserTypeBody::Composite(_) => None,
		}
	}

	/// The composite type this type is, when it is one.
	pub fn composite(&self) -> Option<&CompositeBody> {
		match &self.body {
			UserTypeBody::Domain(_) => None,
			UserTypeBody::Composite(composite) => Some(composite),
		}
	}

	fn to_runtime_definition(&self) -> DefineUserTypeStatement {
		DefineUserTypeStatement {
			kind: DefineKind::Default,
			name: self.name.clone(),
			body: self.body.clone(),
		}
	}

	pub fn to_stored(&self) -> StoredUserTypeDefinition {
		StoredUserTypeDefinition {
			id: self.id,
			name: self.name.clone(),
			body: match &self.body {
				UserTypeBody::Domain(domain) => StoredUserTypeBody::Domain(StoredDomain {
					base: KindText::new(&domain.base),
					base_type: domain.base_type.clone(),
					not_null: domain.not_null,
					default: domain.default.as_ref().map(ExprText::new),
					checks: domain
						.checks
						.iter()
						.map(|check| StoredDomainCheck {
							name: check.name.clone(),
							check: ExprText::new(&check.check),
						})
						.collect(),
				}),
				UserTypeBody::Composite(composite) => {
					StoredUserTypeBody::Composite(StoredComposite {
						fields: composite
							.fields
							.iter()
							.map(|field| StoredCompositeField {
								name: field.name.clone(),
								kind: KindText::new(&field.kind),
								sql_type: field.sql_type.clone(),
							})
							.collect(),
					})
				}
			},
		}
	}
}

impl ToSql for UserTypeDefinition {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.to_runtime_definition().fmt_sql(f, fmt)
	}
}

impl InfoStructure for UserTypeDefinition {
	fn structure(self) -> Value {
		match self.body {
			UserTypeBody::Domain(domain) => Value::from(map! {
				"name" => self.name.into(),
				"kind" => Value::from("domain"),
				"base" => Value::from(domain.base.to_sql()),
				"sql_type" => Value::from(domain.base_type),
				"not_null" => Value::from(domain.not_null),
				"default", if let Some(default) = domain.default => Value::from(default.to_sql()),
				"checks" => Value::from(
					domain
						.checks
						.into_iter()
						.map(|check| Value::from(map! {
							"name", if let Some(name) = check.name => Value::from(name),
							"check" => Value::from(check.check.to_sql()),
						}))
						.collect::<Vec<_>>()
				),
			}),
			UserTypeBody::Composite(composite) => Value::from(map! {
				"name" => self.name.into(),
				"kind" => Value::from("composite"),
				"fields" => Value::from(
					composite
						.fields
						.into_iter()
						.map(|field| Value::from(map! {
							"name" => Value::from(field.name),
							"kind" => Value::from(field.kind.to_sql()),
							"sql_type" => Value::from(field.sql_type),
						}))
						.collect::<Vec<_>>()
				),
			}),
		}
	}
}

impl FromStored for UserTypeDefinition {
	type Stored = StoredUserTypeDefinition;

	fn from_stored(stored: &StoredUserTypeDefinition) -> anyhow::Result<UserTypeDefinition> {
		fn build(stored: &StoredUserTypeDefinition) -> anyhow::Result<UserTypeDefinition> {
			let body = match &stored.body {
				StoredUserTypeBody::Domain(domain) => UserTypeBody::Domain(DomainBody {
					base: domain.base.compile()?,
					base_type: domain.base_type.clone(),
					not_null: domain.not_null,
					default: domain.default.as_ref().map(ExprText::compile).transpose()?,
					checks: domain
						.checks
						.iter()
						.map(|check| {
							Ok(DomainCheck {
								name: check.name.clone(),
								check: check.check.compile()?,
							})
						})
						.collect::<anyhow::Result<Vec<_>>>()?,
				}),
				StoredUserTypeBody::Composite(composite) => {
					UserTypeBody::Composite(CompositeBody {
						fields: composite
							.fields
							.iter()
							.map(|field| {
								Ok(CompositeField {
									name: field.name.clone(),
									kind: field.kind.compile()?,
									sql_type: field.sql_type.clone(),
								})
							})
							.collect::<anyhow::Result<Vec<_>>>()?,
					})
				}
			};
			Ok(UserTypeDefinition {
				id: stored.id,
				name: stored.name.clone(),
				body,
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
	use crate::expr::{BinaryOperator, Expr, Kind, Literal, Param};

	fn round_trip(definition: &UserTypeDefinition) -> UserTypeDefinition {
		let stored = definition.to_stored();
		let bytes = revision::to_vec(&stored).expect("the definition encodes");
		let decoded: StoredUserTypeDefinition =
			revision::from_slice(&bytes).expect("the definition decodes");
		assert_eq!(decoded, stored);
		UserTypeDefinition::from_stored(&decoded).expect("the stored definition compiles")
	}

	#[test]
	fn a_domain_round_trips_through_its_stored_form() {
		let domain = UserTypeDefinition {
			id: UserTypeId(3),
			name: Strand::new("posint"),
			body: UserTypeBody::Domain(DomainBody {
				base: Kind::I32,
				base_type: "integer".to_owned(),
				not_null: true,
				default: Some(Expr::Literal(Literal::Integer(1))),
				checks: vec![
					DomainCheck {
						name: Some("positive".to_owned()),
						check: Expr::Binary {
							left: Box::new(Expr::Param(Param::from("value".to_owned()))),
							op: BinaryOperator::MoreThan,
							right: Box::new(Expr::Literal(Literal::Integer(0))),
						},
					},
					DomainCheck {
						name: None,
						check: Expr::Literal(Literal::Bool(true)),
					},
				],
			}),
		};
		assert_eq!(round_trip(&domain), domain);
		assert!(domain.domain().is_some() && domain.composite().is_none());
	}

	#[test]
	fn a_composite_type_round_trips_through_its_stored_form() {
		let composite = UserTypeDefinition {
			id: UserTypeId(4),
			name: Strand::new("address"),
			body: UserTypeBody::Composite(CompositeBody {
				fields: vec![
					CompositeField {
						name: "street".to_owned(),
						kind: Kind::either(vec![Kind::None, Kind::Null, Kind::String]),
						sql_type: "text".to_owned(),
					},
					CompositeField {
						name: "zip".to_owned(),
						kind: Kind::either(vec![Kind::None, Kind::Null, Kind::I32]),
						sql_type: "integer".to_owned(),
					},
				],
			}),
		};
		assert_eq!(round_trip(&composite), composite);
		assert!(composite.composite().is_some() && composite.domain().is_none());
	}
}
