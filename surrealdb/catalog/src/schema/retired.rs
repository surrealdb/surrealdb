//! Catalog types the schema lock keeps although nothing reads or writes them.
//!
//! A type, once in `catalog-schema.lock`, stays there so that its numbers are never given to
//! another meaning. Enums, domains and composite types are stored as
//! [`crate::catalog::StoredTypeDefinition`] ; the types below are the separate shapes they had,
//! kept only so the lock still lists them. No key maps to them and no code constructs them.

use surrealdb_strand::Strand;

use crate::catalog::{ExprText, KindText, StoredEnumLabel, StoredPermission};
use crate::stored::{Stored, StoredField as _};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Stored)]
#[stored(since = "4.0", transparent)]
pub struct EnumId(pub u32);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Stored)]
#[stored(since = "4.0", transparent)]
pub struct UserTypeId(pub u32);

#[derive(Clone, Debug, Default, Eq, PartialEq, Stored)]
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

#[derive(Clone, Debug, Eq, PartialEq, Stored)]
#[stored(since = "4.0")]
pub struct StoredDomainCheck {
	#[stored(n = 2)]
	pub name: Option<String>,
	#[stored(n = 4)]
	pub check: ExprText,
}

#[derive(Clone, Debug, Eq, PartialEq, Stored)]
#[stored(since = "4.0")]
pub struct StoredDomain {
	#[stored(n = 2)]
	pub base: KindText,
	#[stored(n = 4)]
	pub base_type: String,
	#[stored(n = 6)]
	pub not_null: bool,
	#[stored(n = 8)]
	pub default: Option<ExprText>,
	#[stored(n = 10)]
	pub checks: Vec<StoredDomainCheck>,
}

#[derive(Clone, Debug, Eq, PartialEq, Stored)]
#[stored(since = "4.0")]
pub struct StoredCompositeField {
	#[stored(n = 2)]
	pub name: String,
	#[stored(n = 4)]
	pub kind: KindText,
	#[stored(n = 6)]
	pub sql_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Stored)]
#[stored(since = "4.0")]
pub struct StoredComposite {
	#[stored(n = 2)]
	pub fields: Vec<StoredCompositeField>,
}

#[derive(Clone, Debug, Eq, PartialEq, Stored)]
#[stored(since = "4.0")]
pub enum StoredUserTypeBody {
	#[stored(n = 1)]
	Domain(StoredDomain),
	#[stored(n = 2)]
	Composite(StoredComposite),
}

#[derive(Clone, Debug, Eq, PartialEq, Stored)]
#[stored(since = "4.0")]
pub struct StoredUserTypeDefinition {
	#[stored(n = 2)]
	pub id: UserTypeId,
	#[stored(n = 4)]
	pub name: Strand,
	#[stored(n = 6)]
	pub body: StoredUserTypeBody,
}

/// The schemas of the retired types, for the schema lock.
pub fn collect_retired_schemas(out: &mut Vec<&'static crate::stored::TypeSchema>) {
	StoredEnumDefinition::collect_schemas(out);
	StoredUserTypeDefinition::collect_schemas(out);
}
