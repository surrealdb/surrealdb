//! What the catalog knows about the rows of a table against its field definitions.
//!
//! A [`FieldClaim`] states that a field's definition describes every row of the table. The claims
//! of a table are one [`TableConformance`] under the table's `!fc` key, so that settling or
//! withdrawing a claim neither rewrites a field definition nor changes the catalog version.
//!
//! # What a claim states
//!
//! A [`Conformance::Guaranteed`] claim `(field, digest)` on a table means that every row of the
//! table whose envelope stamp is a generation of the table's ring holds, at every occupied path of
//! the field, a value that the definition with that digest coerced. A field with no claim is
//! unchecked.
//!
//! - A claim says nothing about a row stamped 0 or written in `revision`, which is outside every
//!   claim, nor about a definition whose digest differs from the claim's.
//! - A consumer checks [`FieldClaim::holds_for`] against the current definition and the row's
//!   stamp, and keeps the per-row generation check: a table-wide fact chooses code, and never
//!   replaces the check of the row in hand.
//! - A claim is keyed by the field's `FieldKey` component exactly as stored; a lookup spelled
//!   otherwise misses, which is safe.
//!
//! [`birth_claims_of`] is the claims of a table whose rows all follow its field definitions,
//! because the table was created in the same transaction and has no row yet.
//!
//! A [`FieldCandidate`] is also what the derivation of a table's packed layout reads of a field
//! definition ([`derive_layout`](crate::derive_layout)), through its accessors.

use std::collections::HashSet;

use surrealdb_kvs::value::KVValue;
use surrealdb_kvs::{EncodeContext, Encoded};

use crate::expr::paths::{ID, IN, OUT};
use crate::expr::{Idiom, Kind, Part};
use crate::layout::out_of_range;
use crate::stored::{
	EntryError, SchemaError, Stored, StoredType, canonical, decode_entry, encode_stored,
};
use crate::{Error, StoredFieldDefinition, StoredPermission};

/// What is known of a field's values across the rows of its table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Stored)]
#[stored(since = "4.0")]
pub enum Conformance {
	/// Every row stamped with a generation of the table's ring holds, at every occupied path of
	/// the field, a value the claimed definition coerced.
	#[stored(n = 1)]
	Guaranteed,
}

/// The claim that one field of a table conforms.
#[derive(Clone, Debug, PartialEq, Eq, Stored)]
#[stored(since = "4.0")]
pub struct FieldClaim {
	/// The field, as the `fd` component of its `FieldKey`. Never empty.
	#[stored(n = 2)]
	pub field: String,
	/// The digest of the definition the claim was settled against ([`definition_digest`]).
	#[stored(n = 4)]
	pub definition: i64,
	/// What is claimed.
	#[stored(n = 6)]
	pub conformance: Conformance,
}

impl FieldClaim {
	/// Whether the claim was settled against `definition`: the digests are equal. A definition that
	/// cannot be digested holds no claim.
	pub fn holds_for(&self, definition: &StoredFieldDefinition) -> bool {
		definition_digest(definition).is_ok_and(|digest| digest == self.definition)
	}
}

/// The claims of a table, strictly ascending by field.
///
/// The claims are private: [`TableConformance::new`] and [`TableConformance::with_claim`] are the
/// constructors, and both refuse claims that break the order or name no field.
#[derive(Clone, Debug, Default, PartialEq, Eq, Stored)]
#[stored(since = "4.0", validate)]
pub struct TableConformance {
	/// The claims, strictly ascending by [`FieldClaim::field`].
	#[stored(n = 2)]
	claims: Vec<FieldClaim>,
}

impl TableConformance {
	/// The table's claims holding `claims`, or the refusal of claims that are not strictly
	/// ascending by a nonempty field.
	pub fn new(claims: Vec<FieldClaim>) -> Result<Self, Error> {
		let table = Self {
			claims,
		};
		table.validate().map_err(|e| Error::Entry(EntryError::body(Self::SCHEMA.name, e)))?;
		Ok(table)
	}

	/// The claims, strictly ascending by field.
	pub fn claims(&self) -> &[FieldClaim] {
		&self.claims
	}

	/// The claim on `field`, if the table holds one.
	pub fn claim(&self, field: &str) -> Option<&FieldClaim> {
		let at = self.claims.binary_search_by(|claim| claim.field.as_str().cmp(field)).ok()?;
		self.claims.get(at)
	}

	/// These claims with `claim` inserted, or replacing the claim on the same field. The order is
	/// kept. Refused when `claim` names no field.
	pub fn with_claim(&self, claim: FieldClaim) -> Result<Self, Error> {
		let mut claims = self.claims.clone();
		match claims.binary_search_by(|held| held.field.cmp(&claim.field)) {
			Ok(at) => {
				if let Some(held) = claims.get_mut(at) {
					*held = claim;
				}
			}
			Err(at) => claims.insert(at, claim),
		}
		Self::new(claims)
	}

	/// Checks that the claims are strictly ascending by field and that no field is empty.
	pub fn validate(&self) -> Result<(), SchemaError> {
		let mut previous: Option<&str> = None;
		for claim in &self.claims {
			if claim.field.is_empty() {
				return Err(out_of_range::<FieldClaim>(2));
			}
			if previous.is_some_and(|previous| previous >= claim.field.as_str()) {
				return Err(out_of_range::<Self>(2));
			}
			previous = Some(claim.field.as_str());
		}
		Ok(())
	}
}

/// Written and read at the store's feature level, and refused when it does not pass
/// [`TableConformance::validate`], in either direction.
impl KVValue for TableConformance {
	type KeyContext = ();

	const LEVELLED: bool = true;

	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		Err(Error::Entry(EntryError::Unlevelled {
			ty: <Self as StoredType>::SCHEMA.name,
		})
		.into())
	}

	fn kv_encode_value_at(&self, ctx: &EncodeContext<'_>) -> anyhow::Result<Encoded> {
		self.validate().map_err(|e| Error::Entry(EntryError::body(Self::SCHEMA.name, e)))?;
		Ok(encode_stored(self, ctx).map_err(Error::Entry)?)
	}

	fn kv_canonical(&self) -> anyhow::Result<Vec<u8>> {
		self.validate().map_err(|e| Error::Entry(EntryError::body(Self::SCHEMA.name, e)))?;
		Ok(canonical(self).map_err(Error::Entry)?)
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> anyhow::Result<Self> {
		Ok(decode_entry::<Self>(bytes).map_err(Error::Entry)?)
	}
}

/// The digest of a field definition: FNV-1a-64 over its canonical stored bytes, with the 64 bits
/// read as an `i64`.
///
/// The digest depends on the definition alone, not on the feature level or the grammar it was
/// written in. It is not collision-resistant: it detects that a definition changed and never
/// vouches for one, and a claim never licenses skipping the per-row check. The bits are an `i64`
/// because the stored grammar writes an unsigned value above `i64::MAX` as a decimal.
pub fn definition_digest(definition: &StoredFieldDefinition) -> anyhow::Result<i64> {
	let bytes = definition.kv_canonical()?;
	Ok(i64::from_le_bytes(fnv1a64(&bytes).to_le_bytes()))
}

/// FNV-1a over `bytes`, 64 bits.
fn fnv1a64(bytes: &[u8]) -> u64 {
	const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
	const PRIME: u64 = 0x0000_0100_0000_01b3;
	let mut hash = OFFSET;
	for byte in bytes {
		hash ^= u64::from(*byte);
		hash = hash.wrapping_mul(PRIME);
	}
	hash
}

/// One field definition compiled for the claim rules ([`birth_claims_of`]) and for the derivation
/// of a packed layout ([`derive_layout`](crate::derive_layout)).
///
/// A candidate is a function of the field's name and its stored definition alone, so one compiled
/// from given bytes stands for as long as those bytes do.
#[derive(Clone, Debug)]
pub struct FieldCandidate {
	/// The field, as given.
	field: String,
	/// The field's path.
	idiom: Idiom,
	/// The `TYPE` clause.
	kind: Option<Kind>,
	/// Whether the definition is `FLEXIBLE`.
	flexible: bool,
	/// Whether the definition is `COMPUTED`.
	computed: bool,
	/// The definition's digest.
	digest: i64,
	/// The first part of the path.
	top: Part,
	/// Whether a value can land at this field's places without its definition's coercion, or
	/// after it.
	tainted: bool,
}

impl FieldCandidate {
	/// Compiles `definition`, the stored definition of `field` (the `fd` component of its
	/// `FieldKey`), or answers `None` when its path or `TYPE` clause does not compile, it cannot be
	/// digested, or its path is empty.
	pub fn compile(field: &str, definition: &StoredFieldDefinition) -> Option<Self> {
		let idiom = definition.name.compile().ok()?;
		let kind = match &definition.field_kind {
			Some(text) if idiom.is_id() => Some(text.compile_keyed().ok()?.0),
			Some(text) => Some(text.compile().ok()?),
			None => None,
		};
		let digest = definition_digest(definition).ok()?;
		let top = idiom.first().cloned()?;
		let tainted = definition.readonly
			|| !matches!(definition.create_permission, StoredPermission::Full)
			|| !matches!(definition.update_permission, StoredPermission::Full)
			|| definition.computed.is_some()
			|| definition.reference.is_some()
			|| top == ID[0]
			|| top == IN[0]
			|| top == OUT[0]
			|| !idiom.iter().all(|part| matches!(part, Part::Field(_)));
		Some(Self {
			field: field.to_owned(),
			idiom,
			kind,
			flexible: definition.flexible,
			computed: definition.computed.is_some(),
			digest,
			top,
			tainted,
		})
	}

	/// The field, as the `fd` component of its `FieldKey`.
	pub fn field(&self) -> &str {
		&self.field
	}

	/// The field's path.
	pub fn idiom(&self) -> &Idiom {
		&self.idiom
	}

	/// The `TYPE` clause.
	pub fn kind(&self) -> Option<&Kind> {
		self.kind.as_ref()
	}

	/// Whether the definition is `FLEXIBLE`.
	pub fn flexible(&self) -> bool {
		self.flexible
	}

	/// Whether the field is `COMPUTED`: its value is derived on read and is not stored.
	pub fn computed(&self) -> bool {
		self.computed
	}
}

/// Whether `kind` is exactly `object` or `option<object>`: the kinds under which a named part
/// descends into an object and nothing else.
fn is_object_kind(kind: &Kind) -> bool {
	match kind {
		Kind::Object => true,
		Kind::Either(kinds) => {
			kinds.len() == 2 && kinds.contains(&Kind::None) && kinds.contains(&Kind::Object)
		}
		_ => false,
	}
}

/// The claims a table takes at its birth from its field definitions, each definition given as its
/// `FieldKey` component and stored value.
///
/// Any definition that does not compile claims nothing for the table. Otherwise the claims are
/// those of [`birth_claims_of`] over the compiled definitions.
pub fn birth_claims(definitions: &[(String, StoredFieldDefinition)]) -> Vec<FieldClaim> {
	let mut compiled: Vec<FieldCandidate> = Vec::with_capacity(definitions.len());
	for (field, definition) in definitions {
		let Some(candidate) = FieldCandidate::compile(field, definition) else {
			return Vec::new();
		};
		compiled.push(candidate);
	}
	let candidates: Vec<&FieldCandidate> = compiled.iter().collect();
	birth_claims_of(&candidates)
}

/// The claims a table takes at its birth from its compiled field definitions.
///
/// The fields are grouped by the first part of their path. A group is unchecked as a whole when
/// any member:
///
/// - is `READONLY`, has a create or update permission other than `FULL`, is `COMPUTED` or has a
///   `REFERENCE`, because each lets a value land at the field's path without, or after, the
///   definition's coercion;
/// - sits under `id`, `in` or `out`, which are set outside the field's coercion;
/// - has a path part that is not a named field;
/// - is nested while some proper prefix of its path is not itself a member whose type is exactly
///   `object` or `option<object>`. Under any other parent, a nested definition's coercion can
///   rewrite its ancestor's value after the ancestor's own coercion, so the places the write path
///   coerces differ from the places the claim names.
///
/// Every member with a `TYPE` of a remaining group is claimed. A repeated or empty field name
/// claims nothing for the table. Claims are strictly ascending by field. The work is linear in the
/// number of fields times their path depth, and the order of `candidates` does not matter.
pub fn birth_claims_of(candidates: &[&FieldCandidate]) -> Vec<FieldClaim> {
	// The paths of the members that are exactly an object, against which a nested field's proper
	// prefixes are looked up.
	let objects: HashSet<&[Part]> = candidates
		.iter()
		.copied()
		.filter(|candidate| candidate.kind.as_ref().is_some_and(is_object_kind))
		.map(|candidate| candidate.idiom.0.as_slice())
		.collect();
	// The first parts of the groups that are unchecked as a whole.
	let mut loose: HashSet<&Part> = HashSet::new();
	for candidate in candidates.iter().copied() {
		let structured = (1..candidate.idiom.len()).all(|depth| {
			candidate.idiom.get(..depth).is_some_and(|prefix| objects.contains(prefix))
		});
		if candidate.tainted || !structured {
			loose.insert(&candidate.top);
		}
	}
	let mut claims: Vec<FieldClaim> = candidates
		.iter()
		.copied()
		.filter(|candidate| candidate.kind.is_some() && !loose.contains(&candidate.top))
		.map(|candidate| FieldClaim {
			field: candidate.field.clone(),
			definition: candidate.digest,
			conformance: Conformance::Guaranteed,
		})
		.collect();
	claims.sort_by(|a, b| a.field.cmp(&b.field));
	let duplicated = claims.windows(2).any(|pair| match pair {
		[a, b] => a.field == b.field,
		_ => false,
	});
	if duplicated || claims.iter().any(|claim| claim.field.is_empty()) {
		return Vec::new();
	}
	claims
}

#[cfg(test)]
pub(crate) mod tests {
	use super::*;
	use crate::auth::AuthLimit;
	use crate::generation::tests::{check_frozen, out_of, refusal};
	use crate::schema::StoredDefineDefault;
	use crate::val::TableName;
	use crate::{ExprText, IdiomText, KindText};

	/// The definition of `path` on table `t`, of `kind` when one is given, with nothing else set.
	pub(crate) fn definition(path: &str, kind: Option<&str>) -> StoredFieldDefinition {
		StoredFieldDefinition {
			name: IdiomText::from_raw(path),
			table: TableName::new("t"),
			field_kind: kind.map(KindText::from_raw),
			flexible: false,
			readonly: false,
			value: None,
			assert: None,
			computed: None,
			default: StoredDefineDefault::None,
			select_permission: StoredPermission::Full,
			create_permission: StoredPermission::Full,
			update_permission: StoredPermission::Full,
			comment: None,
			reference: None,
			auth_limit: AuthLimit::new_no_limit(),
			graphql_alias: None,
			graphql_deprecated: None,
			inline: false,
		}
	}

	/// The definitions keyed by their path, as a table's field keys name them.
	pub(crate) fn keyed(
		definitions: Vec<StoredFieldDefinition>,
	) -> Vec<(String, StoredFieldDefinition)> {
		definitions
			.into_iter()
			.map(|definition| (definition.name.as_str().to_owned(), definition))
			.collect()
	}

	/// The fields `definitions` claim at birth.
	fn claimed(definitions: Vec<StoredFieldDefinition>) -> Vec<String> {
		birth_claims(&keyed(definitions)).into_iter().map(|claim| claim.field).collect()
	}

	fn claim(field: &str, definition: i64) -> FieldClaim {
		FieldClaim {
			field: field.to_owned(),
			definition,
			conformance: Conformance::Guaranteed,
		}
	}

	#[test]
	fn fnv1a64_matches_the_published_vectors() {
		assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325);
		assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8c);
		assert_eq!(fnv1a64(b"foobar"), 0x8594_4171_f739_67e8);
	}

	#[test]
	fn a_definition_digest_is_stable_and_follows_the_definition() {
		let name = definition("name", Some("string"));
		let digest = definition_digest(&name).expect("a digest");
		// The digest is stored in claims, so it is fixed for a given definition.
		assert_eq!(digest, -4_598_000_994_399_517_514);
		assert_eq!(definition_digest(&name).expect("a digest"), digest);
		let mut changed = name.clone();
		changed.field_kind = Some(KindText::from_raw("int"));
		assert_ne!(definition_digest(&changed).expect("a digest"), digest);
		let held = claim("name", digest);
		assert!(held.holds_for(&name));
		assert!(!held.holds_for(&changed));
	}

	#[test]
	fn claims_are_strictly_ascending_by_a_nonempty_field() {
		let ty = <TableConformance as StoredType>::SCHEMA.name;
		let claim_ty = <FieldClaim as StoredType>::SCHEMA.name;
		let table = |claims| TableConformance {
			claims,
		};
		assert_eq!(table(vec![claim("a", 1), claim("b", 2)]).validate(), Ok(()));
		let refused = |ty: &'static str| -> Result<(), SchemaError> {
			Err(SchemaError::OutOfRange {
				ty,
				n: 2,
			})
		};
		assert_eq!(table(vec![claim("b", 1), claim("a", 2)]).validate(), refused(ty));
		assert_eq!(table(vec![claim("a", 1), claim("a", 2)]).validate(), refused(ty));
		assert_eq!(table(vec![claim("", 1)]).validate(), refused(claim_ty));
		let unsorted = table(vec![claim("b", 1), claim("a", 2)]);
		assert_eq!(refusal(&unsorted.kv_canonical().unwrap_err()), out_of(ty, 2));
		let bytes = canonical(&unsorted).expect("the grammar holds unsorted claims");
		assert_eq!(
			refusal(&TableConformance::kv_decode_value(&bytes, ()).unwrap_err()),
			out_of(ty, 2)
		);
	}

	#[test]
	fn a_claim_is_inserted_in_order_and_replaces_the_one_on_its_field() {
		let claims = TableConformance::default()
			.with_claim(claim("m", 1))
			.and_then(|table| table.with_claim(claim("a", 2)))
			.and_then(|table| table.with_claim(claim("z", 3)))
			.and_then(|table| table.with_claim(claim("m", 4)))
			.expect("claims on named fields");
		assert_eq!(claims.claims(), [claim("a", 2), claim("m", 4), claim("z", 3)]);
		assert_eq!(claims.validate(), Ok(()));
		assert_eq!(claims.claim("m"), Some(&claim("m", 4)));
		assert_eq!(claims.claim("b"), None);
	}

	#[test]
	fn table_conformance_is_its_frozen_bytes() {
		check_frozen(&TableConformance::default(), &[254, 0, 128, 128, 16, 38, 1, 0, 2, 20, 0]);
		check_frozen(
			&TableConformance {
				claims: vec![claim("age", 42), claim("name", -7)],
			},
			&[
				254, 0, 128, 128, 16, 38, 1, 0, 2, 20, 2, 0, 18, 38, 3, 0, 5, 7, 2, 7, 97, 103,
				101, 4, 170, 6, 38, 1, 0, 1, 0, 38, 3, 0, 6, 9, 2, 7, 110, 97, 109, 101, 4, 4, 249,
				6, 38, 1, 0, 1, 0,
			],
		);
	}

	#[test]
	fn every_typed_field_of_a_plain_table_is_claimed() {
		let fields = vec![
			definition("name", Some("string")),
			definition("age", Some("option<int>")),
			definition("loose", None),
		];
		assert_eq!(claimed(fields), ["age", "name"]);
	}

	#[test]
	fn a_claim_carries_the_digest_of_its_definition() {
		let name = definition("name", Some("string"));
		let claims = birth_claims(&keyed(vec![name.clone()]));
		assert_eq!(claims, [claim("name", definition_digest(&name).expect("a digest"))]);
		assert!(claims.iter().all(|claim| claim.holds_for(&name)));
	}

	#[test]
	fn a_field_written_outside_its_coercion_leaves_its_group_unchecked() {
		let mut readonly = definition("a", Some("string"));
		readonly.readonly = true;
		let mut guarded = definition("b", Some("string"));
		guarded.update_permission = StoredPermission::None;
		let mut created = definition("c", Some("string"));
		created.create_permission = StoredPermission::Specific(ExprText::from_raw("true"));
		let mut computed = definition("d", Some("string"));
		computed.computed = Some(ExprText::from_raw("1"));
		let mut referenced = definition("e", Some("record<u>"));
		referenced.reference = Some(crate::schema::StoredReference {
			on_delete: crate::schema::StoredReferenceDeleteStrategy::Ignore,
		});
		let fields = vec![
			readonly,
			definition("a.x", Some("string")),
			guarded,
			created,
			computed,
			referenced,
			definition("f", Some("string")),
		];
		assert_eq!(claimed(fields), ["f"]);
	}

	#[test]
	fn the_record_id_and_the_ends_of_a_relation_are_never_claimed() {
		let fields = vec![
			definition("id", Some("string")),
			definition("in", Some("record<a>")),
			definition("out", Some("record<b>")),
			definition("kept", Some("int")),
		];
		assert_eq!(claimed(fields), ["kept"]);
	}

	#[test]
	fn a_nested_field_is_claimed_only_under_object_parents() {
		let fields = vec![
			definition("address", Some("object")),
			definition("address.city", Some("string")),
			definition("maybe", Some("option<object>")),
			definition("maybe.inner", Some("int")),
			definition("meta", Some("any")),
			definition("meta.x", Some("string")),
			definition("orphan.x", Some("string")),
		];
		assert_eq!(claimed(fields), ["address", "address.city", "maybe", "maybe.inner"]);
	}

	#[test]
	fn a_path_with_a_part_that_is_not_a_name_leaves_its_group_unchecked() {
		let fields = vec![
			definition("tags", Some("array<string>")),
			definition("tags[*]", Some("string")),
			definition("other", Some("bool")),
		];
		assert_eq!(claimed(fields), ["other"]);
	}

	#[test]
	fn a_repeated_field_or_one_that_does_not_compile_claims_nothing() {
		let fields = vec![definition("name", Some("string")), definition("name", Some("int"))];
		assert_eq!(claimed(fields), Vec::<String>::new());
		let fields =
			vec![definition("name", Some("string")), definition("bad", Some("not a <kind"))];
		assert_eq!(claimed(fields), Vec::<String>::new());
	}

	#[test]
	fn birth_claims_do_not_depend_on_the_order_of_the_definitions() {
		let fields = vec![
			definition("address.city", Some("string")),
			definition("name", Some("string")),
			definition("address", Some("object")),
			definition("age", Some("int")),
		];
		let mut reversed = fields.clone();
		reversed.reverse();
		let forward = birth_claims(&keyed(fields));
		assert_eq!(forward, birth_claims(&keyed(reversed)));
		assert_eq!(forward.len(), 4);
	}
}
