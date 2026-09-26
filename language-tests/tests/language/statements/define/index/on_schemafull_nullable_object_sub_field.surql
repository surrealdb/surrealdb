/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ address: NULL, id: person:1 }]"

[[test.results]]
value = "[{ address: { city: 'Berlin' }, id: person:2 }]"

[[test.results]]
value = "[person:2]"

[[test.results]]
value = "[NULL]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ address: NULL, id: t_null_last:1 }]"

[[test.results]]
value = "[{ address: { city: 'Paris' }, id: t_null_last:2 }]"

[[test.results]]
value = "[t_null_last:2]"

[[test.results]]
value = "[NULL]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ address: NULL, id: t_opt_null:1 }]"

[[test.results]]
value = "[{ address: { city: 'Rome' }, id: t_opt_null:2 }]"

[[test.results]]
value = "[{ id: t_opt_null:3 }]"

[[test.results]]
value = "[t_opt_null:2]"

[[test.results]]
value = "[NULL]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ address: NULL, id: t_null_object:1 }]"

[[test.results]]
error = "Found field 'address.city', but no such field exists for table 't_null_object'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found field 'address.city', but no such field exists for table 't_opt_object'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ address: NULL, id: t_unique:1 }]"

[[test.results]]
value = "[{ address: NULL, id: t_unique:2 }]"

[[test.results]]
value = "[{ address: { city: 'Lisbon' }, id: t_unique:3 }]"

[[test.results]]
error = "Database index `t_unique_city` already contains 'Lisbon', with record `t_unique:3`"

[[test.results]]
value = "[t_unique:3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The field 'address.city' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The field 'address.city' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot set field `address.city` with type `string` as it mismatched with field `address` with type `null | { city: string }`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot set field `address.city` with type `string` as it mismatched with field `address` with type `{ city: string } | null`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot set field `address.city` with type `string` as it mismatched with field `address` with type `none | null | { city: string }`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot set field `address.city` with type `string` as it mismatched with field `address` with type `null | object`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot set field `address.city` with type `string` as it mismatched with field `address` with type `null | { city: string } | { town: string }`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot set field `address.city` with type `string` as it mismatched with field `address` with type `null | string | { city: string }`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t_field_opt:1 }]"

[[test.results]]
value = "[{ address: { city: 'Madrid' }, id: t_field_opt:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot set field `f.anything` with type `record<nonexistent_table>` as it mismatched with field `f` with type `none | null`"

[[test.results]]
error = "Cannot set field `f.city` with type `string` as it mismatched with field `f` with type `none | null`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: NULL, id: t_opt_bare_null:1 }]"

[[test.results]]
value = "[{ id: t_opt_bare_null:2 }]"

[[test.results]]
value = "[NULL]"

[[test.results]]
value = "[]"

*/
-- Issue #7524: on a SCHEMAFULL table, `DEFINE INDEX` on a sub-field of a
-- nullable object (`null | { ... }`) must work exactly as it already does for
-- `option<{ ... }>` (which desugars to `none | { ... }`).
--
-- The index gate (`Kind::allows_sub_fields`) treats a `null` union variant like
-- a `none` variant: it carries no sub-fields of its own, and *reading* a
-- sub-field through it yields `NONE` rather than an error, so it never makes
-- the indexed path invalid. Every other variant must still be object-like, so a
-- union mixing an object with a concrete scalar stays rejected.
--
-- That exemption is read-side only. `DEFINE FIELD address.city` over a
-- `null | { ... }` parent stays REJECTED (Part 2): it is out of scope for
-- #7524, and accepting it would create a schema that cannot store `NULL` in the
-- parent at all, because the document writer skips child fields only when the
-- parent kind `can_be_none()` (`none | any`, deliberately not `null`), so it
-- would descend through a `NULL` parent and fail to coerce the resulting
-- `NONE`.

-- ============================================================================
-- Part 1: DEFINE INDEX over a sub-field of a nullable object parent,
--         exercised with real data including `address = NULL`.
-- ============================================================================

-- 1a. `null | { city: string }` -- the exact schema from the issue.
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD address ON person TYPE null | { city: string } DEFAULT NULL;
DEFINE INDEX person_city ON person FIELDS address.city;
CREATE person:1 SET address = NULL;
CREATE person:2 SET address = { city: 'Berlin' };
SELECT VALUE id FROM person WHERE address.city = 'Berlin';
SELECT VALUE address FROM person:1;

-- 1b. The reversed union `{ city: string } | null` behaves identically.
DEFINE TABLE t_null_last SCHEMAFULL;
DEFINE FIELD address ON t_null_last TYPE { city: string } | null DEFAULT NULL;
DEFINE INDEX t_null_last_city ON t_null_last FIELDS address.city;
CREATE t_null_last:1 SET address = NULL;
CREATE t_null_last:2 SET address = { city: 'Paris' };
SELECT VALUE id FROM t_null_last WHERE address.city = 'Paris';
SELECT VALUE address FROM t_null_last:1;

-- 1c. `option<null | { city: string }>`, i.e. `none | null | { city: string }`:
-- a `NULL` parent, an object parent and a missing parent all coexist.
DEFINE TABLE t_opt_null SCHEMAFULL;
DEFINE FIELD address ON t_opt_null TYPE option<null | { city: string }>;
DEFINE INDEX t_opt_null_city ON t_opt_null FIELDS address.city;
CREATE t_opt_null:1 SET address = NULL;
CREATE t_opt_null:2 SET address = { city: 'Rome' };
CREATE t_opt_null:3;
SELECT VALUE id FROM t_opt_null WHERE address.city = 'Rome';
SELECT VALUE address FROM t_opt_null:1;
SELECT VALUE address FROM t_opt_null:3;

-- 1d. A `null` union over the non-literal `object` kind also resolves the
-- index. Storing an actual object under it then requires an explicit sub-field
-- definition -- but that is pre-existing, unrelated behaviour: `option<object>`
-- rejects the same write with the same error.
DEFINE TABLE t_null_object SCHEMAFULL;
DEFINE FIELD address ON t_null_object TYPE null | object DEFAULT NULL;
DEFINE INDEX t_null_object_city ON t_null_object FIELDS address.city;
CREATE t_null_object:1 SET address = NULL;
CREATE t_null_object:2 SET address = { city: 'Oslo' };
DEFINE TABLE t_opt_object SCHEMAFULL;
DEFINE FIELD address ON t_opt_object TYPE option<object>;
DEFINE INDEX t_opt_object_city ON t_opt_object FIELDS address.city;
CREATE t_opt_object:1 SET address = { city: 'Oslo' };

-- 1e. Index kind is irrelevant: UNIQUE over the same sub-field. Two `NULL`
-- parents both read `NONE` at `address.city` and must not collide, exactly as
-- two `NONE` parents would not; two real cities still do collide.
DEFINE TABLE t_unique SCHEMAFULL;
DEFINE FIELD address ON t_unique TYPE null | { city: string } DEFAULT NULL;
DEFINE INDEX t_unique_city ON t_unique FIELDS address.city UNIQUE;
CREATE t_unique:1 SET address = NULL;
CREATE t_unique:2 SET address = NULL;
CREATE t_unique:3 SET address = { city: 'Lisbon' };
CREATE t_unique:4 SET address = { city: 'Lisbon' };
SELECT VALUE id FROM t_unique WHERE address.city = 'Lisbon';

-- 1f. Negative: a union mixing an object with a concrete non-object variant
-- does not permit sub-field access, so the index is still rejected.
DEFINE TABLE t_mixed SCHEMAFULL;
DEFINE FIELD address ON t_mixed TYPE null | string | { city: string } DEFAULT NULL;
DEFINE INDEX t_mixed_city ON t_mixed FIELDS address.city;

-- 1g. Negative: a bare `null` parent is not a union, carries no sub-fields, and
-- is rejected.
DEFINE TABLE t_bare_null SCHEMAFULL;
DEFINE FIELD address ON t_bare_null TYPE null DEFAULT NULL;
DEFINE INDEX t_bare_null_city ON t_bare_null FIELDS address.city;

-- ============================================================================
-- Part 2: DEFINE FIELD on a sub-field of a nullable parent STILL ERRORS.
--
-- This is the half that must NOT be relaxed. Were any of these accepted, the
-- table could no longer store `NULL` in the parent: the writer would descend
-- through the `NULL` and fail to coerce the `NONE` it reads for the child.
-- ============================================================================

DEFINE TABLE t_field_null SCHEMAFULL;
DEFINE FIELD address ON t_field_null TYPE null | { city: string } DEFAULT NULL;
DEFINE FIELD address.city ON t_field_null TYPE string;

DEFINE TABLE t_field_null_last SCHEMAFULL;
DEFINE FIELD address ON t_field_null_last TYPE { city: string } | null DEFAULT NULL;
DEFINE FIELD address.city ON t_field_null_last TYPE string;

DEFINE TABLE t_field_opt_null SCHEMAFULL;
DEFINE FIELD address ON t_field_opt_null TYPE option<null | { city: string }>;
DEFINE FIELD address.city ON t_field_opt_null TYPE string;

DEFINE TABLE t_field_null_object SCHEMAFULL;
DEFINE FIELD address ON t_field_null_object TYPE null | object DEFAULT NULL;
DEFINE FIELD address.city ON t_field_null_object TYPE string;

-- With two distinct object variants the sub-field must exist in *every* object
-- variant; `city` is absent from `{ town: string }`.
DEFINE TABLE t_two_objects SCHEMAFULL;
DEFINE FIELD address ON t_two_objects TYPE null | { city: string } | { town: string } DEFAULT NULL;
DEFINE FIELD address.city ON t_two_objects TYPE string;

-- A union mixing an object with a concrete scalar stays rejected here too.
DEFINE TABLE t_mixed_field SCHEMAFULL;
DEFINE FIELD address ON t_mixed_field TYPE null | string | { city: string } DEFAULT NULL;
DEFINE FIELD address.city ON t_mixed_field TYPE string;

-- Control: the `none` union is unchanged and still accepts the sub-field
-- definition, and the round-trip works, because the writer skips child fields
-- for a `NONE` parent.
DEFINE TABLE t_field_opt SCHEMAFULL;
DEFINE FIELD address ON t_field_opt TYPE option<{ city: string }>;
DEFINE FIELD address.city ON t_field_opt TYPE string;
CREATE t_field_opt:1;
CREATE t_field_opt:2 SET address = { city: 'Madrid' };

-- ============================================================================
-- Part 3: the degenerate `option<null>` parent (`none | null`).
--
-- It must not accept a sub-field DEFINITION of any kind -- before the fix it
-- did, which let a `record<nonexistent_table>` child be declared on a field
-- whose only successful insert omits it entirely.
--
-- The read-side index gate does still accept a sub-field path here, as a
-- consequence of the `null` exemption. That is inert: the field can only ever
-- hold `NONE` or `NULL`, both of which read as `NONE` at any sub-field, so the
-- index stores nothing, and no child field can be declared to disagree with it.
-- ============================================================================

DEFINE TABLE t_opt_bare_null SCHEMAFULL;
DEFINE FIELD f ON t_opt_bare_null TYPE option<null>;
DEFINE FIELD f.anything ON t_opt_bare_null TYPE record<nonexistent_table>;
DEFINE FIELD f.city ON t_opt_bare_null TYPE string;
DEFINE INDEX t_opt_bare_null_idx ON t_opt_bare_null FIELDS f.city;
CREATE t_opt_bare_null:1 SET f = NULL;
CREATE t_opt_bare_null:2;
SELECT VALUE f FROM t_opt_bare_null:1;
SELECT VALUE id FROM t_opt_bare_null WHERE f.city = 'Berlin';
