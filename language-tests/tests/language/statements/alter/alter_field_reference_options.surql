/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "ALTER FIELD refuses a REFERENCE that DEFINE FIELD refuses, judged on the definition the statement forms: a REFERENCE over a kind that holds something other than records, whether the statement sets the REFERENCE or changes the type under one it keeps, and a REFERENCE on a nested field. An export writes each definition as DEFINE FIELD, so a REFERENCE that ALTER FIELD stored would be refused by the import of the export. The refusals are made on both routes: a statement whose field and table are written and checked when planned, and one whose table is named by an evaluated expression or that follows a change to the catalog, which the host alters."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

# DEFINE FIELD refuses each definition.

[[test.results]]
error = "Cannot use the `REFERENCE` keyword with `TYPE int`. Specify only a `record` type, or a type containing only records, instead."

[[test.results]]
error = "Cannot use the `REFERENCE` keyword with `TYPE none | int`. Specify only a `record` type, or a type containing only records, instead."

[[test.results]]
error = "Cannot use the `REFERENCE` keyword on nested field `o.y`. Specify a referencing field at the root level instead."

# Written names.

[[test.results]]
error = "Cannot use the `REFERENCE` keyword with `TYPE int`. Specify only a `record` type, or a type containing only records, instead."

[[test.results]]
error = "Cannot use the `REFERENCE` keyword with `TYPE none | int`. Specify only a `record` type, or a type containing only records, instead."

[[test.results]]
error = "Cannot use the `REFERENCE` keyword on nested field `o.y`. Specify a referencing field at the root level instead."

# A table named by an evaluated expression.

[[test.results]]
error = "Cannot use the `REFERENCE` keyword with `TYPE int`. Specify only a `record` type, or a type containing only records, instead."

[[test.results]]
error = "Cannot use the `REFERENCE` keyword with `TYPE none | int`. Specify only a `record` type, or a type containing only records, instead."

[[test.results]]
error = "Cannot use the `REFERENCE` keyword on nested field `o.y`. Specify a referencing field at the root level instead."

# After a change to the catalog in the same block.

[[test.results]]
error = "Cannot use the `REFERENCE` keyword with `TYPE int`. Specify only a `record` type, or a type containing only records, instead."

[[test.results]]
error = "Cannot use the `REFERENCE` keyword with `TYPE none | int`. Specify only a `record` type, or a type containing only records, instead."

[[test.results]]
error = "Cannot use the `REFERENCE` keyword on nested field `o.y`. Specify a referencing field at the root level instead."

# Nothing refused was stored.

[[test.results]]
value = "{ n: 'DEFINE FIELD n ON t TYPE int PERMISSIONS FULL' }"

[[test.results]]
value = '''{ o: 'DEFINE FIELD o ON u TYPE none | object PERMISSIONS FULL', "o.y": 'DEFINE FIELD o.y ON u TYPE none | record<u> PERMISSIONS FULL', r: 'DEFINE FIELD r ON u TYPE none | record<u> REFERENCE ON DELETE IGNORE PERMISSIONS FULL' }'''

# A definition the statement leaves holding only records is accepted, on each route.

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ n: 'DEFINE FIELD n ON t TYPE none | record<t> REFERENCE ON DELETE CASCADE PERMISSIONS FULL' }"

[[test.results]]
value = '''{ o: 'DEFINE FIELD o ON u TYPE none | object PERMISSIONS FULL', "o.y": 'DEFINE FIELD o.y ON u TYPE none | record<u> PERMISSIONS FULL', r: 'DEFINE FIELD r ON u TYPE none | record<t | u> REFERENCE ON DELETE IGNORE PERMISSIONS FULL' }'''

*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD n ON t TYPE int;
DEFINE TABLE u SCHEMAFULL;
DEFINE FIELD r ON u TYPE option<record<u>> REFERENCE;
DEFINE FIELD o ON u TYPE option<object>;
DEFINE FIELD o.y ON u TYPE option<record<u>>;

-- DEFINE FIELD refuses a REFERENCE over a kind that is not only records, and
-- one on a nested field.
DEFINE FIELD m ON t TYPE int REFERENCE;
DEFINE FIELD OVERWRITE r ON u TYPE option<int> REFERENCE;
DEFINE FIELD OVERWRITE o.y ON u TYPE option<record<u>> REFERENCE;

-- ALTER FIELD refuses the same definitions: setting REFERENCE, changing the
-- type under a REFERENCE it keeps, and setting REFERENCE on a nested field.
ALTER FIELD n ON t REFERENCE;
ALTER FIELD r ON u TYPE option<int>;
ALTER FIELD o.y ON u REFERENCE;

-- The same, with the table named by an expression the statement evaluates.
ALTER FIELD n ON ('' + 't') REFERENCE;
ALTER FIELD r ON ('' + 'u') TYPE option<int>;
ALTER FIELD o.y ON ('' + 'u') REFERENCE;

-- The same, after a statement in the block that may change the catalog.
{ DEFINE TABLE IF NOT EXISTS t; ALTER FIELD n ON t REFERENCE; };
{ DEFINE TABLE IF NOT EXISTS u; ALTER FIELD r ON u TYPE option<int>; };
{ DEFINE TABLE IF NOT EXISTS u; ALTER FIELD o.y ON u REFERENCE; };

(INFO FOR TABLE t).fields;
(INFO FOR TABLE u).fields;

-- The check judges the definition the statement forms, so a REFERENCE set
-- with a record type or over one, a type changed while dropping the
-- REFERENCE, and a record type changed under a REFERENCE it keeps are
-- accepted.
ALTER FIELD n ON t TYPE option<record<t>> REFERENCE;
ALTER FIELD n ON t REFERENCE ON DELETE CASCADE;
ALTER FIELD r ON ('' + 'u') TYPE option<int> DROP REFERENCE;
{ DEFINE TABLE IF NOT EXISTS u; ALTER FIELD r ON u TYPE option<record<t>> REFERENCE; };
ALTER FIELD r ON u TYPE option<record<t | u>>;

(INFO FOR TABLE t).fields;
(INFO FOR TABLE u).fields;
