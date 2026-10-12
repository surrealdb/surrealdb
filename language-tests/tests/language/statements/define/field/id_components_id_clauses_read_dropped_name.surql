/**
[env]
namespace = true
database = true

[test]
reason = """
	The ASSERT, DEFAULT and permissions of the `id` field read the components of
	the record id that field declares, and ALTER FIELD keeps them. A change of
	the record id that drops or renames a name one of them reads is refused, by
	ALTER and by DEFINE OVERWRITE, naming the clause; otherwise the ASSERT would
	refuse every write and the DEFAULT would quietly read NONE. A change that
	keeps the name, or only moves it to another position, is accepted, and the
	clause goes on reading the component by name.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `t`: the ASSERT of the field `id` reads its component `n`"

[[test.results]]
error = "Cannot change the record id of `t`: the ASSERT of the field `id` reads its component `n`"

[[test.results]]
error = "Cannot change the record id of `t`: the ASSERT of the field `id` reads its component `n`"

[[test.results]]
value = "{ events: {  }, fields: { id: 'DEFINE FIELD id ON t TYPE [a: int, n: int] ASSERT n > 0 PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: t:[2, 1], n: 2 }]"

[[test.results]]
error = "Found t:[0, 1] for field `id`, with record `t:[0, 1]`, but field must conform to: n > 0"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: t:[3, 1, 'z'], n: 3, z: 'z' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `d`: the DEFAULT of the field `id` reads its component `n`"

[[test.results]]
error = "Cannot change the record id of `d`: the DEFAULT of the field `id` reads its component `n`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 'k', id: d:[4, 'k'], n: 4 }]"

[[test.results]]
value = "[{ a: 'k', id: d:[7, 'k'], n: 7 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change the record id of `p`: the PERMISSIONS clause of the field `id` reads its component `n`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

-- ASSERT: dropping, renaming and unnaming `n` are refused, and the definition is unchanged.
DEFINE FIELD id ON t TYPE [a: int, n: int] ASSERT n > 0;
ALTER FIELD id ON t TYPE [a: int, m: int];
ALTER FIELD id ON t TYPE [a: int];
ALTER FIELD id ON t TYPE [int, int];
INFO FOR TABLE t;

-- Moving `n` keeps it: the ASSERT reads the component by name.
ALTER FIELD id ON t TYPE [n: int, a: int];
CREATE t SET n = 2, a = 1 RETURN AFTER;
CREATE t SET n = 0, a = 1 RETURN AFTER;
DELETE t;
-- Adding a component keeps `n` too.
ALTER FIELD id ON t TYPE [n: int, a: int, z: string];
CREATE t SET n = 3, a = 1, z = 'z' RETURN AFTER;
-- A definition with no ASSERT reads nothing.
DEFINE FIELD OVERWRITE id ON t TYPE [b: int, a: int, z: string];

-- DEFAULT, through ALTER and through DEFINE OVERWRITE.
DEFINE FIELD id ON d TYPE [a: string, n: int] DEFAULT ['k', $this.n ?? 7];
ALTER FIELD id ON d TYPE [a: string, m: int];
DEFINE FIELD OVERWRITE id ON d TYPE [a: string, m: int] DEFAULT ['k', $this.n ?? 7];
DEFINE FIELD OVERWRITE id ON d TYPE [n: int, a: string] DEFAULT [$this.n ?? 7, 'k'];
CREATE d SET n = 4 RETURN AFTER;
CREATE d RETURN AFTER;

-- The permissions of the field.
DEFINE FIELD id ON p TYPE [a: string, n: int] PERMISSIONS FOR select FULL, FOR create WHERE n > 0;
ALTER FIELD id ON p TYPE [a: string, m: int];
ALTER FIELD id ON p TYPE [a: string, n: int, m: int];
REMOVE FIELD id ON p;
