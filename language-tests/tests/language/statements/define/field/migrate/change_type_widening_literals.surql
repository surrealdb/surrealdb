/**
[test]
reason = "Change 3 on literal unions: T to T | U is a widening whatever T is, so adding a member to a union of literals (an enum gaining a value) is accepted by an enforced table with rows, keeps a Guaranteed field Guaranteed and mints nothing; the same holds with the new member first."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "10"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ n: 'DEFINE FIELD n ON p TYPE 1 | 2 | 3 PERMISSIONS FULL', s: "DEFINE FIELD s ON p TYPE 'x' | 'y' | 'z' PERMISSIONS FULL", t: "DEFINE FIELD t ON p TYPE 'z' | 'x' | 'y' PERMISSIONS FULL" }"""

[[test.results]]
value = "13"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: p:1, n: 1, ni: true, s: 'x', t: 'x' }, { id: p:2, n: 2, ni: true, s: 'y', t: 'y' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: p:3, n: 3, s: 'z', t: 'z' }]"
*/
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD s ON p TYPE string;
DEFINE FIELD n ON p TYPE string;
DEFINE FIELD t ON p TYPE string;
CREATE p:1 SET s = 'x', n = '1', t = 'x' RETURN NONE;
CREATE p:2 SET s = 'y', n = '2', t = 'y' RETURN NONE;
ALTER FIELD s ON p TYPE 'x' | 'y' MIGRATE EXISTING ROWS AS s_enum USING $value FALLBACK 'x';
ALTER FIELD n ON p TYPE 1 | 2 MIGRATE EXISTING ROWS AS n_enum USING <int>$value FALLBACK 1;
ALTER FIELD t ON p TYPE 'x' | 'y' MIGRATE EXISTING ROWS AS t_enum USING $value FALLBACK 'x';
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
-- A union that gains a member.
ALTER FIELD s ON p TYPE 'x' | 'y' | 'z';
ALTER FIELD n ON p TYPE 1 | 2 | 3;
-- A member added in front.
ALTER FIELD t ON p TYPE 'z' | 'x' | 'y';
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
SELECT id, s, n, t, type::is_int(n) AS ni FROM p ORDER BY id;
CREATE p:3 SET s = 'z', n = 3, t = 'z' RETURN NONE;
SELECT id, s, n, t FROM p:3;
