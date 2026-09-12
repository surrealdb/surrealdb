/**
[env]
namespace = true
database = true

[test]
reason = """
Decorrelating a membership conjunct that leaves a residual moves the residual
onto the join, where it is judged against an object carrying the alias alone —
so every name but the alias answers `NONE`. That stands in for the per-row
form only where the probe rows are objects, on which those names answer `NONE`
too.

A source is not proof of that. Rows read out of the store are records, and a
record is an object, but `FROM <expr>` and `FROM $rows` yield whatever the
expression holds. A geometry is the case that shows it: it is not an object,
yet `type` and `coordinates` answer off one, so the substitute row answers
differently and admits a row the per-row form refuses.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[7]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[s:1]"
*/

DEFINE TABLE r SCHEMALESS;
CREATE r:1 SET v = 1, kind = 'Point' RETURN NONE;

-- A geometry answers `type`, so the per-row form finds `kind != $parent.type`
-- false and the subquery empty. Judged against an alias-only row instead,
-- `type` would answer `NONE` and the row would be admitted.
SELECT * FROM array::shuffle([{ type: 'Point', coordinates: [1, 2] }])[0]
	WHERE 1 IN (SELECT VALUE v FROM r WHERE kind != $parent.type);

-- A source whose rows are objects answers the same either way.
SELECT * FROM [{ type: 'Point', c: 9 }][0]
	WHERE 1 IN (SELECT VALUE v FROM r WHERE kind != $parent.type);

-- A scalar has no field to read, so both forms answer `NONE` and admit it.
SELECT * FROM [7][0] WHERE 1 IN (SELECT VALUE v FROM r WHERE kind != $parent.type);

-- Rows out of the store are records, so a residual over them still lifts.
DEFINE TABLE s SCHEMALESS;
CREATE s:1 SET n = 1, kind = 'Point' RETURN NONE;
CREATE s:2 SET n = 2, kind = 'Line' RETURN NONE;
SELECT VALUE id FROM s WHERE n IN (SELECT VALUE v FROM r WHERE kind = $parent.kind);
