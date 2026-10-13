/**
[env]
namespace = true
database = true

[test]
reason = """
A `COMPUTED` field is never stored, so the record an updating statement returns
has to re-derive it from the values that were just written. That must hold no
matter what the statement did to reach the record: with or without a `WHERE`
condition, with or without a data clause, and for `RETURN AFTER` and
`RETURN DIFF` alike.

The condition and the data clause read a pre-mutation snapshot that has the
computed fields populated, so this is the point where those snapshot values must
be discarded rather than reused: `dbl` on the returned record is derived from the
new `a`, and a `RETURN DIFF` must not report the field as removed.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 5, dbl: 10, id: r:1 }]"

[[test.results]]
value = "[{ a: 5, dbl: 10, id: r:1, note: 'x' }]"

[[test.results]]
value = "[{ a: 5, dbl: 10, id: r:2 }]"

[[test.results]]
value = "[{ a: 5, dbl: 10, id: r:2, note: 'x' }]"

[[test.results]]
value = "[{ a: 5, dbl: 10, id: r:3 }]"

[[test.results]]
value = "[{ a: 5, dbl: 10, id: r:3 }]"

[[test.results]]
value = "[{ a: 5, dbl: 10, id: r:4 }]"

[[test.results]]
value = "[[{ op: 'add', path: '/note', value: 'x' }]]"

[[test.results]]
value = "[{ a: 5, dbl: 10, id: r:5 }]"

[[test.results]]
value = "[{ a: 9, dbl: 18, id: r:5 }]"

[[test.results]]
value = "[r:1, r:2, r:3, r:4, r:5]"
*/

DEFINE TABLE r SCHEMALESS;
DEFINE FIELD a   ON r TYPE number;
DEFINE FIELD dbl ON r COMPUTED a * 2;

-- The computed field is present on the returned record whether or not the
-- statement carries a condition.
CREATE r:1 SET a = 5;
UPDATE r:1 SET note = "x" WHERE true RETURN AFTER;
CREATE r:2 SET a = 5;
UPDATE r:2 SET note = "x" RETURN AFTER;

-- A condition with no data clause reaches the same point by a different route.
CREATE r:3 SET a = 5;
UPDATE r:3 WHERE true;

-- `RETURN DIFF` must not claim the computed field was removed.
CREATE r:4 SET a = 5;
UPDATE r:4 SET note = "x" WHERE true RETURN DIFF;

-- Assigning the field a computed field derives from re-derives it: `dbl` is 18,
-- not the 10 the pre-mutation snapshot held.
CREATE r:5 SET a = 5;
UPDATE r:5 SET a = 9 WHERE true RETURN AFTER;

-- Nothing is stored for the computed field either way.
SELECT VALUE id FROM r WHERE record::exists(id) ORDER BY id;
