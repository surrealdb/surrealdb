/**
[env]
namespace = true
database = true

[test]
reason = """
An aggregated view depends on a `COMPUTED` field when any clause of it spells the
field, or spells a `COMPUTED` field that reads it, directly or through others: in
its groups, in the arguments its aggregates accumulate, or in its condition. A
change to what such a field derives is refused while the table holds records, as
a change to a field the view groups by is. An aggregate over a derived value
would otherwise take the old derivation back out of its stats with the new one.

A `COMPUTED` field that reads the record by a name it computes may read any
field, so a view that spells it may depend on every field that changes.

The tables carry an event, so their writes are observed and `COMPUTED` fields
are derived onto the images the views read.
"""

# `t`: the view sums `c`, which reads `a`, which reads `src`.
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

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: v:['x'], k: 'x', total: 2f }]"

# `a` is not spelled by the view, but `c` reads it.
[[test.results]]
error = "Cannot change the value the computed field `a` of `t` derives: the aggregated view `v` depends on it, and `t` holds records the view has already grouped by the value derived before. Remove `v`, change the field, then define `v` again"

# `src` is stored, and a field the view does not depend on is free to change.
[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: v:['x'], k: 'x', total: 8f }]"

# `s`: the condition of the view reads `big`.
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
value = "[]"

[[test.results]]
value = "[{ id: bigs:[], n: 1 }]"

[[test.results]]
error = "Cannot change the value the computed field `big` of `s` derives: the aggregated view `bigs` depends on it, and `s` holds records the view has already grouped by the value derived before. Remove `bigs`, change the field, then define `bigs` again"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

# `o`: the view groups by `d`, which reads the record by a name it computes.
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

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ d: 'x', id: ov:['x'], n: 1 }]"

[[test.results]]
error = "Cannot change the value the computed field `c` of `o` derives: the aggregated view `ov` may depend on it, through a read of fields of the record by names that cannot be determined, and `o` holds records the view has already grouped by the value derived before. Remove `ov`, change the field, then define `ov` again"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ d: 'z', id: ov:['z'], n: 1 }]"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE FIELD src ON t TYPE option<int>;
DEFINE FIELD k ON t TYPE option<string>;
DEFINE FIELD a ON t COMPUTED src;
DEFINE FIELD c ON t COMPUTED a * 2;
DEFINE EVENT seen ON t THEN {};
DEFINE TABLE v AS SELECT k, math::sum(c) AS total FROM t GROUP BY k;
CREATE t:1 SET src = 1, k = 'x' RETURN NONE;
SELECT * FROM v;
DEFINE FIELD OVERWRITE a ON t COMPUTED src + 1;
DEFINE FIELD e ON t COMPUTED k;
DEFINE FIELD OVERWRITE src ON t TYPE option<int> COMMENT 'stored';
UPDATE t:1 SET src = 4 RETURN NONE;
SELECT * FROM v;

DEFINE TABLE s SCHEMALESS;
DEFINE FIELD n ON s TYPE option<int>;
DEFINE FIELD big ON s COMPUTED n > 10;
DEFINE EVENT seen ON s THEN {};
DEFINE TABLE bigs AS SELECT count() AS n FROM s WHERE big GROUP ALL;
CREATE s:1 SET n = 20 RETURN NONE;
SELECT * FROM bigs;
DEFINE FIELD OVERWRITE big ON s COMPUTED n > 30;
UPDATE s:1 SET n = 25 RETURN NONE;
DELETE s:1 RETURN NONE;

DEFINE TABLE o SCHEMALESS;
DEFINE FIELD a ON o TYPE option<string>;
DEFINE FIELD b ON o TYPE option<string>;
DEFINE FIELD c ON o COMPUTED a;
DEFINE FIELD d ON o COMPUTED type::field(IF true { 'c' } ELSE { 'x' });
DEFINE EVENT seen ON o THEN {};
DEFINE TABLE ov AS SELECT count() AS n, d FROM o GROUP BY d;
CREATE o:1 SET a = 'x', b = 'y' RETURN NONE;
SELECT * FROM ov;
DEFINE FIELD OVERWRITE c ON o COMPUTED b;
UPDATE o:1 SET a = 'z' RETURN NONE;
SELECT * FROM ov;
