/**
[env]
namespace = true
database = true

[test]
reason = """
An aggregated view keys each group's row by the value its clauses derived when
the records of the group were written. An UPDATE or DELETE finds the group a
record leaves from the record's state under the definitions in force then, so
redefining a `COMPUTED` field the view groups by, while the table holds records,
would send them to a group the view holds no row for. That cannot be repaired
from the view's rows, so `DEFINE FIELD` refuses the change and says to remove
the view around it. The refusal changes nothing: the field still derives what it
did and the records still leave the groups they joined. Removing the view,
redefining the field and defining the view again in one transaction leaves a
view that follows the new derivation.

The table carries an event, so its writes are observed and the `COMPUTED` field
is derived onto the images the view reads.
"""

# The setup.
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
value = "[{ c: 'x', id: v:['x'], n: 1 }]"

# The redefinition is refused, and nothing changes.
[[test.results]]
error = "Cannot change the value the computed field `c` of `t` derives: the aggregated view `v` depends on it, and `t` holds records the view has already grouped by the value derived before. Remove `v`, change the field, then define `v` again"

[[test.results]]
value = "[{ c: 'x' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ c: 'z', id: v:['z'], n: 1 }]"

# The view removed, the field redefined and the view defined again.
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
value = "[{ c: 'y', id: v:['y'], n: 1 }]"

# The record leaves the group it joined, whatever it is written to.
[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ c: 'q', id: v:['q'], n: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE FIELD a ON t TYPE option<string>;
DEFINE FIELD b ON t TYPE option<string>;
DEFINE FIELD c ON t COMPUTED a;
DEFINE EVENT seen ON t THEN {};
DEFINE TABLE v AS SELECT count() AS n, c FROM t GROUP BY c;
CREATE t:1 SET a = 'x', b = 'y' RETURN NONE;
SELECT * FROM v;
-- The table holds a record the view has grouped by `a`.
DEFINE FIELD OVERWRITE c ON t COMPUTED b;
SELECT c FROM t:1;
UPDATE t:1 SET a = 'z' RETURN NONE;
SELECT * FROM v;
BEGIN;
REMOVE TABLE v;
DEFINE FIELD OVERWRITE c ON t COMPUTED b;
DEFINE TABLE v AS SELECT count() AS n, c FROM t GROUP BY c;
COMMIT;
SELECT * FROM v;
UPDATE t:1 SET a = 'w' RETURN NONE;
UPDATE t:1 SET b = 'q' RETURN NONE;
SELECT * FROM v;
DELETE t:1 RETURN NONE;
SELECT * FROM v;
