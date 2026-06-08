/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "Right-position `[v] IN field` has different runtime semantics from `field IN [v]` (it's `[v].contains(field)`, not a containment of `field` in a literal array). The strip-path normalisation that rewrites `field IN [v]` to `field = v` must NOT apply when the idiom is on the right — otherwise the IndexScan returns rows for which the original predicate evaluates to false, silently corrupting results."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ field: 5, id: t:r1 }]"

[[test.results]]
value = "[{ field: 7, id: t:r2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id]\n    Filter [ctx: Db] [predicate: [5] INSIDE field]\n        IndexScan [ctx: Db] [index: idx_field, access: = 5, direction: Forward]\n''''

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ field: 5, group: 'g', id: t:r3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: id]\n    Filter [ctx: Db] [predicate: [5] INSIDE field]\n        IndexScan [ctx: Db] [index: idx_ab, access: [5, 'g'], direction: Forward]\n"'''
*/
DEFINE TABLE t;
DEFINE INDEX idx_field ON t FIELDS field;

CREATE t:r1 SET field = 5;
CREATE t:r2 SET field = 7;

-- `[5] IN field` is `[5].contains(field)`. For scalar `field = 5` that
-- is `[5].contains(5)` which is false; for `field = 7` it is also
-- false. So the AND can match no rows.
SELECT id FROM t WHERE field = 5 AND [5] IN field;
EXPLAIN SELECT id FROM t WHERE field = 5 AND [5] IN field;

-- Compound-prefix variant (forced via WITH INDEX so the strip path
-- runs through the Compound arm of `IndexConditionMatcher`): the
-- Right-position IN must not be stripped against a prefix value
-- either.
DEFINE INDEX idx_ab ON t FIELDS field, group;
CREATE t:r3 SET field = 5, group = 'g';

SELECT id FROM t WITH INDEX idx_ab WHERE field = 5 AND group = 'g' AND [5] IN field;
EXPLAIN SELECT id FROM t WITH INDEX idx_ab WHERE field = 5 AND group = 'g' AND [5] IN field;
