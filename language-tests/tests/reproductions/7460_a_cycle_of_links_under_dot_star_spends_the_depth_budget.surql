/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 7460
reason = """
`.*` over a record link fetches the linked record and computes its COMPUTED
fields, which may take `.*` over a link again. Each such fetch spends a unit of
the computation-depth budget, so a cycle of links through a COMPUTED `.*` field,
over a set, an array or a single link, fails with the depth error rather than
recursing until the worker's stack overflows.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Reached excessive computation depth due to functions, subqueries, or computed values"
*/
DEFINE FIELD friends ON person TYPE set<record<person>> DEFAULT <set>[];
DEFINE FIELD friend_docs ON person COMPUTED friends.*;
CREATE person:1 SET friends = <set>[person:2] RETURN NONE;
CREATE person:2 SET friends = <set>[person:1] RETURN NONE;
SELECT id, friend_docs FROM person:1;

DEFINE FIELD friends ON animal TYPE array<record<animal>> DEFAULT [];
DEFINE FIELD friend_docs ON animal COMPUTED friends.*;
CREATE animal:1 SET friends = [animal:2] RETURN NONE;
CREATE animal:2 SET friends = [animal:1] RETURN NONE;
SELECT id, friend_docs FROM animal:1;

DEFINE FIELD me ON node COMPUTED self.*;
CREATE node:1 SET self = node:1 RETURN NONE;
SELECT id, me FROM node:1;
