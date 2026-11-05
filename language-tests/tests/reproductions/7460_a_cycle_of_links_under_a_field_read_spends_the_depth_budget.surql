/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 7460
reason = """
A field read or a destructure through a record link fetches the linked record
and computes its COMPUTED fields, which may read through a link again. Over a
set of links each element is read this way, as over an array. Each such fetch
spends a unit of the computation-depth budget, so a cycle of links through a
COMPUTED field read, over a set, an array or a single link, fails with the depth
error rather than recursing until the worker's stack overflows.
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
*/
DEFINE FIELD friends ON person TYPE set<record<person>> DEFAULT <set>[];
DEFINE FIELD friend_docs ON person COMPUTED friends.friend_docs;
CREATE person:1 SET friends = <set>[person:2] RETURN NONE;
CREATE person:2 SET friends = <set>[person:1] RETURN NONE;
SELECT id, friend_docs FROM person:1;

DEFINE FIELD friends ON animal TYPE array<record<animal>> DEFAULT [];
DEFINE FIELD friend_docs ON animal COMPUTED friends.friend_docs;
CREATE animal:1 SET friends = [animal:2] RETURN NONE;
CREATE animal:2 SET friends = [animal:1] RETURN NONE;
SELECT id, friend_docs FROM animal:1;

DEFINE FIELD friend ON node TYPE option<record<node>>;
DEFINE FIELD friend_doc ON node COMPUTED friend.friend_doc;
CREATE node:1 SET friend = node:2 RETURN NONE;
CREATE node:2 SET friend = node:1 RETURN NONE;
SELECT id, friend_doc FROM node:1;

DEFINE FIELD friends ON pet TYPE set<record<pet>> DEFAULT <set>[];
DEFINE FIELD friend_docs ON pet COMPUTED friends.{ friend_docs };
CREATE pet:1 SET friends = <set>[pet:2] RETURN NONE;
CREATE pet:2 SET friends = <set>[pet:1] RETURN NONE;
SELECT id, friend_docs FROM pet:1;
