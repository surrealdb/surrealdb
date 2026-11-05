/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A statement in a later transaction sees every definition committed before
	that transaction opened: those of the request's own earlier statements.
	Here an index is observed through its effect on writes and reads: a unique
	index rejects a duplicate created after it, an index defined over existing
	rows is kept up to date by later writes, and a removed index stops
	constraining later writes. Use is proven by results, never by plans.
"""

[[test.results]]
value = "[{ a: 1, b: 5, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Database index `u` already contains 1, with record `t:1`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ b: 5, id: t:3 }]"

[[test.results]]
value = "[{ id: t:1 }, { id: t:3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: t:4 }]"
*/

CREATE t:1 SET a = 1, b = 5;
DEFINE INDEX u ON t FIELDS a UNIQUE;
CREATE t:2 SET a = 1;

DEFINE INDEX i ON t FIELDS b;
CREATE t:3 SET b = 5;
SELECT id FROM t WHERE b = 5 ORDER BY id;

REMOVE INDEX u ON t;
CREATE t:4 SET a = 1;
