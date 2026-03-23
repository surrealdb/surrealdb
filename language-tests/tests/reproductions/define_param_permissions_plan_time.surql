/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "DEFINE PARAM with PERMISSIONS FULL should be resolvable at plan time; PERMISSIONS NONE should still work at runtime for privileged users"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: items:1, x: 1 }]"

[[test.results]]
value = "[{ id: items:2, x: 2 }]"

[[test.results]]
value = "[{ id: items:1, x: 1 }, { id: items:2, x: 2 }]"

[[test.results]]
value = "[{ id: items:1, x: 1 }, { id: items:2, x: 2 }]"
*/

-- PERMISSIONS FULL: safe to resolve at plan time
DEFINE PARAM $full_table VALUE "items" PERMISSIONS FULL;
-- PERMISSIONS NONE: must not be resolved at plan time, but still
-- accessible at runtime for owner-level users
DEFINE PARAM $none_table VALUE "items" PERMISSIONS NONE;

DEFINE TABLE items SCHEMAFULL;
DEFINE FIELD x ON items TYPE int;
CREATE items:1 SET x = 1;
CREATE items:2 SET x = 2;

-- Both should return the same results for an owner-level user
SELECT * FROM type::table($full_table) ORDER BY x;
SELECT * FROM type::table($none_table) ORDER BY x;
