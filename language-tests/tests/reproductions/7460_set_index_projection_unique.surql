/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Unique wildcard set indexes must reject overlapping elements and release element keys after updates and deletes."
issue = 7460

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
match = "$error = /Database index `unique_vals` already contains/"
error = true

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: unique_sets:c }]"

[[test.results]]
match = "$error = /Database index `unique_vals` already contains/"
error = true
*/

DEFINE FIELD vals ON unique_sets TYPE set<int>;
DEFINE INDEX unique_vals ON unique_sets FIELDS vals.* UNIQUE;

CREATE unique_sets:a SET vals = <set>[1, 2] RETURN NONE;
CREATE unique_sets:b SET vals = <set>[2, 3] RETURN NONE;

-- Updating releases 2, while retaining the entry for 1 and adding 4.
UPDATE unique_sets:a SET vals = <set>[1, 4] RETURN NONE;
CREATE unique_sets:b SET vals = <set>[2, 3] RETURN NONE;

-- Deleting releases 2 again so another record can reuse it.
DELETE unique_sets:b RETURN NONE;
CREATE unique_sets:c SET vals = <set>[2, 5] RETURN NONE;
SELECT id FROM unique_sets WHERE vals CONTAINS 2;
CREATE unique_sets:d SET vals = <set>[4, 6] RETURN NONE;
