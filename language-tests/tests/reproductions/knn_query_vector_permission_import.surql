/**
[test]
reason = """
Setup for knn_query_vector_permission: a record access, vector tables a record
user can read, protected tables whose `PERMISSIONS FOR select` predicate binds
or reads `$parent` in different ways, and rows of a table whose `x` differs
from the protected rows'.
"""

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:alice }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD DURATION FOR SESSION 12h;
DEFINE TABLE pts PERMISSIONS FULL;
DEFINE TABLE bpts PERMISSIONS FULL;
DEFINE TABLE person PERMISSIONS FULL;
DEFINE TABLE thing PERMISSIONS FULL;
DEFINE TABLE likes TYPE RELATION PERMISSIONS FULL;
DEFINE TABLE t PERMISSIONS FULL;
DEFINE INDEX hn ON pts FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32;
FOR $i IN 0..6 { CREATE type::record('pts', $i) SET point = [<float> $i, 0f], x = $i; CREATE type::record('bpts', $i) SET point = [<float> $i, 0f], x = $i; };
CREATE user:alice;
CREATE person:a, thing:x RETURN NONE;
RELATE person:a->likes->thing:x SET w = 4 RETURN NONE;
DEFINE TABLE filtered PERMISSIONS FOR select WHERE array::len(acl[WHERE $parent.enabled]) > 0;
DEFINE TABLE looked_up PERMISSIONS FOR select WHERE array::len(person:a->likes[WHERE w = $parent.w]) > 0;
DEFINE TABLE own_row PERMISSIONS FOR select WHERE $this.x = 1;
DEFINE TABLE plain PERMISSIONS FOR select WHERE enabled = true;
DEFINE TABLE enclosing PERMISSIONS FOR select WHERE $parent.x = 1;
CREATE filtered:one, looked_up:one, own_row:one, plain:one, enclosing:one SET enabled = true, acl = [1], x = 1, w = 4, v = [5f, 0f] RETURN NONE;
CREATE t:1, t:2 RETURN NONE;
UPDATE t:1 SET x = 1 RETURN NONE;
UPDATE t:2 SET x = 2 RETURN NONE;
