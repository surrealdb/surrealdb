/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 7460
reason = """
A B-tree column that flattens a field declared as a set (`s.*` over
`TYPE set<int>`) stores one entry per set, keyed by the whole set, so it
cannot answer an element lookup. Every containment form over such a field
(`CONTAINS`, `CONTAINSANY`, `CONTAINSALL`, `IN`, and the `s.*` spellings)
reads the documents instead and returns every matching record, on every
engine. A UNIQUE index over such a column keeps constraining the whole set.
`.*` over a set yields the set's elements, each record link fetched.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, s: {1, 2, 3} }]"

[[test.results]]
value = "[{ id: t:2, s: {3, 4} }]"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t:2]"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{1, 2, 3}]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: o:1, s: {1, 2} }]"

[[test.results]]
value = "[{ id: o:2 }]"

[[test.results]]
value = "[o:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: u:1, s: {'a', 'b'} }]"

[[test.results]]
value = "[{ id: u:2, s: {'b', 'c'} }]"

[[test.results]]
error = "Database index `ux` already contains {'a', 'b'}, with record `u:1`"

[[test.results]]
value = "[u:1, u:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{{ id: t:1, s: {1, 2, 3} }, { id: t:2, s: {3, 4} }}]"
[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "{1,}"
*/
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD s ON t TYPE set<int>;
DEFINE INDEX ix ON t FIELDS s.*;
CREATE t:1 SET s = <set>[1, 2, 3];
CREATE t:2 SET s = <set>[3, 4];
SELECT VALUE id FROM t WHERE s CONTAINS 3 ORDER BY id;
SELECT VALUE id FROM t WHERE s CONTAINSANY [1, 4] ORDER BY id;
SELECT VALUE id FROM t WHERE s CONTAINSALL [1, 2] ORDER BY id;
SELECT VALUE id FROM t WHERE 4 IN s ORDER BY id;
SELECT VALUE id FROM t WHERE s.* CONTAINS 3 ORDER BY id;
SELECT VALUE id FROM t WHERE 1 INSIDE s.* ORDER BY id;
SELECT count() FROM t WHERE s CONTAINS 3 GROUP ALL;
SELECT VALUE s.* FROM t:1;

-- A field that may hold a set is treated the same way.
DEFINE TABLE o SCHEMALESS;
DEFINE FIELD s ON o TYPE option<set<int>>;
DEFINE INDEX ix ON o FIELDS s.*;
CREATE o:1 SET s = <set>[1, 2];
CREATE o:2;
SELECT VALUE id FROM o WHERE s CONTAINS 2 ORDER BY id;

-- A UNIQUE index keeps constraining the whole set: sets sharing an element
-- may coexist, an equal set may not.
DEFINE TABLE u SCHEMALESS;
DEFINE FIELD s ON u TYPE set<string>;
DEFINE INDEX ux ON u FIELDS s.* UNIQUE;
CREATE u:1 SET s = <set>['a', 'b'];
CREATE u:2 SET s = <set>['b', 'c'];
CREATE u:3 SET s = <set>['a', 'b'];
SELECT VALUE id FROM u WHERE s CONTAINS 'b' ORDER BY id;

-- `.*` over a set of record links fetches each linked record.
CREATE r:1 SET links = <set>[t:1, t:2] RETURN NONE;
SELECT VALUE links.* FROM r:1;

-- A containment no stored set satisfies returns no rows, through the scan and without the index alike.
SELECT VALUE id FROM t WHERE s CONTAINS 999;
SELECT VALUE id FROM t WITH NOINDEX WHERE s CONTAINS 999;
SELECT VALUE id FROM t WHERE s.* CONTAINS 999;
SELECT VALUE id FROM t WHERE s CONTAINSANY [998, 999];
SELECT VALUE id FROM t WITH NOINDEX WHERE s CONTAINSANY [998, 999];

-- A field read over a set collects equal values into one member.
RETURN (<set>[{ x: 1, y: 'a' }, { x: 1, y: 'b' }]).x;
