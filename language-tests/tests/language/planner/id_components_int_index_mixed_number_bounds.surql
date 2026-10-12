/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	An index over an int component of the record id is read with bounds of
	any number kind. A write stores the component as an int, and the index
	keys a number by its value, so `n = 2.0`, `n = 2dec`, an inclusive bound
	written as a float or a decimal and an IN list of mixed kinds find the
	int rows, and answer as the same queries WITH NOINDEX do. The component
	is not leading, so no key range serves these and the index is read.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ feed: 'a', id: t:['a', 2], n: 2 }]"

[[test.results]]
value = "[{ feed: 'b', id: t:['b', 3], n: 3 }]"

[[test.results]]
value = "[{ feed: 'c', id: t:['c', 2], n: 2 }]"

[[test.results]]
value = "[t:['a', 2], t:['c', 2]]"

[[test.results]]
value = "[t:['a', 2], t:['c', 2]]"

[[test.results]]
value = "[t:['a', 2], t:['c', 2]]"

[[test.results]]
value = "[t:['a', 2], t:['c', 2]]"

[[test.results]]
value = "[t:['b', 3]]"

[[test.results]]
value = "[t:['a', 2], t:['b', 3], t:['c', 2]]"

[[test.results]]
value = "[t:['a', 2], t:['b', 3], t:['c', 2]]"

[[test.results]]
value = '''
'ProjectValue [ctx: Db] [expr: id]
    Filter [ctx: Db] [predicate: id[1] = 2f]
        IndexScan [ctx: Db] [table: t, index: by_n, access: = 2f, direction: Forward]
''''

*/

DEFINE FIELD id ON t TYPE [feed: string, n: int];
DEFINE INDEX by_n ON t FIELDS n;
CREATE t SET feed = 'a', n = 2;
CREATE t SET feed = 'b', n = 3;
CREATE t SET feed = 'c', n = 2.0;
SELECT VALUE id FROM t WHERE n = 2.0 ORDER BY id;
SELECT VALUE id FROM t WHERE n = 2dec ORDER BY id;
SELECT VALUE id FROM t WHERE n >= 2.0 AND n < 3dec ORDER BY id;
SELECT VALUE id FROM t WHERE n <= 2.0 ORDER BY id;
SELECT VALUE id FROM t WHERE n > 2dec ORDER BY id;
SELECT VALUE id FROM t WHERE n IN [2.0, 3dec] ORDER BY id;
SELECT VALUE id FROM t WITH NOINDEX WHERE n IN [2.0, 3dec] ORDER BY id;
EXPLAIN SELECT VALUE id FROM t WHERE n = 2.0;
