/**
[env]
namespace = true
database = true
# surrealkv refuses versioned reads unless the datastore is opened versioned.
backend = ["surrealkv"]
versioned = true
timeout = 5000

[test]
reason = """
	A read at a VERSION stamp reads the keys the stamp holds, among them records written and
	deleted since, so a predicate on `id` is judged over the table as of the stamp and no id bound
	narrows the scan. The same predicate without a stamp reads the present rows.
"""

[[test.results]]
value = "'OK'"

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
value = "[t:2, t:3]"

[[test.results]]
value = "[t:2, t:3]"

[[test.results]]
value = "[t:3, t:2]"

[[test.results]]
value = "[t:3, t:4, t:5]"

[[test.results]]
value = "[t:3, t:4]"
*/

{
	CREATE t:1 SET n = 1;
	CREATE t:2 SET n = 2;
	CREATE t:3 SET n = 3;
	RETURN 'OK';
};
SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;
DELETE t:2 RETURN NONE;
CREATE t:4 SET n = 4 RETURN NONE;
CREATE t:5 SET n = 5 RETURN NONE;
SELECT VALUE id FROM t WHERE id > t:1 VERSION $t1;
SELECT VALUE id FROM t WHERE id > t:1 AND id < t:4 VERSION $t1;
SELECT VALUE id FROM t WHERE id >= t:2 ORDER BY id DESC VERSION $t1;
SELECT VALUE id FROM t WHERE id > t:1;
SELECT VALUE id FROM t WHERE id > t:1 AND id < t:5;
