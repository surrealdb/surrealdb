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
	A subquery read at a VERSION is read at that stamp whether or not only its truthiness is read,
	so a stamp before a row was written finds no row where the present table has one.
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
value = "[outer:1]"

[[test.results]]
value = "[outer:2]"

[[test.results]]
value = "[outer:1, outer:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[outer:1]"
*/

{
	CREATE outer:1 SET x = 1;
	CREATE outer:2 SET x = 2;
	CREATE inner:1 SET x = 1;
	RETURN 'OK';
};
SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;
CREATE inner:2 SET x = 2 RETURN NONE;
DELETE inner:1 RETURN NONE;
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WHERE x = $parent.x VERSION $t1);
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WHERE x = $parent.x);
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner VERSION $t1);
SELECT VALUE id FROM outer WHERE !(SELECT id FROM inner VERSION $t1);
SELECT VALUE id FROM outer WHERE (SELECT id FROM inner WHERE x = $parent.x) VERSION $t1;
