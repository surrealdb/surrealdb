/**
[env]
namespace = true
database = true
timeout = 30000

[test]
reason = "A predicate that steps through a record link above a top-k scan of a SCHEMAFULL table fetches the linked record of every row it judges, so it raises the error of a linked record it cannot compute, and a predicate that never reaches that record answers as it would without one."

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
error = "Could not cast into `int` using input `'boom'`"

[[test.results]]
value = "[{ id: p:1099 }, { id: p:1098 }, { id: p:1097 }, { id: p:1096 }, { id: p:1095 }]"

[[test.results]]
value = "[{ a: 0, id: p:0 }, { a: 1, id: p:1 }, { a: 2, id: p:2 }]"

*/

DEFINE TABLE o SCHEMAFULL;
DEFINE FIELD s ON o TYPE string;
DEFINE FIELD x ON o COMPUTED type::int(s);
CREATE o:1 SET s = '1' RETURN NONE;
CREATE o:5 SET s = 'boom' RETURN NONE;
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE int;
DEFINE FIELD link ON p TYPE record<o>;
FOR $n IN 0..1200 { CREATE type::record('p', $n) CONTENT { a: $n, link: o:1 } RETURN NONE; };
UPDATE p:1100 SET link = o:5 RETURN NONE;
SELECT id FROM p WHERE link.x = 1 ORDER BY a LIMIT 5;
SELECT id FROM p WHERE a < 1100 AND link.x = 1 ORDER BY a DESC LIMIT 5;
SELECT id, a FROM p WHERE a < 1100 ORDER BY a LIMIT 3;
