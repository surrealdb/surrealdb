/**
[test]

[[test.results]]
value = "{ f: true, id: t:1 }"

[[test.results]]
value = "{ f: true, id: t:2 }"

[[test.results]]
value = "{ f: false, id: t:3 }"

[[test.results]]
value = "{ f: false, id: t:4 }"

[[test.results]]
value = "[{ id: t:3 }, { id: t:4 }]"

[[test.results]]
value = "[{ id: t:4 }]"

[[test.results]]
value = "[]"

*/

CREATE ONLY t:1 SET f=true;
CREATE ONLY t:2 SET f=true;
CREATE ONLY t:3 SET f=false;
CREATE ONLY t:4 SET f=false;

SELECT id FROM t WHERE !f;
SELECT id FROM t WHERE !f START 1;
SELECT id FROM t WHERE !f START 2;