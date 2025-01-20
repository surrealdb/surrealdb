/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ f: '1', id: t:1 }]"

[[test.results]]
value = "[{ f: '1', id: t:1 }]"

*/


DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD f ON t VALUE "1";

CREATE t:1 SET f = "2";
SELECT * FROM t;


