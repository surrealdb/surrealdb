/**
[test]

[[test.results]]
value = "[{ a: 1, b: '2', id: table:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: '2', id: table:1 }]"

[[test.results]]
value = "[{ a: '1', b: 2, id: table:2 }]"

[[test.results]]
value = "[{ a: 1, b: '2', id: table:1 }, { a: '1', b: 2, id: table:2 }]"

*/
CREATE table:1 SET a = 1, b = "2";

DEFINE TABLE OVERWRITE table SCHEMAFULL;
DEFINE FIELD a ON TABLE table TYPE string;
DEFINE FIELD b ON TABLE table TYPE number;

SELECT * FROM table;
CREATE table:2 SET a = "1", b = 2;
SELECT * FROM table;
