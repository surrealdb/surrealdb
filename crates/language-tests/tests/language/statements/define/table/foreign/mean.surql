/**
[test]

[[test.results]]
value = "[{ a: 3, id: test:a }]"

[[test.results]]
value = "NONE"


[[test.results]]
value = "[{ avg: 3 }]"

[[test.results]]
value = "[{ a: 2, id: test:a }]"

[[test.results]]
value = "[{ avg: 2 }]"

[[test.results]]
value = "[{ a: 5, id: test:b }]"

[[test.results]]
value = "[{ avg: 3.5 }]"

*/
CREATE test:a SET a = 3;
DEFINE TABLE foo AS SELECT math::mean(a) AS avg FROM test GROUP ALL;
SELECT avg FROM foo;
UPDATE test:a SET a = 2;
SELECT avg FROM foo;
CREATE test:b SET a = 5;
SELECT avg FROM foo;
