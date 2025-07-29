/**
[test]

[[test.results]]
value = "[{ a: 3, id: test:a }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 2, id: test:a }]"

[[test.results]]
value = "[{ avg: 2 }]"

*/
CREATE test:a SET a = 3;
DEFINE TABLE foo AS SELECT math::mean(a) AS avg FROM test GROUP ALL;
UPDATE test:a SET a = 2;
SELECT avg FROM foo;
