/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1000 }]"

*/
CREATE |test:1000| SET time = time::now() RETURN NONE;
SELECT count() FROM test GROUP ALL;
