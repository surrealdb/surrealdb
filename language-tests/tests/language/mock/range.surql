/**
[env]
# Creating 1000 mock records can take a few seconds
timeout = 10000
context-timeout = 10000

[test]

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1000 }]"

*/
CREATE |test:100..1100| SET time = time::now() RETURN NONE;
SELECT count() FROM test GROUP ALL;
