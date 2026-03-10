/**
[test]
reason = "Test FLEXIBLE with array<object>"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ items: [{ id: 1 }], id: test:1 }]"

[[test.results]]
value = "[{ items: [{ id: 2, extra: 'allowed' }], id: test:2 }]"

*/

-- FLEXIBLE with array<object>
DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD items ON test TYPE array<object> FLEXIBLE;
DEFINE FIELD items[*].id ON test TYPE number;

CREATE test:1 SET items = [{ id: 1 }];
CREATE test:2 SET items = [{ id: 2, extra: 'allowed' }]; -- extra is allowed

