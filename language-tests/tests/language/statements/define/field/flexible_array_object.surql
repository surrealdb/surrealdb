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
value = "[{ items: [{ num: 1 }], id: test:1 }]"

[[test.results]]
value = "[{ items: [{ num: 2, extra: 'allowed' }], id: test:2 }]"

*/

-- FLEXIBLE with array<object>
DEFINE TABLE test SCHEMAFULL;
DEFINE FIELD items ON test TYPE array<object> FLEXIBLE;
DEFINE FIELD items.*.num ON test TYPE number;

CREATE test:1 SET items = [{ num: 1 }];
CREATE test:2 SET items = [{ num: 2, extra: 'allowed' }]; -- extra is allowed

