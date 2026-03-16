/**
[env]
new-planner-strategy = "compute-only"

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ email: 'email@domain.com', id: test:1 }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'test' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ email: 'email@domain.com', id: test:1 }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'test' }, operation: 'Iterate Table' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ email: 'email@domain.com', id: test:1 }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'test' }, operation: 'Iterate Table' }, { detail: { reason: 'WITH NOINDEX' }, operation: 'Fallback' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ email: 'email@domain.com', id: test:1 }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'test' }, operation: 'Iterate Table' },  { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/

DEFINE INDEX OVERWRITE email ON test COLUMNS email;
UPSERT test:1 SET email = 'email@domain.com';

SELECT * FROM test WHERE email CONTAINS 'domain.com' EXPLAIN;
SELECT * FROM test WHERE email CONTAINS 'domain.com';

SELECT * FROM test WHERE 'domain.com' INSIDE email  EXPLAIN;
SELECT * FROM test WHERE 'domain.com' INSIDE email;

SELECT * FROM test WITH NOINDEX WHERE email CONTAINS 'domain.com' EXPLAIN;
SELECT * FROM test WITH NOINDEX WHERE email CONTAINS 'domain.com';

DELETE FROM test WHERE email CONTAINS 'domain.com' EXPLAIN;
DELETE FROM test WHERE email CONTAINS 'domain.com';
SELECT * FROM test;