/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "Pre-decode filter for CONTAINSANY, CONTAINSALL, CONTAINSNONE on arrays, including empty RHS lists. EXPLAIN asserts pre_decode_filter: yes."

[[test.results]]
value = "{ id: pcfa:1, tags: ['alpha', 'beta', 'gamma'] }"

[[test.results]]
value = "{ id: pcfa:2, tags: ['delta'] }"

[[test.results]]
value = "{ id: pcfa:3, tags: ['alpha'] }"

[[test.results]]
value = "[{ id: pcfa:1, tags: ['alpha', 'beta', 'gamma'] }]"

[[test.results]]
value = "[{ id: pcfa:1, tags: ['alpha', 'beta', 'gamma'] }]"

[[test.results]]
value = "[{ id: pcfa:2, tags: ['delta'] }, { id: pcfa:3, tags: ['alpha'] }]"

[[test.results]]
value = "[{ id: pcfa:1, tags: ['alpha', 'beta', 'gamma'] }, { id: pcfa:2, tags: ['delta'] }, { id: pcfa:3, tags: ['alpha'] }]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: *]\n    TableScan [ctx: Db] [table: pcfa, direction: Forward, predicate: tags CONTAINSANY ['beta'], pre_decode_filter: yes]\n"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: *]\n    TableScan [ctx: Db] [table: pcfa, direction: Forward, predicate: tags CONTAINSALL ['alpha', 'beta'], pre_decode_filter: yes]\n"'''
*/

CREATE ONLY pcfa:1 SET tags = ['alpha', 'beta', 'gamma'];
CREATE ONLY pcfa:2 SET tags = ['delta'];
CREATE ONLY pcfa:3 SET tags = ['alpha'];

SELECT * FROM pcfa WHERE tags CONTAINSANY ['beta', 'zeta'];
SELECT * FROM pcfa WHERE tags CONTAINSALL ['alpha', 'beta'];
SELECT * FROM pcfa WHERE tags CONTAINSNONE ['beta', 'gamma'];
SELECT * FROM pcfa WHERE tags CONTAINSALL [];
SELECT * FROM pcfa WHERE tags CONTAINSANY [];

EXPLAIN SELECT * FROM pcfa WITH NOINDEX WHERE tags CONTAINSANY ['beta'];
EXPLAIN SELECT * FROM pcfa WITH NOINDEX WHERE tags CONTAINSALL ['alpha', 'beta'];
