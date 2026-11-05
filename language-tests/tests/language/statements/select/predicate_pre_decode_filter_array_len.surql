/**
[env]
namespace = true
database = true

[test]
reason = "`WHERE array::len(field) <op> N` reads just the array's rev-2 prologue varint (no element decode) and compares the count against the numeric literal. Covers equality, inequality, and ordering ops. EXPLAIN shows the predicate as a filter over the table scan."

[[test.results]]
value = "{ id: pflen:1, tags: [] }"

[[test.results]]
value = "{ id: pflen:2, tags: ['x'] }"

[[test.results]]
value = "{ id: pflen:3, tags: ['x', 'y', 'z'] }"

[[test.results]]
value = "{ id: pflen:4, tags: ['a', 'b', 'c', 'd', 'e'] }"

[[test.results]]
value = "[{ id: pflen:3 }]"

[[test.results]]
value = "[{ id: pflen:2 }, { id: pflen:3 }, { id: pflen:4 }]"

[[test.results]]
value = "[{ id: pflen:3 }, { id: pflen:4 }]"

[[test.results]]
value = "[{ id: pflen:1 }, { id: pflen:2 }]"

[[test.results]]
value = "[{ id: pflen:2 }, { id: pflen:3 }, { id: pflen:4 }]"

[[test.results]]
value = "[{ id: pflen:3 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    Filter [ctx: Db] [predicate: array::len(tags) > 0]
        TableScan [ctx: Db] [table: pflen, direction: Forward]
''''

*/

CREATE ONLY pflen:1 SET tags = [];
CREATE ONLY pflen:2 SET tags = ['x'];
CREATE ONLY pflen:3 SET tags = ['x', 'y', 'z'];
CREATE ONLY pflen:4 SET tags = ['a', 'b', 'c', 'd', 'e'];

SELECT id FROM pflen WHERE array::len(tags) = 3;
SELECT id FROM pflen WHERE array::len(tags) > 0;
SELECT id FROM pflen WHERE array::len(tags) >= 3;
SELECT id FROM pflen WHERE array::len(tags) <= 1;
SELECT id FROM pflen WHERE array::len(tags) != 0;
SELECT id FROM pflen WHERE 3 = array::len(tags);

EXPLAIN SELECT id FROM pflen WITH NOINDEX WHERE array::len(tags) > 0;
