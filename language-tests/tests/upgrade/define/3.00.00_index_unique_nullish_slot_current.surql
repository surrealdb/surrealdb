/**
[env]
imports = ["upgrade/define/3.00.00_index_unique_nullish_slot_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
importing-version = "<3.0.2"
reason = """
The plans the reads in 3.00.00_index_unique_nullish_slot.surql are answered
by, and the write that distinguishes a nullish tuple from a complete one: a
second tuple equal to a nullish tuple the old binary stored at its unique slot
does not collide with it, and an equality read then returns both, one from the
slot and one from the non-unique entry written now.

The write is asserted against the current checkout only because releases
differ on whether such a tuple collides. `EXPLAIN` output is not stable across
the 3.x line either; the seed axis sends every released version straight to
the current checkout."""

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: pair, index: pair_ab, access: ['k', 'v'], direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: pair, index: pair_ab, access: ['k', NONE], direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: pair, index: pair_ab, access: ['k', NULL], direction: Forward]
"'''

[[test.results]]
value = '''
"ProjectValue [ctx: Db] [expr: id]
    IndexScan [ctx: Db] [table: pair, index: pair_ab, access: ['k'], direction: Forward]
"'''

[[test.results]]
value = "[{ a: 'k', id: pair:7 }]"

[[test.results]]
value = "[{ a: 'k', b: NULL, id: pair:8 }]"

[[test.results]]
value = "[pair:2, pair:7]"

[[test.results]]
value = "[pair:3, pair:8]"

[[test.results]]
value = "[pair:1, pair:2, pair:3, pair:7, pair:8]"

*/
EXPLAIN SELECT VALUE id FROM pair WHERE a = 'k' AND b = 'v';
EXPLAIN SELECT VALUE id FROM pair WHERE a = 'k' AND b = NONE;
EXPLAIN SELECT VALUE id FROM pair WHERE a = 'k' AND b = NULL;
EXPLAIN SELECT VALUE id FROM pair WHERE a = 'k';
-- A nullish tuple equals no other for uniqueness.
CREATE pair:7 SET a = 'k';
CREATE pair:8 SET a = 'k', b = NULL;
-- Both writers' entries are read together.
SELECT VALUE id FROM pair WHERE a = 'k' AND b = NONE ORDER BY id;
SELECT VALUE id FROM pair WHERE a = 'k' AND b = NULL ORDER BY id;
SELECT VALUE id FROM pair WHERE a = 'k' ORDER BY id;
