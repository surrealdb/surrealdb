/**
[env]
imports = ["upgrade/define/3.00.00_index_unique_nullish_slot_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
# Releases 3.0.2 to 3.0.5 write no entry for a tuple holding NONE or NULL on a
# UNIQUE index, so no reader finds one through the index; from 3.1.0 such a
# tuple is a non-unique entry, the placement 3.00.02_index_unique_nullish_coexist.surql
# covers.
importing-version = "<3.0.2"
reason = """
A UNIQUE index over two columns written by 3.0.0 or 3.0.1 stores a tuple
holding NONE or NULL at the tuple's unique slot, as it stores a complete one.
An equality read on such a tuple, and a read pinning the leading value alone,
find it there, and a complete tuple the old writer stored still collides with
an equal one written now. Its plan, and a second nullish tuple written after
the upgrade, are asserted in the _current sibling."""

[[test.results]]
value = "[pair:1]"

[[test.results]]
value = "[pair:2]"

[[test.results]]
value = "[pair:3]"

[[test.results]]
value = "[pair:1, pair:2, pair:3]"

[[test.results]]
value = "[pair:5]"

[[test.results]]
value = "[pair:4, pair:5]"

[[test.results]]
error = "Database index `pair_ab` already contains ['k', 'v'], with record `pair:1`"

*/
-- Equality on a complete tuple, a NONE tuple and a NULL tuple.
SELECT VALUE id FROM pair WHERE a = 'k' AND b = 'v';
SELECT VALUE id FROM pair WHERE a = 'k' AND b = NONE;
SELECT VALUE id FROM pair WHERE a = 'k' AND b = NULL;
-- The leading value alone reads complete and nullish tuples together.
SELECT VALUE id FROM pair WHERE a = 'k' ORDER BY id;
SELECT VALUE id FROM pair WHERE a = 'j' AND b = NONE;
SELECT VALUE id FROM pair WHERE a = 'j' ORDER BY id;
-- A complete tuple still collides.
CREATE pair:6 SET a = 'k', b = 'v';
