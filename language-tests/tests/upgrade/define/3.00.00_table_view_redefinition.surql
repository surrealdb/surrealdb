/**
[env]
imports = ["upgrade/define/3.00.00_table_view_redefinition_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
# Only a binary whose redefinition drops the registrations of the views reading
# the redefined one can write the orphaned chain: the current checkout refuses
# `REMOVE TABLE o1` while `o2` and `o3` read it. Every released reader drops
# those registrations itself and refuses to remove an orphaned view, so the hop
# is pinned from both ends, which also keeps the ordinary language-test runner
# away. The seed axis sends every released version straight to the current
# checkout.
importing-version = "<3.4.0-0"
version = ">=3.4.0-a"
reason = """
A view's registrations on the tables it reads, its rows and a grouped view's
counts are all written by the older binary, and redefining a view on the current
one reads each of them: the registrations of the views reading it are kept, and
each row it held is removed through the views over it before the new rows reach
them, so `v2` and `g` end holding what the new definition implies and keep
receiving writes.

The older binary also leaves views reading a table it removed. Neither a
removal nor a redefinition of such a view has a definition of that table to
refresh; each still succeeds, and the redefined view reads only what it names.
"""

[[test.results]]
value = "['g', 'v2']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['g', 'v2']"

[[test.results]]
value = "[{ a: 2, id: v2:2 }]"

[[test.results]]
value = "[{ c: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 2, id: v2:2 }, { a: 3, id: v2:3 }]"

[[test.results]]
value = "[{ c: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: o3:1 }]"

[[test.results]]
value = "['o3']"

[[test.results]]
value = "['g', 'o3', 'osrc', 'src', 'v1', 'v2']"

*/
-- The registrations the older binary wrote for the views reading v1.
object::keys((INFO FOR TABLE v1).tables);
-- Redefining v1 replaces the rows it holds and keeps its readers registered.
DEFINE TABLE OVERWRITE v1 AS SELECT * FROM src WHERE a > 1;
object::keys((INFO FOR TABLE v1).tables);
-- The rows the older binary wrote were removed through v2 and g.
SELECT * FROM v2 ORDER BY id;
SELECT c FROM g;
-- A write to the source still reaches the views over the redefined one.
CREATE src:3 SET a = 3 RETURN NONE;
SELECT * FROM v2 ORDER BY id;
SELECT c FROM g;

-- o2 and o3 read o1, which the older binary removed; each can be removed or redefined.
REMOVE TABLE o2;
DEFINE TABLE OVERWRITE o3 AS SELECT * FROM osrc;
SELECT * FROM o3 ORDER BY id;
object::keys((INFO FOR TABLE osrc).tables);
object::keys((INFO FOR DB).tables);
