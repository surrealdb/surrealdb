/**
[test]
reason = """
Setup for 3.00.00_table_view_redefinition.surql. The older binary writes two
chains of views.

In the first, `v2` and the grouped `g` read the view `v1`, which reads `src`.
Each view is defined over rows that already exist, so each is filled when it is
defined, and every row and count is what the definitions imply: no released
binary carries a write to `src` on through `v1` to the views that read it.

In the second, `o2` and `o3` read the view `o1`. Redefining `o1` drops the
registrations they hold on it, which is what lets the following `REMOVE TABLE
o1` succeed and leaves both reading a table that no longer exists. `o1` is
redefined while it holds no row, which every released binary accepts.
"""
run = false
*/
DEFINE TABLE src SCHEMALESS;
CREATE src:1 SET a = 1 RETURN NONE;
CREATE src:2 SET a = 2 RETURN NONE;
DEFINE TABLE v1 AS SELECT * FROM src;
DEFINE TABLE v2 AS SELECT * FROM v1;
DEFINE TABLE g AS SELECT count() AS c FROM v1 GROUP ALL;

DEFINE TABLE osrc SCHEMALESS;
DEFINE TABLE o1 AS SELECT * FROM osrc;
DEFINE TABLE o2 AS SELECT * FROM o1;
DEFINE TABLE o3 AS SELECT * FROM o1;
DEFINE TABLE OVERWRITE o1 AS SELECT * FROM osrc;
REMOVE TABLE o1;
CREATE osrc:1 SET a = 1 RETURN NONE;
