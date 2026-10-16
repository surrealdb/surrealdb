/**
[test]
reason = """
Setup for 3.00.00_table_view_redefined_away_from_its_source.surql: the old
binary defines two views over `src`, then redefines one as a view over `other`
and the other as an ordinary table. A released binary leaves each view's
registration under `src` when it does. Each view selects `id`, which 3.0
requires of a view."""
run = false
*/
DEFINE TABLE src SCHEMALESS;
DEFINE TABLE other SCHEMALESS;
CREATE src:1 SET a = 1;
CREATE other:1 SET a = 10;
DEFINE TABLE v AS SELECT id, a FROM src;
DEFINE TABLE OVERWRITE v AS SELECT id, a FROM other;
DEFINE TABLE w AS SELECT id, a FROM src;
DEFINE TABLE OVERWRITE w SCHEMALESS;
