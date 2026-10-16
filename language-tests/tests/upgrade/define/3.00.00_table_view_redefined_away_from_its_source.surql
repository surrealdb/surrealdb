/**
[env]
imports = ["upgrade/define/3.00.00_table_view_redefined_away_from_its_source_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
A store an older release wrote holds a registration under `src` for each view
that once read it, although neither reads it now: `v` was redefined over
`other` and `w` as an ordinary table. This checkout reads a registration only
when its view still reads the table, so a write to `src` changes neither,
neither refuses a migration of `src`, and once both are removed nothing refuses
removing it.

Released binaries maintain both views from `src` and refuse the removal, so the
body runs only where the upgrade target is this checkout: the seed axis, each
old release straight to HEAD, and the final adjacent hop. The import is plain
SurrealQL every release writes, so `introduced` stays 3.0.0."""

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 10, id: v:1 }]"

[[test.results]]
value = "[{ a: 1, id: w:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[6]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/
-- A write to `src` reaches neither former view.
UPDATE src:1 SET a = 5 RETURN NONE;
SELECT * FROM v ORDER BY id;
SELECT * FROM w ORDER BY id;
-- Neither former view refuses a migration of the table it used to read, or its
-- removal.
DEFINE FIELD a ON src TYPE int MIGRATE EXISTING ROWS AS a_plus USING $value + 1 FALLBACK 0;
SELECT VALUE a FROM src ORDER BY id;
REMOVE TABLE w;
REMOVE TABLE v;
REMOVE TABLE src;
