/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
`DEFINE TABLE OVERWRITE` over a view removes the view's registration from every
table the view read, whatever the new definition is. A view moved to another
source is no longer written by its former one, a view turned back into an
ordinary table is no longer maintained at all, and `INFO FOR TABLE` on the
former source lists neither. Neither refuses a migration of the table it used to
read, and once both are removed, nothing refuses removing it.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 10, id: v:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 5, id: w:1 }]"

[[test.results]]
value = "{  }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[6, 8]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"
*/

DEFINE TABLE src SCHEMAFULL;
DEFINE FIELD a ON src TYPE int;
DEFINE TABLE other SCHEMAFULL;
DEFINE FIELD a ON other TYPE int;
CREATE src:1 SET a = 1 RETURN NONE;
CREATE other:1 SET a = 10 RETURN NONE;
-- A view moved from `src` to `other`.
DEFINE TABLE v AS SELECT a FROM src;
DEFINE TABLE OVERWRITE v AS SELECT a FROM other;
UPDATE src:1 SET a = 5 RETURN NONE;
SELECT * FROM v;
-- A view turned back into an ordinary table keeps the rows it held.
DEFINE TABLE w AS SELECT a FROM src;
DEFINE TABLE OVERWRITE w SCHEMALESS;
CREATE src:2 SET a = 7 RETURN NONE;
SELECT * FROM w ORDER BY id;
(INFO FOR TABLE src).tables;
-- Neither former view refuses a migration of `src`.
ALTER FIELD a ON src TYPE int MIGRATE EXISTING ROWS AS a_plus USING $value + 1 FALLBACK 0;
SELECT VALUE a FROM src ORDER BY id;
REMOVE TABLE w;
REMOVE TABLE v;
REMOVE TABLE src;
