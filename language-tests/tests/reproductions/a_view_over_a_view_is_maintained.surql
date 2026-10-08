/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A second-order view — one whose source table is itself a `DEFINE TABLE … AS SELECT` — was maintained only by its own backfill. The ungrouped view arm wrote its row and returned without running the view table's lifecycle, so the write never reached the views defined over it and the outer view stayed at whatever the backfill left."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ field: 1, id: target:1 }]"

[[test.results]]
value = "[{ field: 2, id: target:2 }]"

[[test.results]]
value = "[{ field: 1, id: mirror:1 }, { field: 2, id: mirror:2 }]"

[[test.results]]
value = "[{ id: total:[], n: 2 }]"

*/

DEFINE TABLE target SCHEMALESS;
DEFINE TABLE mirror AS SELECT field FROM target;
DEFINE TABLE total AS SELECT count() AS n FROM mirror GROUP ALL;

CREATE target:1 SET field = 1;
CREATE target:2 SET field = 2;

SELECT * FROM mirror ORDER BY field;

-- Both rows reached `mirror`, so both reached the view defined over it.
SELECT * FROM total;
