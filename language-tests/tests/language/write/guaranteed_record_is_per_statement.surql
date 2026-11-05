/**
[env]
imports = ["language/write/guaranteed_record_is_per_statement_import.surql"]

[test]
reason = "An `UPSERT <table> ... WHERE` that matches nothing creates exactly one record, once for the whole statement — not once per scanned record and not once per batch. The first case scans more rows than one batch holds and still creates one; the empty table creates one; and any statement that did match records creates none, however many it matched. The `RETURN tag` on each statement reports what the fallback produced; the fallback itself is driven by what the statement matched rather than by what it returned, which `reproductions/upsert_where_return_none_phantom.surql` holds across the RETURN modes."

[[test.results]]
value = "[{ tag: 'a' }]"

[[test.results]]
value = "[{ count: 601 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ tag: 'b' }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ tag: 'c' }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ tag: 'd' }, { tag: 'd' }, { tag: 'd' }]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 3 }]"

*/

-- 600 rows scanned, none matched, one record created.
UPSERT many SET tag = 'a' WHERE n = 99 RETURN tag;
SELECT count() FROM many GROUP ALL;
SELECT count() FROM many WHERE tag = 'a' GROUP ALL;

-- Nothing scanned at all, still exactly one.
UPSERT empty SET tag = 'b' WHERE n = 99 RETURN tag;
SELECT count() FROM empty GROUP ALL;

-- One match creates nothing.
UPSERT few SET tag = 'c' WHERE n = 2 RETURN tag;
SELECT count() FROM few GROUP ALL;
SELECT count() FROM few WHERE tag = 'c' GROUP ALL;

-- Several matches create nothing either.
UPSERT few SET tag = 'd' WHERE n > 0 RETURN tag;
SELECT count() FROM few GROUP ALL;
SELECT count() FROM few WHERE tag = 'd' GROUP ALL;
