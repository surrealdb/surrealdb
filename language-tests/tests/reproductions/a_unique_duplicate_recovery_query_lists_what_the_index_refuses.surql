/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
The query `doc/UPGRADING.md` gives for finding the duplicates that make a
`REBUILD INDEX` of a `UNIQUE` index fail lists exactly the values such an index
refuses. The index keys an array once per distinct element, so two records
sharing an element collide, a record holding an element twice is keyed once, and
an array inside an array is one key. It keeps `NULL`, `NONE` and a top-level
empty array (keyed as `NONE`) as non-unique entries, so those are never listed.
The table `s` shows what the index refuses; the table `t` holds the same rows
without the index, as a table an old node wrote to would.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database index `uq` already contains d'2026-08-28T09:00:00Z', with record `s:1`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database index `uq` already contains [], with record `s:9`"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database index `uq` already contains [1, 2], with record `s:11`"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database index `uq` already contains d'2027-01-01T00:00:00Z', with record `s:13`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 2, u: d'2026-08-28T09:00:00Z' }, { n: 2, u: d'2027-01-01T00:00:00Z' }, { n: 2, u: [] }, { n: 2, u: [1, 2] }]"
*/

DEFINE INDEX uq ON s FIELDS u UNIQUE;
CREATE s:1 SET u = [d'2026-08-28T09:00:00Z', 1] RETURN NONE;
CREATE s:2 SET u = [d'2026-08-28T09:00:00.000Z', 2] RETURN NONE;
CREATE s:3 SET u = NULL RETURN NONE;
CREATE s:4 SET u = NULL RETURN NONE;
CREATE s:5 SET u = NONE RETURN NONE;
CREATE s:6 SET u = NONE RETURN NONE;
CREATE s:7 SET u = [] RETURN NONE;
CREATE s:8 SET u = [] RETURN NONE;
CREATE s:9 SET u = [[], 'p'] RETURN NONE;
CREATE s:10 SET u = [[], 'r'] RETURN NONE;
CREATE s:11 SET u = [[1, 2], 3] RETURN NONE;
CREATE s:12 SET u = [[1, 2], 4] RETURN NONE;
CREATE s:13 SET u = d'2027-01-01T00:00:00Z' RETURN NONE;
CREATE s:14 SET u = d'2027-01-01T00:00:00Z' RETURN NONE;
CREATE s:15 SET u = 'q' RETURN NONE;
CREATE s:16 SET u = [d'2028-01-01T00:00:00Z', d'2028-01-01T00:00:00Z'] RETURN NONE;
CREATE t:1 SET u = [d'2026-08-28T09:00:00Z', 1] RETURN NONE;
CREATE t:2 SET u = [d'2026-08-28T09:00:00.000Z', 2] RETURN NONE;
CREATE t:3 SET u = NULL RETURN NONE;
CREATE t:4 SET u = NULL RETURN NONE;
CREATE t:5 SET u = NONE RETURN NONE;
CREATE t:6 SET u = NONE RETURN NONE;
CREATE t:7 SET u = [] RETURN NONE;
CREATE t:8 SET u = [] RETURN NONE;
CREATE t:9 SET u = [[], 'p'] RETURN NONE;
CREATE t:10 SET u = [[], 'r'] RETURN NONE;
CREATE t:11 SET u = [[1, 2], 3] RETURN NONE;
CREATE t:12 SET u = [[1, 2], 4] RETURN NONE;
CREATE t:13 SET u = d'2027-01-01T00:00:00Z' RETURN NONE;
CREATE t:14 SET u = d'2027-01-01T00:00:00Z' RETURN NONE;
CREATE t:15 SET u = 'q' RETURN NONE;
CREATE t:16 SET u = [d'2028-01-01T00:00:00Z', d'2028-01-01T00:00:00Z'] RETURN NONE;
SELECT * FROM (
    SELECT u, count() AS n FROM (
        SELECT id, u FROM (
            SELECT id, u FROM t WITH NOINDEX
            WHERE !(type::is_array(u) AND array::len(u) = 0)
            SPLIT u
        ) GROUP BY id, u
    ) GROUP BY u
) WHERE n > 1 AND u != NONE AND u != NULL;
