/**
[env]
imports = ["upgrade/value/3.00.00_record_format_mixed_import.surql"]

[test]
upgrade = true
reason = "Records a released binary wrote in the `revision` format stay readable beside records the current build writes in the stored-value format, in one table, through scans, filters, projections, nested paths, point reads and an index the old binary populated. The current build raises the store it opens to the stored format, so every record it rewrites or creates is in that format while the untouched ones are not."
# The stored-value format is the current checkout's; older targets write and read only `revision`.
version = ">=3.4.0-a"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ d: 2.5dec, dt: d'2012-01-01T08:00:00Z', f: 1.5f, id: doc:1, link: person:a, n: 1, nothing: NULL, obj: { x: 1, y: { z: 'deep' } }, s: 'one', tags: ['a', 'b'], u: u'00000000-0000-0000-0000-000000000001' }, { id: doc:2, n: 20, s: 'two' }, { id: doc:3, n: 3, obj: { x: 3 }, s: 'three' }, { id: doc:4, n: 4, obj: { x: 4, y: { z: 'new' } }, s: 'four' }]"

[[test.results]]
value = "[{ id: typed:1, n: 1, s: 'a string longer than twelve bytes', tags: ['x'] }, { id: typed:2, n: 22, opt: 'present', s: 'b', tags: [] }, { id: typed:3, n: 3, s: 'c', tags: ['y', 'z'] }]"

[[test.results]]
value = "[{ id: doc:2, n: 20 }, { id: doc:3, n: 3 }, { id: doc:4, n: 4 }]"

[[test.results]]
value = "[{ n: 22, s: 'b' }, { n: 3, s: 'c' }]"

[[test.results]]
value = "['deep', 'new']"

[[test.results]]
value = "[1, 'one']"

[[test.results]]
value = "[22, 'present']"

[[test.results]]
value = "[{ c: 3, total: 26 }]"

[[test.results]]
value = "[typed:1]"

[[test.results]]
value = "[typed:1]"

[[test.results]]
value = "[typed:2]"

[[test.results]]
value = "[typed:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

*/
-- Rewrite one old record of each table, and add a new one: each table now mixes formats.
UPDATE doc:2 SET n = 20 RETURN NONE;
CREATE doc:4 SET n = 4, s = 'four', obj = { x: 4, y: { z: 'new' } } RETURN NONE;
UPDATE typed:2 SET n = 22 RETURN NONE;
CREATE typed:3 SET n = 3, s = 'c', tags = ['y', 'z'] RETURN NONE;
-- Whole records of both formats.
SELECT * FROM doc ORDER BY id;
SELECT * FROM typed ORDER BY id;
-- A filter and a projection across both formats.
SELECT id, n FROM doc WHERE n > 2 ORDER BY id;
SELECT s, n FROM typed WHERE n >= 2 ORDER BY s;
-- A nested path, read from an old and a new record.
SELECT VALUE obj.y.z FROM doc WHERE obj.y.z != NONE ORDER BY id;
-- Point reads of an old and a rewritten record.
SELECT VALUE [n, s] FROM ONLY doc:1;
SELECT VALUE [n, opt] FROM ONLY typed:2;
-- Aggregates over the mixed table.
SELECT count() AS c, math::sum(n) AS total FROM typed GROUP ALL;
-- The index the old binary populated finds an untouched record and a rewritten one, and no longer finds the value the rewrite replaced; each read answers what its table-scan twin answers.
SELECT VALUE id FROM typed WHERE n = 1;
SELECT VALUE id FROM typed WITH NOINDEX WHERE n = 1;
SELECT VALUE id FROM typed WHERE n = 22;
SELECT VALUE id FROM typed WITH NOINDEX WHERE n = 22;
SELECT VALUE id FROM typed WHERE n = 2;
SELECT VALUE id FROM typed WITH NOINDEX WHERE n = 2;
