/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Compound UNIQUE index: explicit != NONE / > NONE on trailing option column excludes NONE keys (same range mechanism as analyze_compound_conditions NotEqual→MoreThan; relates to #7280)"
issue = 7280

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 1, id: t7280_compound:one }]"

[[test.results]]
value = "[{ a: 1, b: 1, c: 'x', id: t7280_compound:two }]"

[[test.results]]
value = "[{ c: NONE, id: t7280_compound:one }, { c: 'x', id: t7280_compound:two }]"

[[test.results]]
value = "[{ c: 'x', id: t7280_compound:two }]"

[[test.results]]
value = "[{ c: 'x', id: t7280_compound:two }]"

[[test.results]]
value = "[{ c: 'x', id: t7280_compound:two }]"

[[test.results]]
value = "[{ c: 'x', id: t7280_compound:two }]"

[[test.results]]
value = "[{ c: 'x', id: t7280_compound:two }]"

[[test.results]]
value = "[{ c: 'x', id: t7280_compound:two }]"

[[test.results]]
value = "[{ c: NONE, id: t7280_compound:one }]"

*/

DEFINE TABLE OVERWRITE t7280_compound SCHEMAFULL;
DEFINE FIELD OVERWRITE a ON t7280_compound TYPE int;
DEFINE FIELD OVERWRITE b ON t7280_compound TYPE int;
DEFINE FIELD OVERWRITE c ON t7280_compound TYPE option<string>;
DEFINE INDEX OVERWRITE t7280_compound_abc ON t7280_compound FIELDS a, b, c UNIQUE;

-- Two keys under the same (a,b) prefix: trailing c is NONE vs a string
CREATE t7280_compound:one SET a = 1, b = 1;
CREATE t7280_compound:two SET a = 1, b = 1, c = 'x';

-- Prefix on (a,b) only: must see BOTH rows (including c = NONE)
SELECT id, c FROM t7280_compound WHERE a = 1 AND b = 1 ORDER BY id;

-- Trailing column strictly above NONE: scan excludes NONE-valued index entries
SELECT id, c FROM t7280_compound WHERE a = 1 AND b = 1 AND c > NONE ORDER BY id;

-- Same as above with WITH NOINDEX (sanity: table scan agrees with index)
SELECT id, c FROM t7280_compound WITH NOINDEX WHERE a = 1 AND b = 1 AND c > NONE ORDER BY id;

-- NotEqual to NONE is lowered to a strict range above NONE for compound indexes (see exec/index/analysis.rs)
SELECT id, c FROM t7280_compound WHERE a = 1 AND b = 1 AND c != NONE ORDER BY id;

SELECT id, c FROM t7280_compound WITH NOINDEX WHERE a = 1 AND b = 1 AND c != NONE ORDER BY id;

-- IS NOT NONE: same exclusion of NONE keys as != NONE / > NONE (see exec/index/analysis.rs)
SELECT id, c FROM t7280_compound WHERE a = 1 AND b = 1 AND c IS NOT NONE ORDER BY id;

SELECT id, c FROM t7280_compound WITH NOINDEX WHERE a = 1 AND b = 1 AND c IS NOT NONE ORDER BY id;

-- Exact match on NONE: must find the NONE row
SELECT id, c FROM t7280_compound WHERE a = 1 AND b = 1 AND c = NONE ORDER BY id;
