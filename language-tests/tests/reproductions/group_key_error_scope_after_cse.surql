/**
[env]
namespace = true
database = true

[test]
reason = "GROUP BY on an index or computed idiom (tags[0], a.b, string::lowercase(x), <int>x) with LIMIT 0 must return the same result as with LIMIT 1 (an empty or single group), not an internal IR verification error. Aggregates in the projection are optional. A repeated idiom in the projection and in GROUP BY is one value, and the CSE pass must not merge an op bound in an error scope into an op outside it."

[[test.results]]
value = "NONE"

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
value = "[{ k: 'a' }]"

[[test.results]]
value = "[{ c: 1, k: 'a' }]"

[[test.results]]
value = "[{ k: 'a', s: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ k: 1 }]"

[[test.results]]
value = "[{ c: 1, k: 1 }]"

[[test.results]]
value = "[{ k: 1, s: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ k: '3' }]"

[[test.results]]
value = "[{ c: 1, k: '3' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ k: 3 }]"

[[test.results]]
value = "[{ c: 1, k: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ k: 'a', m: 1f }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ k: 1, m: 1f }]"
*/

-- Group keys that are index or computed idioms, with and without aggregates, under LIMIT 0 and
-- LIMIT 1. A repeated idiom in the projection and in GROUP BY shares one value.
DEFINE TABLE f2;
CREATE f2:1 SET tags = ['a'], a = {b: 1}, x = '7' RETURN NONE;
CREATE f2:2 SET tags = ['b'], a = {b: 2}, x = '3' RETURN NONE;

SELECT tags[0] AS k FROM f2 GROUP BY tags[0] LIMIT 0;
SELECT tags[0] AS k, count() AS c FROM f2 GROUP BY tags[0] LIMIT 0;
SELECT tags[0] AS k, math::sum(1) AS s FROM f2 GROUP BY tags[0] LIMIT 0;
SELECT tags[0] AS k FROM f2 GROUP BY tags[0] LIMIT 1;
SELECT tags[0] AS k, count() AS c FROM f2 GROUP BY tags[0] LIMIT 1;
SELECT tags[0] AS k, math::sum(1) AS s FROM f2 GROUP BY tags[0] LIMIT 1;
SELECT a.b AS k FROM f2 GROUP BY a.b LIMIT 0;
SELECT a.b AS k, count() AS c FROM f2 GROUP BY a.b LIMIT 0;
SELECT a.b AS k, math::sum(1) AS s FROM f2 GROUP BY a.b LIMIT 0;
SELECT a.b AS k FROM f2 GROUP BY a.b LIMIT 1;
SELECT a.b AS k, count() AS c FROM f2 GROUP BY a.b LIMIT 1;
SELECT a.b AS k, math::sum(1) AS s FROM f2 GROUP BY a.b LIMIT 1;
SELECT string::lowercase(x) AS k FROM f2 GROUP BY k LIMIT 0;
SELECT string::lowercase(x) AS k, count() AS c FROM f2 GROUP BY k LIMIT 0;
SELECT string::lowercase(x) AS k FROM f2 GROUP BY k LIMIT 1;
SELECT string::lowercase(x) AS k, count() AS c FROM f2 GROUP BY k LIMIT 1;
SELECT <int>x AS k FROM f2 GROUP BY k LIMIT 0;
SELECT <int>x AS k, count() AS c FROM f2 GROUP BY k LIMIT 0;
SELECT <int>x AS k FROM f2 GROUP BY k LIMIT 1;
SELECT <int>x AS k, count() AS c FROM f2 GROUP BY k LIMIT 1;
SELECT tags[0] AS k, math::mean(1) AS m FROM f2 GROUP BY tags[0] LIMIT 0;
SELECT tags[0] AS k, math::mean(1) AS m FROM f2 GROUP BY tags[0] LIMIT 1;
SELECT a.b AS k, math::mean(a.b) AS m FROM f2 GROUP BY a.b LIMIT 0;
SELECT a.b AS k, math::mean(a.b) AS m FROM f2 GROUP BY a.b LIMIT 1;
