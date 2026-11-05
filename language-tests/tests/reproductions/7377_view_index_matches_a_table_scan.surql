/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Indexes on a plain, an aggregate and a filtered view, defined before the source table is inserted into, updated and deleted from, answer ORDER BY, LIMIT and WHERE as a scan of the view does. Each oracle check also requires its plan to read the view's index, and the oracle statement lists the checks that do not hold."
issue = 7377

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
value = "NONE"

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
value = "[10, 5, 4, 1, 0]"

[[test.results]]
value = "[{ g: 1, mx: 10, n: 3 }, { g: 0, mx: 4, n: 2 }]"

[[test.results]]
value = "[10, 5, 4]"

[[test.results]]
value = "[[], [], []]"

[[test.results]]
value = "[]"

*/

FOR $i IN 1..=4 { CREATE type::record('parent', $i) SET t = $i, g = $i % 2 RETURN NONE; };
DEFINE TABLE child TYPE NORMAL SCHEMALESS AS SELECT * FROM parent PERMISSIONS NONE;
DEFINE INDEX child_t ON child FIELDS t;
DEFINE TABLE agg AS SELECT count() AS n, math::max(t) AS mx, g FROM parent GROUP BY g;
DEFINE INDEX agg_mx ON agg FIELDS mx;
DEFINE TABLE filt AS SELECT t FROM parent WHERE t > 2;
DEFINE INDEX filt_t ON filt FIELDS t UNIQUE;
CREATE parent:5 SET t = 5, g = 1 RETURN NONE;
UPDATE parent:1 SET t = 10 RETURN NONE;
DELETE parent:2;
UPDATE parent:3 SET t = 1 RETURN NONE;
CREATE parent:6 SET t = 0, g = 0 RETURN NONE;

-- What each view holds after those writes.
SELECT VALUE t FROM child ORDER BY t DESC;
SELECT g, mx, n FROM agg ORDER BY mx DESC;
SELECT VALUE t FROM filt ORDER BY t DESC;

-- The keys the writes replaced or removed are gone from each view's index:
-- child t = 3 and t = 2, agg mx = 3, filt t = 3.
RETURN [(SELECT VALUE id FROM child WHERE t IN [2, 3]), (SELECT VALUE id FROM agg WHERE mx = 3), (SELECT VALUE id FROM filt WHERE t = 3)];
RETURN [
	{ q: 'child desc', ok: (SELECT VALUE id FROM child ORDER BY t DESC) == (SELECT VALUE id FROM child WITH NOINDEX ORDER BY t DESC) },
	{ q: 'child desc (plan)', ok: (EXPLAIN SELECT VALUE id FROM child ORDER BY t DESC).contains('Index') },
	{ q: 'child limit', ok: (SELECT VALUE id FROM child ORDER BY t LIMIT 2) == (SELECT VALUE id FROM child WITH NOINDEX ORDER BY t LIMIT 2) },
	{ q: 'child limit (plan)', ok: (EXPLAIN SELECT VALUE id FROM child ORDER BY t LIMIT 2).contains('Index') },
	{ q: 'child range', ok: (SELECT VALUE id FROM child WHERE t > 3 ORDER BY t) == (SELECT VALUE id FROM child WITH NOINDEX WHERE t > 3 ORDER BY t) },
	{ q: 'child range (plan)', ok: (EXPLAIN SELECT VALUE id FROM child WHERE t > 3 ORDER BY t).contains('Index') },
	{ q: 'child updated', ok: (SELECT VALUE id FROM child WHERE t = 10) == (SELECT VALUE id FROM child WITH NOINDEX WHERE t = 10) },
	{ q: 'child updated (plan)', ok: (EXPLAIN SELECT VALUE id FROM child WHERE t = 10).contains('Index') },
	{ q: 'child stale after update', ok: (SELECT VALUE id FROM child WHERE t = 3) == (SELECT VALUE id FROM child WITH NOINDEX WHERE t = 3) },
	{ q: 'child stale after update (plan)', ok: (EXPLAIN SELECT VALUE id FROM child WHERE t = 3).contains('Index') },
	{ q: 'child stale after delete', ok: (SELECT VALUE id FROM child WHERE t = 2) == (SELECT VALUE id FROM child WITH NOINDEX WHERE t = 2) },
	{ q: 'child stale after delete (plan)', ok: (EXPLAIN SELECT VALUE id FROM child WHERE t = 2).contains('Index') },
	{ q: 'agg desc', ok: (SELECT n, mx, g FROM agg ORDER BY mx DESC) == (SELECT n, mx, g FROM agg WITH NOINDEX ORDER BY mx DESC) },
	{ q: 'agg desc (plan)', ok: (EXPLAIN SELECT n, mx, g FROM agg ORDER BY mx DESC).contains('Index') },
	{ q: 'agg equal', ok: (SELECT VALUE id FROM agg WHERE mx = 10) == (SELECT VALUE id FROM agg WITH NOINDEX WHERE mx = 10) },
	{ q: 'agg equal (plan)', ok: (EXPLAIN SELECT VALUE id FROM agg WHERE mx = 10).contains('Index') },
	{ q: 'agg stale', ok: (SELECT VALUE id FROM agg WHERE mx = 3) == (SELECT VALUE id FROM agg WITH NOINDEX WHERE mx = 3) },
	{ q: 'agg stale (plan)', ok: (EXPLAIN SELECT VALUE id FROM agg WHERE mx = 3).contains('Index') },
	{ q: 'filt desc', ok: (SELECT VALUE t FROM filt ORDER BY t DESC) == (SELECT VALUE t FROM filt WITH NOINDEX ORDER BY t DESC) },
	{ q: 'filt desc (plan)', ok: (EXPLAIN SELECT VALUE t FROM filt ORDER BY t DESC).contains('Index') },
	{ q: 'filt equal', ok: (SELECT VALUE t FROM filt WHERE t = 10) == (SELECT VALUE t FROM filt WITH NOINDEX WHERE t = 10) },
	{ q: 'filt equal (plan)', ok: (EXPLAIN SELECT VALUE t FROM filt WHERE t = 10).contains('Index') },
	{ q: 'filt stale after leaving the filter', ok: (SELECT VALUE id FROM filt WHERE t = 3) == (SELECT VALUE id FROM filt WITH NOINDEX WHERE t = 3) },
	{ q: 'filt stale after leaving the filter (plan)', ok: (EXPLAIN SELECT VALUE id FROM filt WHERE t = 3).contains('Index') }
].filter(|$c| !$c.ok).map(|$c| $c.q);
