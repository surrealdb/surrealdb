/**
[test]
reason = "Plan-time cycle guard handles transitive cycles between tables when the inner subquery's WHERE is index-eligible. Exercises plan_btree_scan_source — the per-AccessPath method that compiles a BTreeScan against a resolved table — through the cycle-guard fallback. Pre-fix the same query stack-overflowed at plan time; the fix forces the inner same-table reference to fall back to runtime resolution while the outer scan still benefits from index pushdown."

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: idx_a:a1, val: 'alpha' }, { id: idx_a:a2, val: 'public-a' }]"

[[test.results]]
value = "[{ id: idx_b:b1, val: 'beta' }, { id: idx_b:b2, val: 'public-b' }]"

[[test.results]]
value = "[{ id: idx_a:a2, val: 'public-a' }]"

[env]
namespace = true
database = true
auth = { level = "owner" }

*/

-- Indexed variant of cycle_guard_transitive_table_cycle.surql. Both tables
-- have a BTree index on `val`; the cycle-spanning permissions reference
-- `val = $this.val` so the inner SELECT is index-eligible. The cycle guard
-- has to fall back for the same-table reference *before* index resolution
-- even tries to look up the access path — otherwise plan_btree_scan_source
-- runs against a table whose context is still being constructed.
{
    DEFINE TABLE idx_a SCHEMAFULL
        PERMISSIONS FOR SELECT WHERE
            val IN (SELECT VALUE val FROM idx_b WHERE val = $this.val);

    DEFINE TABLE idx_b SCHEMAFULL
        PERMISSIONS FOR SELECT WHERE
            val IN (SELECT VALUE val FROM idx_a WHERE val = $this.val);

    DEFINE FIELD val ON idx_a TYPE string;
    DEFINE FIELD val ON idx_b TYPE string;

    DEFINE INDEX idx_a_val ON idx_a FIELDS val;
    DEFINE INDEX idx_b_val ON idx_b FIELDS val;

    CREATE idx_a:a1 SET val = 'alpha';
    CREATE idx_a:a2 SET val = 'public-a';
    CREATE idx_b:b1 SET val = 'beta';
    CREATE idx_b:b2 SET val = 'public-b';

    RETURN 'OK';
};

-- Outer plan compiles both tables. Inner self-reference falls back; outer
-- index lookup still wins.
SELECT * FROM idx_a ORDER BY id;
SELECT * FROM idx_b ORDER BY id;

-- WHERE predicate hits the BTree access path on idx_a; the cycle-guard
-- branch fires while the planner is in plan_btree_scan_source.
SELECT * FROM idx_a WHERE val = 'public-a' ORDER BY id;
