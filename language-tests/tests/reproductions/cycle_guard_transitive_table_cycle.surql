/**
[test]
reason = "Plan-time cycle guard handles transitive cycles between tables (A's permission references B, B's permission references A). All compilation succeeds and execution returns correct results."

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: tab_a:a1, val: 'alpha' }, { id: tab_a:a2, val: 'public-a' }]"

[[test.results]]
value = "[{ id: tab_b:b1, val: 'beta' }, { id: tab_b:b2, val: 'public-b' }]"

[[test.results]]
value = "[{ id: tab_a:a2, val: 'public-a' }]"

[env]
namespace = true
database = true
auth = { level = "owner" }

*/

-- A's permission references B; B's permission references A. Compile-time:
-- try_resolve(A) → push A → resolve_table_context(A)
--   compile A's perm → SELECT FROM B → try_resolve(B) → push B
--     resolve_table_context(B) → compile B's perm → SELECT FROM A
--       → try_resolve(A) → A in guard → fall back (runtime resolution)
--   ... B fully resolves with inner-A as runtime fallback
-- ... A fully resolves with B fully resolved
{
    DEFINE TABLE tab_a SCHEMAFULL
        PERMISSIONS FOR SELECT WHERE
            val IN (SELECT VALUE val FROM tab_b WHERE val = $this.val);

    DEFINE TABLE tab_b SCHEMAFULL
        PERMISSIONS FOR SELECT WHERE
            val IN (SELECT VALUE val FROM tab_a WHERE val = $this.val);

    DEFINE FIELD val ON tab_a TYPE string;
    DEFINE FIELD val ON tab_b TYPE string;

    CREATE tab_a:a1 SET val = 'alpha';
    CREATE tab_a:a2 SET val = 'public-a';
    CREATE tab_b:b1 SET val = 'beta';
    CREATE tab_b:b2 SET val = 'public-b';

    RETURN 'OK';
};

-- Outer plan must compile both tab_a and tab_b without infinite recursion.
SELECT * FROM tab_a ORDER BY id;

SELECT * FROM tab_b ORDER BY id;

-- A WHERE clause that exercises the predicate-compilation path through
-- the same cycle guard.
SELECT * FROM tab_a WHERE val = 'public-a' ORDER BY id;
