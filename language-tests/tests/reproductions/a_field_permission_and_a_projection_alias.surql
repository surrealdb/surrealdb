/**
[env]
namespace = true
database = true
imports = ["reproductions/a_field_permission_and_a_projection_alias_import.surql"]
auth = { namespace = "test", database = "test", access = "account", rid = "user:alice" }

[test]
reason = """
alice owns `p:1` and `p:3`; `p:2` is bob's, so its `g` must never be visible.
It is not, in every shape below: the value is redacted at the record, before
any projection can move or rename it.

A denied field read under its own name is emitted holding `NONE`: nothing is
disclosed, but the row admits the field exists. A projection alias reusing the
guarded name -- `SELECT n AS g` -- is returned, since `n` is unguarded and alice
may read it. A permission belongs to the field it is defined on, not to
whatever the output happens to call a value.
"""

[[test.results]]
value = "[{ g: 30 }, { g: NONE }, { g: 10 }]"

[[test.results]]
value = "[30, NONE, 10]"

[[test.results]]
value = "[{ g: 30, id: p:1, n: 1, owner: user:alice }, { id: p:2, n: 2, owner: user:bob }, { g: 10, id: p:3, n: 3, owner: user:alice }]"

[[test.results]]
value = "[{ n: 30 }, { n: NONE }, { n: 10 }]"

[[test.results]]
value = "[{ zz: 30 }, { zz: NONE }, { zz: 10 }]"

[[test.results]]
value = "[{ n: { inner: 30 } }, { n: { inner: NONE } }, { n: { inner: 10 } }]"

[[test.results]]
value = "[{ n: [30] }, { n: [NONE] }, { n: [10] }]"

[[test.results]]
value = "[{ n: 30 }, { n: NONE }, { n: 10 }]"

[[test.results]]
value = "[{ g: 1 }, { g: 2 }, { g: 3 }]"

[[test.results]]
value = "[{ g: 1, owner: user:alice }, { g: 2, owner: user:bob }, { g: 3, owner: user:alice }]"
*/

-- The guarded field, read as itself.
SELECT g FROM p;
SELECT VALUE g FROM p;
SELECT * FROM p;

-- The guarded field renamed: the value is judged at the record, so the
-- rename carries a redaction, never a permission.
SELECT g AS n FROM p;
SELECT g AS zz FROM p;
SELECT { inner: g } AS n FROM p;
SELECT [g] AS n FROM p;
SELECT VALUE { n: g } FROM p;

-- An unguarded field renamed onto the guarded fields name.
SELECT n AS g FROM p;
SELECT owner, n AS g FROM p;

