/**
[env]
namespace = true
database = true

[test]
reason = """
`_join<n>` is the form a join mints for the alias naming its matched right
row, and a read off that alias reads a row already in hand. The name is no
proof of one, though: a plan that carries `_join<n>` anywhere reserves that
index and a join then mints a different one, precisely so a user's field may
keep the name. Such a field may hold a record link, and reading through it
fetches — deriving COMPUTED fields that need not answer the same twice. So the
rules may not read the name as a tell, and the predicate above a projection of
one stays put rather than being duplicated into the scan beneath it.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "{ attributes: { projections: '*' }, children: [{ attributes: { predicate: 'x' }, children: [{ children: [{ attributes: { projections: 'x' }, children: [{ attributes: { direction: 'Forward', table: 't' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }], context: 'Rt', operator: 'Subquery' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'SelectProject' }"
*/

DEFINE TABLE cfg SCHEMALESS;
DEFINE TABLE t SCHEMALESS;
CREATE cfg:one RETURN NONE;
-- A user's field, which happens to carry the minted form as its name.
CREATE t:one SET _join0 = cfg:one RETURN NONE;
DEFINE FIELD bad ON cfg COMPUTED <int> 'x';
DEFINE FIELD coin ON cfg COMPUTED rand::bool();

-- Read through, the field fetches `cfg:one` and derives `bad`, so it raises.
-- That is what makes it a dereference and not a read of a row in hand.
SELECT VALUE _join0.bad FROM t:one;

-- The projection's path is therefore not duplicated into a predicate above
-- it: `Filter [predicate: x]` stands over the projection, and the scan
-- carries no predicate of its own.
SELECT * FROM (SELECT _join0.coin AS x FROM t) WHERE x EXPLAIN;
