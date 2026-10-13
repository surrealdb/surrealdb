/**
[env]
namespace = true
database = true

[test]
upgrade = true
reason = """
Walks one datastore through every version in the chain. The UNIQUE index is
defined by the first version, and each later version inserts a fresh email
through it, reads back every row any earlier version wrote, and re-tests that a
duplicate is still rejected.

This is the property adjacent-pair testing cannot express: not that one release
can read the index the release before it wrote, but that a uniqueness
constraint created once keeps both halves of its contract — the lookup and the
rejection — while every release in turn writes new keys into it."""

[test.chain]
since = ">=3.0.0"
setup = "upgrade/chain/3.00.00_index_unique_setup.surql"
mutate = "upgrade/chain/3.00.00_index_unique_mutate.surql"

[[test.results]]
match = '''$result.indexes.chain_uniq_email == "DEFINE INDEX chain_uniq_email ON chain_uniq FIELDS email UNIQUE"'''

[[test.results]]
value = "[{ email: 'first@example.com', hop: 0, id: chain_uniq:first }]"

[[test.results]]
match = '''$result[0].count == $hop + 1'''

[[test.results]]
match = '''$result == array::range(0, $hop + 1)'''

[[test.results]]
error = true

*/
INFO FOR TABLE chain_uniq;
-- The row the first walked version wrote is still found by its indexed column,
-- after every later release has written new keys into the same index.
SELECT * FROM chain_uniq WITH INDEX chain_uniq_email WHERE email = 'first@example.com';
SELECT count() FROM chain_uniq GROUP ALL;
-- The setup row plus one row per version walked so far, each stamped with the
-- hop that wrote it: proof that every release in turn maintained the index.
SELECT VALUE hop FROM chain_uniq ORDER BY hop;
-- Uniqueness is still enforced by the index an older release built, so this
-- must fail at every hop. It stays in the graded body; in mutate its error
-- would abort the hop instead of being asserted.
CREATE chain_uniq SET email = 'first@example.com';
