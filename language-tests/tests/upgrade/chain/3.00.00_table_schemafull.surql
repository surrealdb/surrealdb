/**
[env]
namespace = true
database = true

[test]
upgrade = true
reason = """
Walks one datastore through every version in the chain. The SCHEMAFULL table and
its typed fields are defined by the first version, and each later version writes
a conforming row through that schema and then re-reads every row any earlier
version wrote.

A pairwise hop can only show that the release after the writer still honours the
schema. This walk shows that a schema authored once keeps coercing, defaulting
and rejecting identically while every release in turn writes through it — and
that the row the oldest binary stored comes back unchanged after all of them
have."""

[test.chain]
since = ">=3.0.0"
setup = "upgrade/chain/3.00.00_table_schemafull_setup.surql"
mutate = "upgrade/chain/3.00.00_table_schemafull_mutate.surql"

[[test.results]]
match = '''string::contains($result.tables.chain_table_schemafull, "SCHEMAFULL")'''

[[test.results]]
match = '''
	string::contains($result.fields.name, "TYPE string ASSERT $value != NONE")
		&& string::contains($result.fields.age, "TYPE int ASSERT")
		&& string::contains($result.fields.email, "string::is_email")
		&& string::contains($result.fields.tags, "TYPE array<string>")
		&& string::contains($result.fields.tags, "DEFAULT []")
		&& string::contains($result.fields.hop, "TYPE int")
		&& string::contains($result.fields.wrote_at, "TYPE string")
'''

[[test.results]]
match = '''
	count($result) == 1
		&& $result[0].name == 'user-1'
		&& $result[0].age == 21
		&& $result[0].email == 'user1@example.com'
		&& $result[0].tags == []
		&& ($hop == 1) == ($result[0].wrote_at == $version)
'''

[[test.results]]
match = '''$result == array::range(21, 21 + $hop)'''

[[test.results]]
match = '''count($result) == $hop && count(array::distinct($result)) == $hop'''

[[test.results]]
error = true

[[test.results]]
error = true

[[test.results]]
error = true

*/
INFO FOR DB;
INFO FOR TABLE chain_table_schemafull;
-- The row the first walked version wrote reads back exactly as it wrote it,
-- DEFAULT-filled `tags` included, after every later release has opened the
-- store and written through the same schema. Its `wrote_at` stamp names the
-- binary that created it, so it equals the running version at hop 1 and at no
-- other hop.
SELECT name, age, email, tags, wrote_at FROM chain_table_schemafull WHERE hop = 1;
-- One conforming row per version walked so far, each aged 20 + its hop.
SELECT VALUE age FROM chain_table_schemafull ORDER BY age;
-- Every row is stamped with the binary that wrote it, and no two hops share a
-- stamp: proof that every release in turn wrote through the schema, not just
-- the last one.
SELECT VALUE wrote_at FROM chain_table_schemafull;
-- The constraints the first version defined are still enforced by this one: an
-- ASSERT on a numeric range, an ASSERT calling a builtin, and a TYPE coercion.
-- Every other field is supplied so the rejection can only come from the one
-- under test. These must stay in the graded body -- an erroring statement in
-- `mutate` aborts the hop and ends the walk.
CREATE chain_table_schemafull SET name = 'Carol', age = -5, email = 'carol@example.com', hop = 0, wrote_at = 'probe';
CREATE chain_table_schemafull SET name = 'Dave', age = 40, email = 'not-an-email', hop = 0, wrote_at = 'probe';
CREATE chain_table_schemafull SET name = 'Erin', age = 'thirty', email = 'erin@example.com', hop = 0, wrote_at = 'probe';
