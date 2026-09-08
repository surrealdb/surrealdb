/**
[env]
namespace = true
database = true

[test]
upgrade = true
reason = """
Walks one datastore through every version in the chain. The first version
defines a SCHEMAFULL table with a computed `VALUE` field, a `READONLY DEFAULT`
field and one seed row; every later version adds a row through those definitions
and then rewrites a dependency of the computed field on the seed row its
predecessors have been rewriting since.

Adjacent-pair testing can only show that the one release following the writer
recomputes the field once. This shows that a field definition an old binary
wrote still drives recomputation on every release in turn, that no value any
earlier binary computed has drifted from what the `VALUE` clause produces, and
that `READONLY` keeps rejecting writes for the whole life of the store."""

[test.chain]
since = ">=3.0.0"
setup = "upgrade/chain/3.00.00_field_computed_setup.surql"
mutate = "upgrade/chain/3.00.00_field_computed_mutate.surql"

[[test.results]]
match = '''
    string::contains($result.fields.full_name, "VALUE string::concat(name.first, ' ', name.last)")
    && string::contains($result.fields.created_at, "READONLY")
    && string::contains($result.fields.created_at, "DEFAULT d'2025-01-01T00:00:00Z'")
'''

[[test.results]]
match = '''
    $result[0].revision == $hop
    && $result[0].full_name == string::concat('Alice Rev', <string> $hop)
    && $result[0].created_at == d'2025-01-01T00:00:00Z'
'''

[[test.results]]
value = "[]"

[[test.results]]
match = '''
    count($result) == $hop
    && array::all(array::map($result, |$r, $i| $r.hop == $i + 1
        && $r.full_name == string::concat('Hop ', <string> $r.hop)))
'''

[[test.results]]
match = '''$result[0].count == $hop + 1'''

[[test.results]]
match = '''count(array::distinct($result)) == $hop'''

[[test.results]]
error = true

*/
INFO FOR TABLE chain_field_computed;
-- The seed row after this hop's rewrite, which mutate applied. Reading it here
-- rather than asserting on the write's own return value keeps the body free of
-- state changes, so grading it at every hop is repeatable.
SELECT revision, full_name, created_at FROM chain_field_computed:seed;
-- No stored computed value anywhere in the table has drifted from what the
-- VALUE clause produces, including the rows earlier binaries computed.
SELECT id FROM chain_field_computed WHERE full_name != string::concat(name.first, ' ', name.last);
-- One row per version walked so far, each computed by the binary that wrote it.
SELECT hop, full_name FROM chain_field_computed WHERE hop > 0 ORDER BY hop;
SELECT count() FROM chain_field_computed GROUP ALL;
-- Every walked version contributed exactly one row: proof that each release in
-- turn wrote through the definitions, not just the last one.
SELECT VALUE wrote_at FROM chain_field_computed WHERE hop > 0;
-- READONLY still rejects a write on a field the first walked version defined.
UPDATE chain_field_computed:seed SET created_at = d'2099-12-31T00:00:00Z';
