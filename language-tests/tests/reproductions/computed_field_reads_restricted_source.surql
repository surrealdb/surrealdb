/**
[env]
imports = ["reproductions/computed_field_reads_restricted_source_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "The streaming read path evaluated computed fields against the full record and only then removed the fields the caller may not select, so a readable computed field kept the value it derived from a denied one. The legacy read path re-derives computed fields against the reduced document; this asserts the streaming path does the same, that re-deriving does not reintroduce a computed field whose own permissions deny it, and that a computed field's own clause is answered with the re-derived value. Each query reaches the record through a different one of the four read paths that order compute against permissions."

# Scanned by record id. `secret` is absent, as its permissions require, and
# `derived` computes from it, so it must be NONE rather than carrying the value
# across. `denied` is a computed field the caller may not read at all, so it
# must stay absent. `gated`'s own clause reads `$value` and is answered with the
# value the caller receives, so it is allowed and present as NONE; answering it
# on the pre-reduction value would deny it, and the missing key would then
# report what `secret` holds.
[[test.results]]
value = "[{ derived: NONE, gated: NONE, id: items:one }]"

# The same through an explicit projection of just the readable computed field.
[[test.results]]
value = "[{ derived: NONE }]"

# Resolved by a WHERE predicate rather than by the record-id fast path.
[[test.results]]
value = "[{ derived: NONE, gated: NONE, id: items:one }]"

# Nothing stored is cut on this table, so no re-derivation runs — but the
# computed fields' own clauses must still be applied, so `hidden` is absent.
[[test.results]]
value = "[{ id: bare:one, note: 'NOTE', shown: 'SHOWN' }]"

# Dereferenced through a record link, which resolves the target through the
# single-record fetch path.
[[test.results]]
value = "[{ derived: NONE, secret: NONE }]"

# Reached by a graph traversal, which resolves the target through the batched
# linked-record path.
[[test.results]]
value = "[{ derived: [NONE] }]"

*/

SELECT * FROM items:one;
SELECT derived FROM items:one;
SELECT * FROM items WHERE id = items:one;
SELECT * FROM bare:one;
SELECT link.derived AS derived, link.secret AS secret FROM holders:one;
SELECT ->owns->items.derived AS derived FROM account:alice;
