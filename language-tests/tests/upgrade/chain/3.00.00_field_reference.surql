/**
[env]
namespace = true
database = true

[test]
upgrade = true
reason = """
Walks one datastore through every version in the chain. The first version
defines the REFERENCE field, its ON DELETE behaviour and the COMPUTED
back-traversal field, and writes the author plus one book; every later version
links one more book to that same author.

The Ref key family therefore ends up holding back-link entries written by a
different binary per hop, all of which must resolve through a definition an
older release wrote. A pairwise hop only ever reads back-links produced by one
writer; this asserts that every release in turn appended to the family without
disturbing what its predecessors put there."""

[test.chain]
since = ">=3.0.0"
setup = "upgrade/chain/3.00.00_field_reference_setup.surql"
mutate = "upgrade/chain/3.00.00_field_reference_mutate.surql"

[[test.results]]
match = '''string::contains($result.fields.author, "REFERENCE ON DELETE UNSET")'''

[[test.results]]
match = '''string::contains($result.fields.books, "COMPUTED <~chain_field_reference_book")'''

[[test.results]]
match = '''count($result) == $hop + 1 AND $result[0] == chain_field_reference_book:0 AND $result[$hop] == type::record('chain_field_reference_book', $hop)'''

[[test.results]]
match = '''count($result) == $hop + 1 AND array::distinct($result) == [chain_field_reference_author:seed]'''

[[test.results]]
match = '''count($result) == $hop AND count(array::distinct($result)) == $hop'''

*/
INFO FOR TABLE chain_field_reference_book;
INFO FOR TABLE chain_field_reference_author;
-- The back-reference list grows by one book per version walked. It comes back
-- in referencing-record-id order, so book 0 — the entry the first version wrote
-- — is still first and the book this hop's binary wrote is last.
SELECT VALUE books FROM ONLY chain_field_reference_author:seed;
-- Read the same links from the owning side: every book, whichever binary wrote
-- it, still resolves to the one author.
SELECT VALUE author FROM chain_field_reference_book;
-- One book per version walked so far, each stamped by the binary that wrote it:
-- proof that every release in turn appended to the reference family, not just
-- the last one. Book 0 carries no `hop` and is excluded.
SELECT VALUE wrote_at FROM chain_field_reference_book WHERE hop != NONE;
