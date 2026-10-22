/**
[env]
imports = ["reproductions/relate_onto_a_hidden_edge_is_refused_whatever_it_names_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
	A re-RELATE onto a stored edge is refused when it names an endpoint other
	than the one the edge holds. For a caller whose reads are checked, an
	endpoint it may not read counts as differing whatever the statement names
	(GHSA-3rcm-c653-wgmr), so whether the re-RELATE is refused or written never
	tells it whether a guess was right: a correct guess and a wrong one are both
	refused, whether the table's `select` permission hides the edge (`wrote`),
	its `in` and `out` fields' permissions hide them (`read`), or a `select`
	predicate hides the edge (`likes:theirs`). An edge the caller may read is
	judged by its endpoints alone: naming the ones it holds writes it, and
	naming another is refused.
"""

[[test.results]]
error = "Cannot change the `in` or `out` endpoint of an existing edge"

[[test.results]]
error = "Cannot change the `in` or `out` endpoint of an existing edge"

[[test.results]]
error = "Cannot change the `in` or `out` endpoint of an existing edge"

[[test.results]]
error = "Cannot change the `in` or `out` endpoint of an existing edge"

[[test.results]]
error = "Cannot change the `in` or `out` endpoint of an existing edge"

[[test.results]]
value = "[likes:own]"

[[test.results]]
error = "Cannot change the `in` or `out` endpoint of an existing edge"
*/

-- The table's `select` permission hides the edge: the stored endpoints and a
-- wrong `in` are refused alike.
RELATE person:carol->wrote:edge77->book:diary;
RELATE person:mallory->wrote:edge77->book:diary;

-- The `in` and `out` fields' `select` permissions hide them.
RELATE person:carol->read:edge77->book:diary;
RELATE person:mallory->read:edge77->book:diary;

-- The `select` predicate hides an edge whose `in` is not the caller.
RELATE user:bob->likes:theirs->book:diary;

-- An edge the caller may read is judged by its endpoints.
RELATE user:alice->likes:own->book:diary RETURN VALUE id;
RELATE user:alice->likes:own->book:other;
