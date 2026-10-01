/**
[env]
imports = ["upgrade/define/3.00.00_value_depth_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
# Only a binary that does not bound a stored value's nesting depth can write the
# state, and only one that does can show the bound holding, so the hop is pinned
# from both ends. The pair together also keeps the ordinary language-test runner
# away: it matches both fields against its own version, and replaying the import
# against a build that enforces the bound would fail at the write.
importing-version = "<3.4.0-0"
version = ">=3.4.0-a"
# The depth limit has one enforcement point, on the write path. Reading recurses
# once per nesting level through the derived `DeserializeRevisioned` impls for
# `Value` and `Array`, which apply no limit, so how deep a stored value may be
# read is set by whichever thread stack the decode lands on rather than by any
# bound. The KVS pool that a rocksdb scan visitor can be dispatched to takes the
# default stack, several times smaller than a runtime worker's, and a debug
# build spends enough stack per level to exhaust it well inside the write bound.
#
# Every seed stores the same depth; none of them truncates the write. Seeds from
# 3.1.0 onward pass because that release's wire format lets a pre-decode filter
# skip a value it rejects without decoding it, and the one statement here that
# runs on the small stack filters the deep row out. On the earlier format the
# whole record has to be materialised to evaluate that same filter. Those passes
# are real coverage of the surrounding contract — the row enumerates, its value
# comes off disk whole, a rewrite is refused with a typed error, and the table
# keeps serving — but they do not exercise the unbounded recursion.
wip = true
reason = """
A record's whole value tree is stored under one revisioned encoding, and nothing
in that encoding records how deeply it nests. The bound the reading binary
applies is therefore a property of the reader alone: a row already on disk can
sit past it, and the reader has to stay correct in its presence rather than
assume it never occurs.

This is the only place that state is produced, because it cannot be produced by
the reader — a build enforcing the bound refuses the write that would create it,
so the row has to come from an older binary. The seed axis carries it: every
released version writes the row and hands it straight to the current checkout.

What has to hold is a three-part contract, and the assertions below are ordered
to separate its parts. The row still decodes off storage, so the table it lives
in stays enumerable and its neighbours stay reachable. Rewriting that row is
refused, with a typed error rather than a torn write. And the refusal is
confined to the row that crosses the bound: data the same old binary wrote
within it is returned in full, from a server still serving.

The body never returns the over-deep value itself. That the value nests past the
bound is established by the write refusal, which walks the same tree and reports
per statement, rather than by reading the value back."""

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[1]"

[[test.results]]
error = "The value is nested too deeply. A value may be nested at most 256 levels; reduce the nesting or split the data across records"

[[test.results]]
value = "0"

[[test.results]]
value = "[{ id: up_vdepth_shallow:1, v: [[[[[1]]]]] }]"

*/
-- The row the old binary left behind is still in the table: a scan over the key
-- range counts it like any other.
SELECT count() FROM up_vdepth_deep GROUP ALL;
-- Its value came off disk whole rather than as NONE or a truncated stub: the
-- decoder applies no depth bound of its own, and the outermost array still
-- holds exactly the one element it was written with.
SELECT VALUE array::len(v) FROM up_vdepth_deep:1;
-- Rewriting the row is refused. The bound is checked over the value on its way
-- back to storage, so this errors only because what was read really does nest
-- past it — which is what makes the assertions above about a genuine violation
-- rather than an ordinary record.
UPDATE up_vdepth_deep:1 SET w = 1 RETURN NONE;
-- The refusal fails closed: the rejected write left nothing behind, so no row
-- carries the field it would have set.
RETURN array::len((SELECT id FROM up_vdepth_deep WHERE w = 1));
-- Liveness, and the reason it comes last: the server is still serving and the
-- store is still intact after the refusal, and a record the same old binary
-- wrote within the bound comes back in full. Without it the test cannot tell a
-- contained, typed rejection from a store that stopped answering.
SELECT * FROM up_vdepth_shallow:1;
