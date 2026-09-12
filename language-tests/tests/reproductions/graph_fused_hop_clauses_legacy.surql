/**
[env]
namespace = true
database = true
planner-strategy = ["compute-only"]

[test]
reason = """
Half of a pair. Same queries as
`graph_fused_hop_clauses_streaming.surql`, pinned to the legacy compute path;
the two files' expected results are the divergence.

A run of consecutive graph hops is one path expression, and each hop carries
its own clauses: `ONLY`, and the `LIMIT`/`ORDER`/`WHERE` of the `SELECT` form a
hop is written in. The legacy path evaluates a hop at a time, so a hop's
clauses apply to that hop's result, once per value the previous hop produced.
The streaming planner threads the hops into one operator chain — the output of
one scan is the input of the next — and reduces the run to a single set of
clauses over the chain as a whole. Two consequences are recorded here.

`ONLY` becomes the run's, not the hop's: a run holding one is unwrapped
whatever position that hop sits at, so a single-hop `ONLY` inside a two-hop run
answers one value where the legacy path answers a one-element array. The two
controls bound it — `ONLY` on every hop unwraps on both engines, and `ONLY` on
none unwraps on neither — which is what shows the divergence is the mixture
rather than the keyword.

A `LIMIT` becomes the run's too: the last hop's `LIMIT 1` keeps one row for the
whole traversal rather than one per value the middle hop produced, so a run
over a middle hop that fans out to two reports one leaf where the legacy path
reports two. Which leaf survives the run-global limit is not fixed, which is
why the length is what this asserts.

The legacy answers are the intended contract: a clause a hop is written with
belongs to that hop. Honouring it needs the fusion to evaluate each hop per
input value, which is the work the fusion exists to avoid, so this is a design
question rather than a patch. Pinned so the difference is visible while both
engines are live.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[person:b]"

[[test.results]]
value = "[person:b]"

[[test.results]]
value = "person:b"

[[test.results]]
value = "[person:b]"

[[test.results]]
value = "2"

[[test.results]]
value = "[leaf:l1, leaf:l2, leaf:l3, leaf:l4]"
*/

-- A linear graph, so nothing here turns on the order a fan-out arrives in.
CREATE person:a, person:b RETURN NONE;
RELATE person:a->knows->person:b RETURN NONE;

-- A branching one, for the clause a hop carries rather than the ONLY.
CREATE hub:h, mid:m1, mid:m2, leaf:l1, leaf:l2, leaf:l3, leaf:l4 RETURN NONE;
RELATE hub:h->down->mid:m1 RETURN NONE;
RELATE hub:h->down->mid:m2 RETURN NONE;
RELATE mid:m1->down->leaf:l1 RETURN NONE;
RELATE mid:m1->down->leaf:l2 RETURN NONE;
RELATE mid:m2->down->leaf:l3 RETURN NONE;
RELATE mid:m2->down->leaf:l4 RETURN NONE;

-- ONLY on the first hop of a two-hop run.
SELECT VALUE ->(SELECT VALUE id FROM ONLY knows LIMIT 1)->person FROM ONLY person:a;

-- ONLY on the last hop of one.
SELECT VALUE ->knows->(SELECT VALUE id FROM ONLY person LIMIT 1) FROM ONLY person:a;

-- Control: ONLY on every hop. Both engines unwrap.
SELECT VALUE ->(SELECT VALUE id FROM ONLY knows LIMIT 1)->(SELECT VALUE id FROM ONLY person LIMIT 1) FROM ONLY person:a;

-- Control: ONLY on no hop. Both engines answer an array.
SELECT VALUE ->knows->person FROM ONLY person:a;

-- A `LIMIT` the last hop carries, over a run whose middle hop fans out to
-- two. The length is the assertion: which leaf the run-global limit keeps is
-- not fixed, and the count is what differs.
SELECT VALUE array::len(->down->mid->(SELECT VALUE out FROM down LIMIT 1)) FROM ONLY hub:h;

-- Control: the same run with no clause on any hop.
SELECT VALUE array::sort(->down->mid->(SELECT VALUE out FROM down)) FROM ONLY hub:h;