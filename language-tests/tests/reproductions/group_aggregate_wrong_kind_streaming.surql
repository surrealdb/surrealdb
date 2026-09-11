/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
Half of a pair. Same queries as `group_aggregate_wrong_kind_legacy.surql`,
pinned to the streaming planner; the two files' expected results are the
divergence.

What an aggregate does with a row whose value it cannot fold. The streaming
answers here are the intended contract, and they turn on whether the group has
an answer at all:

- A wrong-kind value is skipped when the group folded at least one usable value, so one malformed
  row in heterogeneous data does not discard the rest. Legacy errors instead, because it reduces a
  group by collecting the values and handing the whole collection to the scalar form, which
  type-checks every element.
- A wrong-kind value raises when the group folded nothing — the third query, where both engines
  produce the *same* error. That agreement is the point of including it: the alternative was
  returning the accumulator's identity (`0` here), which no caller can distinguish from a real
  sum, and it is what the streaming path used to do.
- An absent value is skipped rather than ordered. Legacy orders it, and because `NONE` sorts below
  every other value that makes the minimum of any sparsely populated column `NONE` — the fourth
  query. Ignoring nulls is what SQL specifies for an aggregate.

Legacy is the wrong answer in the first, second and fourth: erroring over
heterogeneous data is hostile in a schemaless store, and a `NONE` minimum
reports the absence of a value as though it were the smallest one.
"""

[[test.results]]
value = "[{ g: 1, id: s:1, n: 10 }]"

[[test.results]]
value = "[{ g: 1, id: s:2, n: 20 }]"

[[test.results]]
value = "[{ g: 1, id: s:3, n: 'oops' }]"

[[test.results]]
value = "[{ g: 2, id: s:4, n: 'bad' }]"

[[test.results]]
value = "[{ g: 3, id: s:5, w: 'pear' }]"

[[test.results]]
value = "[{ g: 3, id: s:6 }]"

[[test.results]]
value = "[{ t: 30 }]"

[[test.results]]
value = "[{ lo: 10 }]"

[[test.results]]
error = "Incorrect arguments for function math::sum(). Argument 1 was the wrong type. Expected `number` but found `'bad'`"

[[test.results]]
value = "[{ hi: 'pear', lo: 'pear' }]"
*/

CREATE s:1 SET g = 1, n = 10;
CREATE s:2 SET g = 1, n = 20;
CREATE s:3 SET g = 1, n = 'oops';
CREATE s:4 SET g = 2, n = 'bad';
CREATE s:5 SET g = 3, w = 'pear';
CREATE s:6 SET g = 3;

-- Two numbers fold and the string is skipped.
SELECT math::sum(n) AS t FROM s WHERE g = 1 GROUP ALL;
SELECT math::min(n) AS lo FROM s WHERE g = 1 GROUP ALL;

-- Nothing folds, so the wrong-kind value raises instead of reporting `0`.
-- Both engines agree here; keep this query when editing the pair.
SELECT math::sum(n) AS t FROM s WHERE g = 2 GROUP ALL;

-- One row carries `w` and one does not. The absent value is skipped, not
-- ordered below `'pear'`.
SELECT array::min(w) AS lo, array::max(w) AS hi FROM s WHERE g = 3 GROUP ALL;
