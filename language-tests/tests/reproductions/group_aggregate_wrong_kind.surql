/**
[env]

[test]
reason = """
What an aggregate does with a row whose value it cannot fold. The contract
turns on whether the group has an answer at all:

- A wrong-kind value is skipped when the group folded at least one usable
  value, so one malformed row in heterogeneous data does not discard the rest.
- A wrong-kind value raises when the group folded nothing -- the third query.
  Returning the accumulator's identity (`0` here) instead is indistinguishable
  from a real sum.
- An absent value is skipped rather than ordered. `NONE` sorts below every
  other value, so ordering it would make the minimum of any sparsely populated
  column `NONE` -- the fourth query. Ignoring nulls is what SQL specifies for
  an aggregate.
- When every row of the group is missing the value there is nothing left to
  skip to, so the group reports the aggregate's answer over no values -- the
  last two queries.
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

[[test.results]]
value = "[{ t: 0 }]"

[[test.results]]
value = "[{ lo: NONE }]"
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
-- Keep this query when editing the pair above.
SELECT math::sum(n) AS t FROM s WHERE g = 2 GROUP ALL;

-- One row carries `w` and one does not. The absent value is skipped, not
-- ordered below `'pear'`.
SELECT array::min(w) AS lo, array::max(w) AS hi FROM s WHERE g = 3 GROUP ALL;

-- Every row of the group is missing the value, so there is nothing to fold
-- and nothing to skip to.
SELECT math::sum(nope) AS t FROM s WHERE g = 3 GROUP ALL;
SELECT time::min(nope) AS lo FROM s WHERE g = 3 GROUP ALL;
