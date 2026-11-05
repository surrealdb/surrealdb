/**
[env]
namespace = true
database = true

[test]
reason = """
An aggregate's arguments after the first — the separator of
`array::join(txt, sep)` — describe a group rather than a row, so each group is
owed exactly one evaluation of them: not one per row, and not one for the
whole statement with every later group reusing it. A sequence counts the
evaluations directly, and the value each group joined on says which draw
reached it.

The groups are evaluated in group-key order, which is the order they are
reported in. That is why the second query interleaves the groups in the input
and puts the later key first: an argument evaluated when a group is opened
would hand the first draw to `b`.

`GROUP ALL` names one group, so it draws once however many rows arrive, and a
`GROUP BY` whose input yields no row opens no group and draws nothing at all —
the argument may write or advance a sequence, so a group that does not exist
must not pay for one.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ g: 'a', x: 'x0y' }, { g: 'b', x: 'p1q' }]"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ g: 'a', x: 'x0y' }, { g: 'b', x: 'p1q' }]"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ x: 'x0y0p0q' }]"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "0"
*/

-- One draw per group, and each group joins on its own.
DEFINE SEQUENCE s BATCH 1000 START 0;
SELECT array::join(w, sequence::nextval('s').to_string()) AS x, g
FROM [{ g: 'a', w: 'x' }, { g: 'a', w: 'y' }, { g: 'b', w: 'p' }, { g: 'b', w: 'q' }]
GROUP BY g ORDER BY g;
RETURN sequence::nextval('s');

-- The same answers when the groups interleave and the later key arrives first:
-- the draws follow group-key order, not the order the groups were opened.
DEFINE SEQUENCE m BATCH 1000 START 0;
SELECT array::join(w, sequence::nextval('m').to_string()) AS x, g
FROM [{ g: 'b', w: 'p' }, { g: 'a', w: 'x' }, { g: 'b', w: 'q' }, { g: 'a', w: 'y' }]
GROUP BY g ORDER BY g;
RETURN sequence::nextval('m');

-- `GROUP ALL` is one group, so one draw however many rows it folds.
DEFINE SEQUENCE t BATCH 1000 START 0;
SELECT array::join(w, sequence::nextval('t').to_string()) AS x
FROM [{ w: 'x' }, { w: 'y' }, { w: 'p' }, { w: 'q' }]
GROUP ALL;
RETURN sequence::nextval('t');

-- No row means no group, and no group is owed an evaluation.
DEFINE SEQUENCE z BATCH 1000 START 0;
SELECT array::join(w, sequence::nextval('z').to_string()) AS x, g FROM [] GROUP BY g;
RETURN sequence::nextval('z');
