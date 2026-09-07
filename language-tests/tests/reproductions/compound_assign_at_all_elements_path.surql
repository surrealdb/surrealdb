/**
[env]
namespace = true
database = true

[test]
reason = """
A compound assignment at an all-elements path aggregates across the elements
instead of applying per element, and writes the aggregate to each of them.

`SET items[*].n += 10` over `n` values of 1 and 2 leaves both elements holding
`[1, 2, 10]`: the collected values of every element, with the right-hand side
appended. The field changes type from a number to an array, and neither element
ends up with its own value incremented. `-=` aggregates the same way and then
subtracts nothing, because the operand is not an element of the collection it
was handed: two elements holding 10 and 20 both end up holding `[10, 20]`. For
`+=` and `+?=` over arrays, two elements holding `[1]` and `[2]` both end up
holding `[[1], [2], 9]`.

Plain `=` at the same path is correct: it writes the value to each element
separately. So is every operator at a single-element path, `[0]` and `[$]`
included. The aggregation is specific to the combination of a compound
operator and `[*]`.

The compound operators read through the path before writing, and a read at
`[*]` yields the collection of matching values rather than one value per
element; the operator is then applied once to that collection and the result
written back through the same path to every element.

This records the behaviour as it stands so that a change to it is visible.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: p:0, items: [{ n: 1 }, { n: 2 }] }]"

[[test.results]]
value = "[{ id: p:0, items: [{ n: 5 }, { n: 5 }] }]"

[[test.results]]
value = "[{ id: p:1, items: [{ n: 1 }, { n: 2 }] }]"

[[test.results]]
value = "[{ id: p:1, items: [{ n: [1, 2, 10] }, { n: [1, 2, 10] }] }]"

[[test.results]]
value = "[{ id: p:2, items: [{ n: 10 }, { n: 20 }] }]"

[[test.results]]
value = "[{ id: p:2, items: [{ n: [10, 20] }, { n: [10, 20] }] }]"

[[test.results]]
value = "[{ id: p:3, items: [{ l: [1] }, { l: [2] }] }]"

[[test.results]]
value = "[{ id: p:3, items: [{ l: [[1], [2], 9] }, { l: [[1], [2], 9] }] }]"

[[test.results]]
value = "[{ id: p:4, items: [{ l: [1] }, { l: [2] }] }]"

[[test.results]]
value = "[{ id: p:4, items: [{ l: [[1], [2], 9] }, { l: [[1], [2], 9] }] }]"

[[test.results]]
value = "[{ id: p:5, items: [{ n: 1 }, { n: 2 }] }]"

[[test.results]]
value = "[{ id: p:5, items: [{ n: 11 }, { n: 2 }] }]"

[[test.results]]
value = "[{ id: p:5, items: [{ n: 11 }, { n: 12 }] }]"
*/

-- `=` at `[*]` writes each element separately, which is the intended shape.
DEFINE TABLE p SCHEMALESS;
CREATE p:0 SET items = [{ n: 1 }, { n: 2 }];
UPDATE p:0 SET items[*].n = 5;

-- Each compound operator at the same path, against numbers and against arrays.
CREATE p:1 SET items = [{ n: 1 }, { n: 2 }];
UPDATE p:1 SET items[*].n += 10;
CREATE p:2 SET items = [{ n: 10 }, { n: 20 }];
UPDATE p:2 SET items[*].n -= 1;
CREATE p:3 SET items = [{ l: [1] }, { l: [2] }];
UPDATE p:3 SET items[*].l += [9];
CREATE p:4 SET items = [{ l: [1] }, { l: [2] }];
UPDATE p:4 SET items[*].l +?= [9];

-- The same operators at a single-element path are correct, so the aggregation
-- is specific to `[*]`.
CREATE p:5 SET items = [{ n: 1 }, { n: 2 }];
UPDATE p:5 SET items[0].n += 10;
UPDATE p:5 SET items[$].n += 10;
