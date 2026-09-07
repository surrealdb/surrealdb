/**
[env]
namespace = true
database = true

[test]
reason = """
An indexed `SET` or `UNSET` path applies to an `array` but is mostly dropped on
a `set`. The two halves of this test run the same six statements and differ
only in the declared type of `items`.

Against the array every statement lands. Against the set only `items[$]` does:
`items[0]`, `items[1]` and `items[*]` write nothing, and neither `UNSET
items[$]` nor `UNSET items[*]` removes anything. No error is raised — the write
is discarded silently, so a caller cannot tell the difference from a write that
had no work to do.

The split follows the part kinds handled by the async mutators behind a data
clause: the last-element part has a set arm, while the numeric-index and
all-elements arms are written for arrays and objects and fall through for a
set. The synchronous `Value::put` handles all four part kinds over a set, so
the two implementations disagree about what these statements mean.

This records the behaviour as it stands, not the behaviour that would be
consistent, so that a change on either side of that disagreement is visible.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: st:1, items: {{ n: 1 }, { n: 2 }, { n: 3 }} }]"

[[test.results]]
value = "[{ id: st:1, items: {{ n: 1 }, { n: 2 }, { n: 3 }} }]"

[[test.results]]
value = "[{ id: st:1, items: {{ n: 1 }, { n: 2 }, { n: 3 }} }]"

[[test.results]]
value = "[{ id: st:1, items: {{ n: 1 }, { n: 2 }, { n: 3 }} }]"

[[test.results]]
value = "[{ id: st:1, items: {{ last: true, n: 3 }, { n: 1 }, { n: 2 }} }]"

[[test.results]]
value = "[{ id: st:1, items: {{ last: true, n: 3 }, { n: 1 }, { n: 2 }} }]"

[[test.results]]
value = "[{ id: st:1, items: {{ last: true, n: 3 }, { n: 1 }, { n: 2 }} }]"

[[test.results]]
value = "[{ id: ar:1, items: [{ n: 1 }, { n: 2 }, { n: 3 }] }]"

[[test.results]]
value = "[{ id: ar:1, items: [{ n: 1 }, { mid: true, n: 2 }, { n: 3 }] }]"

[[test.results]]
value = "[{ id: ar:1, items: [{ first: true, n: 1 }, { mid: true, n: 2 }, { n: 3 }] }]"

[[test.results]]
value = "[{ id: ar:1, items: [{ all: true, first: true, n: 1 }, { all: true, mid: true, n: 2 }, { all: true, n: 3 }] }]"

[[test.results]]
value = "[{ id: ar:1, items: [{ all: true, first: true, n: 1 }, { all: true, mid: true, n: 2 }, { all: true, last: true, n: 3 }] }]"

[[test.results]]
value = "[{ id: ar:1, items: [{ all: true, first: true, n: 1 }, { all: true, mid: true, n: 2 }] }]"

[[test.results]]
value = "[{ id: ar:1, items: [] }]"
*/

DEFINE TABLE st SCHEMALESS;
DEFINE FIELD items ON st TYPE set<object>;
DEFINE TABLE ar SCHEMALESS;

CREATE st:1 SET items = [{ n: 1 }, { n: 2 }, { n: 3 }];
UPDATE st:1 SET items[1].mid = true;
UPDATE st:1 SET items[0].first = true;
UPDATE st:1 SET items[*].all = true;
UPDATE st:1 SET items[$].last = true;
UPDATE st:1 UNSET items[$];
UPDATE st:1 UNSET items[*];

CREATE ar:1 SET items = [{ n: 1 }, { n: 2 }, { n: 3 }];
UPDATE ar:1 SET items[1].mid = true;
UPDATE ar:1 SET items[0].first = true;
UPDATE ar:1 SET items[*].all = true;
UPDATE ar:1 SET items[$].last = true;
UPDATE ar:1 UNSET items[$];
UPDATE ar:1 UNSET items[*];
