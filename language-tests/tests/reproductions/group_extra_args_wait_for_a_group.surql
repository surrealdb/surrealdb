/**
[env]
namespace = true
database = true

[test]
reason = """
An aggregate's arguments after the first describe the group rather than a row —
the separator of `array::join(txt, ' ')` — so they are evaluated once for the
statement rather than once per row. That one evaluation is owed to a group, and
which groups exist is what decides whether it happens: `GROUP ALL` has its
group whatever reaches it, while a `GROUP BY` key can only be learned from a
row, so an input that yields none opens nothing to evaluate for.

An argument may write, raise or sleep, which is what makes this worth pinning
rather than an implementation detail. A `THROW` is what makes it observable:
the evaluation is the whole effect, so nothing else has to be asserted to see
whether it happened.

The non-empty cases are the control that keeps this honest — an evaluation owed
to a group must not be skipped — and they pin the other half of the contract
too: one evaluation for the whole group, not one per row.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 'x', g: 1, id: p:1 }]"

[[test.results]]
value = "[{ a: 'y', g: 1, id: p:2 }]"

[[test.results]]
error = "An error occurred: boom"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ j: 'x-y' }]"

[[test.results]]
value = "[{ g: 1, j: 'x-y' }]"
*/

DEFINE TABLE e SCHEMALESS;
CREATE p:1 SET g = 1, a = 'x';
CREATE p:2 SET g = 1, a = 'y';

-- `GROUP ALL` has its group even over an empty table, so the separator is
-- evaluated for it; no `GROUP BY` key exists, so nothing is evaluated there.
SELECT array::join(a, THROW 'boom') AS j FROM e GROUP ALL;
SELECT a, array::join(a, THROW 'boom') AS j FROM e GROUP BY a;

-- Controls: a group that a row opened still evaluates the separator, once.
SELECT array::join(a, '-') AS j FROM p GROUP ALL;
SELECT g, array::join(a, '-') AS j FROM p GROUP BY g;
