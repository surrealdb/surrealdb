/**
[test]
reason = """
A grouped projection reduces to one output value per group, and which
reduction applies depends on the expression. Three contracts share this
pipeline, and no override is set here because all planner strategies owe the
same answers for every one of them:

- An expression the row cannot influence — a literal, a composite literal, a
  constant arithmetic expression, a param, a builtin call over any of those —
  has a single value for the group and is reported as that value, whatever the
  group's row count. `GROUP ALL` included, where the group is the whole table.
  Whether a call appears makes no difference: `math::abs(-7)` reduces exactly
  as `-7` and `4 + 3` do, since the decision is made by reading the expression
  to its leaves rather than by matching its shape. A rule that reduced some
  constant expressions and collected others could not be predicted from the
  query text, so the uniformity is asserted here rather than left to chance.
  An idiom is judged the same way, by its root: `name` reads the row, while
  `$conf.label`, `({ label: 'x' }).label` and `[1, 2][0]` are rooted in an
  explicit expression and are no more row-dependent than it is. `$conf` and
  `$conf.label` reducing differently would be the same unpredictability in
  another guise.
- `array::group` returns the *unique* items, agreeing with its scalar form, so
  a group holding apple/banana/apple reports two names and `array::len` over it
  reports 2.
- A projection that reads the row has no single value to report, so each row's
  value is collected, duplicates included.

The last two are what stop the first from being over-applied: reducing a
row-reading projection to one value would silently speak for the whole group
from one row, and deduplicating the collection would drop rows the caller can
count.
"""

[[test.results]]
value = "[{ id: items:1, name: 'apple', type: 'fruit' }]"

[[test.results]]
value = "[{ id: items:2, name: 'banana', type: 'fruit' }]"

[[test.results]]
value = "[{ id: items:3, name: 'apple', type: 'fruit' }]"

[[test.results]]
value = "[{ id: items:4, name: 'carrot', type: 'veg' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ k: 7, type: 'fruit' }, { k: 7, type: 'veg' }]"

[[test.results]]
value = "[{ k: 'x', type: 'fruit' }, { k: 'x', type: 'veg' }]"

[[test.results]]
value = "[{ k: 2, type: 'fruit' }, { k: 2, type: 'veg' }]"

[[test.results]]
value = "[{ k: NONE, type: 'fruit' }, { k: NONE, type: 'veg' }]"

[[test.results]]
value = "[{ k: [1, 2], type: 'fruit' }, { k: [1, 2], type: 'veg' }]"

[[test.results]]
value = "[{ k: { a: 1 }, type: 'fruit' }, { k: { a: 1 }, type: 'veg' }]"

[[test.results]]
value = "[{ k: 7, type: 'fruit' }, { k: 7, type: 'veg' }]"

[[test.results]]
value = "[{ k: 7, type: 'fruit' }, { k: 7, type: 'veg' }]"

[[test.results]]
value = "[{ k: 'X', type: 'fruit' }, { k: 'X', type: 'veg' }]"

[[test.results]]
value = "[{ k: 7, type: 'fruit' }, { k: 7, type: 'veg' }]"

[[test.results]]
value = "[{ k: 7, type: 'fruit' }, { k: 7, type: 'veg' }]"

[[test.results]]
value = "[{ k: items:1, type: 'fruit' }, { k: items:1, type: 'veg' }]"

[[test.results]]
value = "[{ k: 'x', type: 'fruit' }, { k: 'x', type: 'veg' }]"

[[test.results]]
value = "[{ k: 'x', type: 'fruit' }, { k: 'x', type: 'veg' }]"

[[test.results]]
value = "[{ k: 1, type: 'fruit' }, { k: 1, type: 'veg' }]"

[[test.results]]
value = "[{ k: 'x!', type: 'fruit' }, { k: 'x!', type: 'veg' }]"

[[test.results]]
value = "[{ k: NONE, type: 'fruit' }, { k: NONE, type: 'veg' }]"

[[test.results]]
value = "[{ k: 7, n: 3, type: 'fruit' }, { k: 7, n: 1, type: 'veg' }]"

[[test.results]]
value = "[{ k: 7 }]"

[[test.results]]
value = "[{ names: ['apple', 'banana'], type: 'fruit' }, { names: ['carrot'], type: 'veg' }]"

[[test.results]]
value = "[{ n: 2, type: 'fruit' }, { n: 1, type: 'veg' }]"

[[test.results]]
value = "[{ type: 'fruit', uniq: ['apple', 'banana'] }, { type: 'veg', uniq: ['carrot'] }]"

[[test.results]]
value = "[{ n: 3, name: ['apple', 'banana', 'apple'], type: 'fruit' }, { n: 1, name: ['carrot'], type: 'veg' }]"






















*/

CREATE items:1 SET type = 'fruit', name = 'apple';
CREATE items:2 SET type = 'fruit', name = 'banana';
CREATE items:3 SET type = 'fruit', name = 'apple';
CREATE items:4 SET type = 'veg', name = 'carrot';

LET $conf = { label: 'x' };

-- Expressions the row cannot influence.
SELECT 7 AS k, type FROM items GROUP BY type;
SELECT 'x' AS k, type FROM items GROUP BY type;
SELECT (9) / 4 AS k, type FROM items GROUP BY type;
SELECT $unset AS k, type FROM items GROUP BY type;
SELECT [1, 2] AS k, type FROM items GROUP BY type;
SELECT { a: 1 } AS k, type FROM items GROUP BY type;

-- Builtin calls over row-independent arguments reduce the same way, so that
-- adding a call to an expression cannot change the shape of the answer.
SELECT math::abs(-7) AS k, type FROM items GROUP BY type;
SELECT type::int('7') AS k, type FROM items GROUP BY type;
SELECT string::uppercase('x') AS k, type FROM items GROUP BY type;
SELECT string::len('abcdefg') AS k, type FROM items GROUP BY type;
SELECT math::abs(-7) + 0 AS k, type FROM items GROUP BY type;
SELECT items:1 AS k, type FROM items GROUP BY type;

-- An idiom is judged by its root. One rooted in an explicit expression is no
-- more row-dependent than that expression, so `$conf` and `$conf.label` must
-- reduce the same way; the row-rooted control below is what keeps that from
-- being over-applied.
SELECT $conf.label AS k, type FROM items GROUP BY type;
SELECT ({ label: 'x' }).label AS k, type FROM items GROUP BY type;
SELECT [1, 2][0] AS k, type FROM items GROUP BY type;
SELECT $conf.label + '!' AS k, type FROM items GROUP BY type;
SELECT $conf.missing AS k, type FROM items GROUP BY type;

-- Beside a real aggregate, and under GROUP ALL.
SELECT 7 AS k, count() AS n, type FROM items GROUP BY type;
SELECT 7 AS k FROM items GROUP ALL;

-- `array::group` reports the unique items, which `array::len` makes countable.
SELECT type, array::group(name) AS names FROM items GROUP BY type;
SELECT type, array::len(array::group(name)) AS n FROM items GROUP BY type;
SELECT type, array::distinct(array::group(name)) AS uniq FROM items GROUP BY type;

-- A projection that reads the row keeps one element per row.
SELECT name, count() AS n, type FROM items GROUP BY type;
