/**
[env]
namespace = true
database = true

[test]
reason = """
A mutation answers the same whether it is a statement of its own or nested in
another statement. A `SELECT` holding a nested `CREATE` reaches the planner as
the write it holds rather than as a read, and the write inside it is planned
like any other — which is the case that would regress silently.
"""

[[test.results]]
value = "[{ id: person:tobie, name: 'Tobie' }]"

[[test.results]]
value = "[{ id: person:jaime, n: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ id: person:tobie, made: [{ id: log:1 }] }]"

[[test.results]]
value = "[{ id: log:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: audit:1 }]"
*/

-- A creating statement against a specific record id.
CREATE person:tobie SET name = 'Tobie';

-- A creating statement whose data clause evaluates an expression.
CREATE person:jaime SET n = 1 + 1;

-- Both rows are visible to an ordinary read.
SELECT count() FROM person GROUP ALL;

-- A mutation in scalar position reaches the planner as the write it is.
SELECT id, (CREATE log:1) AS made FROM person:tobie;

-- The nested mutation wrote what it reported. One target, one row, so the
-- statement is deterministic without relying on `$parent`.
SELECT * FROM log;

-- A mutation whose output is suppressed still writes.
CREATE audit:1 RETURN NONE;
SELECT * FROM audit;
