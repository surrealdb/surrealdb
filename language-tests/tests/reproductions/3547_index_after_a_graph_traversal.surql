/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Indexing the result of a graph traversal: parentheses or a graph step take the first of the traversal's results, in a projection with or without `*` and in a RETURN, while `.field[0]` continues the chain and indexes each result's field"
issue = 3547

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ first: product:1, id: person:1 }, { first: product:2, id: person:2 }]"

[[test.results]]
value = "[product:1, product:2]"

[[test.results]]
value = "[{ first: product:1 }, { first: product:2 }]"

[[test.results]]
value = "[{ first: product:1 }, { first: product:2 }]"

[[test.results]]
value = '[{ "->order": { out: product:1 } }, { "->order": { out: product:2 } }]'

[[test.results]]
value = "[{ first: product:1, id: person:1 }, { first: product:2, id: person:2 }]"

[[test.results]]
value = '[{ "->order": { out: product:1 }, id: person:1 }, { "->order": { out: product:2 }, id: person:2 }]'

[[test.results]]
value = "[{ first: [product:1] }, { first: [product:2] }]"

[[test.results]]
value = "[product:2, product:2]"

[[test.results]]
value = "product:1"

[[test.results]]
value = "[product:1, product:2]"

[[test.results]]
value = "[product:1, product:2]"

[[test.results]]
value = "[[NONE, NONE], [NONE]]"

[[test.results]]
value = "[NONE, NONE]"

[[test.results]]
value = "[1, 3]"

*/

CREATE person:1, person:2, product:1, product:2 RETURN NONE;
RELATE person:1->order:1->product:1 RETURN NONE;
RELATE person:1->order:2->product:2 RETURN NONE;
RELATE person:2->order:3->product:2 RETURN NONE;

-- The forms the issue reports as working.
SELECT id, array::first(->order.out) AS first FROM person ORDER BY id;
SELECT VALUE (->order.out)[0] FROM person ORDER BY id;
SELECT test[0] AS first FROM (SELECT id, ->order.out AS test FROM person ORDER BY id);

-- Parentheses end the chain, so `[0]` takes the first order's product, aliased or not,
-- alone or beside `*`.
SELECT (->order.out)[0] AS first FROM person ORDER BY id;
SELECT (->order.out)[0] FROM person ORDER BY id;
SELECT *, (->order.out)[0] AS first FROM person ORDER BY id;
SELECT *, (->order.out)[0] FROM person ORDER BY id;
SELECT (->order.out)[0..1] AS first FROM person ORDER BY id;
SELECT VALUE (->order.out)[$] FROM person ORDER BY id;
RETURN (person:1->order.out)[0];

-- A graph step's results are indexed directly.
SELECT VALUE ->order->product[0] FROM person ORDER BY id;
SELECT VALUE ->order[0].out FROM person ORDER BY id;

-- `.out[0]` continues the chain: it indexes each order's `out`, a record id, as
-- `[{ a: [1, 2] }, { a: [3, 4] }].a[0]` indexes each `a`.
SELECT VALUE ->order.out[0] FROM person ORDER BY id;
RETURN person:1->order.out[0];
RETURN [{ a: [1, 2] }, { a: [3, 4] }].a[0];
