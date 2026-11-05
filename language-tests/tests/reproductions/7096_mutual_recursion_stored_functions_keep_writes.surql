/**
[env]
namespace = true
database = true

[test]
reason = "Writes made by a stored function reached through mutual recursion (cascade -> recurse_walk -> cascade) persist, on a SCHEMAFULL table that the caller also reads"
issue = 7096

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ budget: { allocated: 1000 }, id: item:a, title: 'A' }]"

[[test.results]]
value = "[{ id: item:b, title: 'B' }]"

[[test.results]]
value = "[{ id: item:c, title: 'C' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ count: 1, success: true }"

[[test.results]]
value = "[{ budget: { allocated: 1000 }, id: item:a }, { budget: { allocated: 400, currency: 'USD' }, id: item:b }, { budget: { allocated: 200, currency: 'USD' }, id: item:c }]"

[[test.results]]
value = "[{ count: 2 }]"
*/
DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD title ON item TYPE string;
DEFINE FIELD budget ON item TYPE none | object;
DEFINE FIELD budget.allocated ON item TYPE none | number;
DEFINE FIELD budget.currency ON item TYPE none | string;
DEFINE TABLE allocates SCHEMAFULL TYPE RELATION IN item OUT item;
DEFINE FIELD amount ON allocates TYPE number;
DEFINE TABLE audit SCHEMALESS;
DEFINE FUNCTION OVERWRITE fn::alloc_walk($edges: array, $idx: int) {
  IF $idx >= array::len($edges) { RETURN NONE; };
  LET $edge = $edges[$idx];
  UPDATE $edge.child_id SET budget = { allocated: $edge.amount, currency: 'USD' };
  CREATE audit CONTENT { item_id: $edge.child_id, action: 'allocated' };
  RETURN fn::alloc_walk($edges, $idx + 1);
};
DEFINE FUNCTION OVERWRITE fn::recurse_walk($child_ids: array, $idx: int, $depth: int) {
  IF $idx >= array::len($child_ids) { RETURN NONE; };
  LET $_ = fn::cascade($child_ids[$idx], $depth + 1);
  RETURN fn::recurse_walk($child_ids, $idx + 1, $depth);
};
DEFINE FUNCTION OVERWRITE fn::cascade($parent_id: record<item>, $depth: int) -> object {
  IF $depth > 10 { RETURN { success: false, error: 'max_depth' }; };
  LET $par = (SELECT id, budget FROM item WHERE id = $parent_id LIMIT 1)[0];
  IF $par = NONE { RETURN { success: false, error: 'not_found' }; };
  LET $edges = (SELECT in AS child_id, amount FROM allocates WHERE out = $parent_id) ?? [];
  IF array::len($edges) = 0 { RETURN { success: true, count: 0 }; };
  LET $_ = fn::alloc_walk($edges, 0);
  LET $child_ids = array::map($edges, |$e| $e.child_id);
  LET $_ = fn::recurse_walk($child_ids, 0, $depth);
  RETURN { success: true, count: array::len($edges) };
};
CREATE item:a SET title = 'A', budget = { allocated: 1000 };
CREATE item:b SET title = 'B';
CREATE item:c SET title = 'C';
RELATE item:b->allocates->item:a SET amount = 400 RETURN NONE;
RELATE item:c->allocates->item:b SET amount = 200 RETURN NONE;
RETURN fn::cascade(item:a, 0);
SELECT id, budget FROM item ORDER BY id;
SELECT count() FROM audit GROUP ALL;
