/**
[env]
imports = ["reproductions/a_permission_chain_partitions_by_key_range_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
	The enforcement chain -- the table's `PERMISSIONS FOR select`, the
	projection layers carrying its `COMPUTED` fields and field `SELECT`
	guards, and the source -- fixes the record image every operator above it
	sees, and no user expression is evaluated inside it. That forbids a
	vertical cut: moving an operator across the chain changes which rows are
	judged, or judges them against a different image.

	It does not forbid a horizontal one. Running the whole chain once per
	disjoint key range and concatenating the results judges every row exactly
	once, by the same nodes, in the same order, so the answer is the answer
	the single chain gives. These record ids partition exactly: `doc:..3`,
	`doc:3..5` and `doc:5..` cover every row and share none.

	Each pair pins that parity for one shape the chain answers: the whole
	layered row, a predicate over a stored field, a predicate over a computed
	field, a LIMIT, a LIMIT under an ORDER BY, and a grand-total count. The
	paging cases are where the two cuts meet: a LIMIT counts rows the
	permission node has already judged, so it belongs above the union and
	never inside a range or inside the chain, while the count the ranges
	answer in parts is the whole count because it sums.

	Each whole side is pinned literally as well, so parity cannot be
	satisfied by two sides that are both empty or both wrong: the table
	permission drops `doc:3`, the field guards cut `double` and `secret` on
	the rows this caller does not own, and `is_mine` is computed on every row
	that survives.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ double: 2, id: doc:1, is_mine: true, n: 1, owner: user:alice, public: true, secret: 's1' }, { id: doc:2, is_mine: false, n: 2, owner: user:bob, public: true }, { double: 8, id: doc:4, is_mine: true, n: 4, owner: user:alice, public: false, secret: 's4' }, { id: doc:5, is_mine: false, n: 5, owner: user:bob, public: true }, { double: 12, id: doc:6, is_mine: true, n: 6, owner: user:alice, public: true, secret: 's6' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: doc:4, n: 4 }, { id: doc:5, n: 5 }, { id: doc:6, n: 6 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: doc:1, is_mine: true }, { id: doc:4, is_mine: true }, { id: doc:6, is_mine: true }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[doc:1, doc:2, doc:4]"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[doc:6, doc:5, doc:4]"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ count: 5, is_int: true }"

[[test.results]]
value = "true"
*/

-- The whole layered row.
LET $whole = (SELECT * FROM doc ORDER BY id);
RETURN $whole;
LET $parts = array::concat(
	(SELECT * FROM doc:..3 ORDER BY id),
	(SELECT * FROM doc:3..5 ORDER BY id),
	(SELECT * FROM doc:5.. ORDER BY id)
);
RETURN $parts = $whole;

-- A predicate over a stored field, judged above the chain.
LET $whole_stored = (SELECT id, n FROM doc WHERE n > 2 ORDER BY id);
RETURN $whole_stored;
LET $parts_stored = array::concat(
	(SELECT id, n FROM doc:..3 WHERE n > 2 ORDER BY id),
	(SELECT id, n FROM doc:3..5 WHERE n > 2 ORDER BY id),
	(SELECT id, n FROM doc:5.. WHERE n > 2 ORDER BY id)
);
RETURN $parts_stored = $whole_stored;

-- A predicate over a computed field, judged above the layer that sets it.
LET $whole_computed = (SELECT id, is_mine FROM doc WHERE is_mine ORDER BY id);
RETURN $whole_computed;
LET $parts_computed = array::concat(
	(SELECT id, is_mine FROM doc:..3 WHERE is_mine ORDER BY id),
	(SELECT id, is_mine FROM doc:3..5 WHERE is_mine ORDER BY id),
	(SELECT id, is_mine FROM doc:5.. WHERE is_mine ORDER BY id)
);
RETURN $parts_computed = $whole_computed;

-- A LIMIT is a vertical cut: it belongs above the union, not inside a range,
-- and above the permission node, not inside the chain.
LET $whole_page = (SELECT VALUE id FROM doc LIMIT 3);
RETURN $whole_page;
RETURN (SELECT VALUE id FROM $parts LIMIT 3) = $whole_page;

-- An ordering and a LIMIT belong above the union, not inside a range.
LET $whole_top = (SELECT VALUE id FROM doc ORDER BY n DESC LIMIT 3);
RETURN $whole_top;
RETURN (SELECT VALUE id FROM $parts ORDER BY n DESC LIMIT 3) = $whole_top;

-- A grand total the ranges answer in parts that sum.
LET $whole_count = (SELECT VALUE count() FROM doc GROUP ALL)[0];
RETURN { count: $whole_count, is_int: type::is_int($whole_count) };
RETURN math::sum(array::concat(
	(SELECT VALUE count() FROM doc:..3 GROUP ALL),
	(SELECT VALUE count() FROM doc:3..5 GROUP ALL),
	(SELECT VALUE count() FROM doc:5.. GROUP ALL)
)) = $whole_count;
