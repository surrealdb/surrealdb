/**
[test]
reason = "Differential check: graph semi-join bitmap branches (all-ro) return identical results to the legacy engine (compute-only). Results are normalized with array::sort because the fused plan resolves in doc-ID order."
issue = 549

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 48, 51, 54, 57]"

[[test.results]]
value = "[4, 16, 28, 40, 52]"

[[test.results]]
value = "[0, 12, 24, 36, 48]"

[[test.results]]
value = "[48]"
*/

DEFINE FIELD category ON items TYPE string;
DEFINE INDEX idx_category ON items FIELDS category;
DEFINE TABLE linked TYPE RELATION;

-- 60 items over 3 categories; every third item links to hub:1, every
-- fourth to hub:2 (overlapping memberships).
CREATE hub:1 RETURN NONE;
CREATE hub:2 RETURN NONE;
FOR $i IN 0..60 {
    LET $it = (CREATE type::record('items', $i) SET category = ['red', 'green', 'blue'][$i % 3], pos = $i);
    IF $i % 3 == 0 { RELATE ($it[0].id)->linked->hub:1 RETURN NONE; };
    IF $i % 4 == 0 { RELATE ($it[0].id)->linked->hub:2 RETURN NONE; };
};

-- Indexed equality AND reachability.
RETURN array::sort((SELECT VALUE pos FROM items WHERE category = 'red' AND ->linked->hub CONTAINS hub:1));

-- The flipped spelling.
RETURN array::sort((SELECT VALUE pos FROM items WHERE category = 'green' AND hub:2 INSIDE ->linked->hub));

-- Two graph conjuncts beside one index branch.
RETURN array::sort((SELECT VALUE pos FROM items WHERE category = 'red' AND ->linked->hub CONTAINS hub:1 AND ->linked->hub CONTAINS hub:2));

-- An anchor no row reaches.
RETURN array::sort((SELECT VALUE pos FROM items WHERE category = 'red' AND ->linked->hub CONTAINS hub:2 AND pos > 40));
