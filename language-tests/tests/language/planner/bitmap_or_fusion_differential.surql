/**
[test]
reason = "Differential check: top-level OR bitmap union plans (all-ro) return identical results to the legacy engine (compute-only). Results are normalized with array::sort because the fused shapes forbid ORDER BY (an ORDER keeps the streaming union plan), and a bitmap resolve emits in doc-ID order while streaming plans emit branch-merge order."
issue = 550

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
value = "[0, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18, 20, 21, 23, 24, 26, 27, 29, 30, 32, 33, 35, 36, 38, 39, 41, 42, 44, 45, 47, 48, 50, 51, 53, 54, 56, 57, 59]"

[[test.results]]
value = "[0, 2, 3, 4, 6, 8, 9, 10, 12, 14, 15, 16, 18, 20, 21, 22, 24, 26, 27, 28, 30, 32, 33, 34, 36, 38, 39, 40, 42, 44, 45, 46, 48, 50, 51, 52, 54, 56, 57, 58]"

[[test.results]]
value = "[1, 4, 7, 10, 13, 16, 19, 22, 25, 28, 31, 34, 37, 40, 43, 46, 49, 52, 55, 56, 57, 58, 59]"

[[test.results]]
value = "[0, 1, 2, 3, 4, 5, 6, 7, 9, 11, 12, 13, 15, 17, 18, 19, 21, 23, 24, 25, 27, 29, 30, 31, 33, 35, 36, 37, 39, 41, 42, 43, 45, 47, 48, 49, 51, 53, 54, 55, 57, 59]"

[[test.results]]
value = "[]"
*/

DEFINE FIELD category ON items TYPE string;
DEFINE FIELD flag ON items TYPE bool;
DEFINE FIELD score ON items TYPE int;
DEFINE INDEX idx_category ON items FIELDS category;
DEFINE INDEX idx_flag ON items FIELDS flag;
DEFINE INDEX idx_score ON items FIELDS score;

-- 60 records over 3 categories, alternating flag, score 0..59.
FOR $i IN 0..60 {
    CREATE type::record('items', $i) SET
        category = ['red', 'green', 'blue'][$i % 3],
        flag = $i % 2 == 0,
        score = $i,
        pos = $i;
};

-- OR of two selective equality predicates.
RETURN array::sort((SELECT VALUE pos FROM items WHERE category = 'red' OR category = 'blue'));

-- Overlap-heavy branches: every even red row satisfies both sides.
RETURN array::sort((SELECT VALUE pos FROM items WHERE category = 'red' OR flag = true));

-- An equality branch unions with a range branch.
RETURN array::sort((SELECT VALUE pos FROM items WHERE category = 'green' OR score >= 55));

-- Three branches.
RETURN array::sort((SELECT VALUE pos FROM items WHERE category = 'red' OR flag = false OR score < 5));

-- Both branches empty.
RETURN array::sort((SELECT VALUE pos FROM items WHERE category = 'nope' OR score > 100));
