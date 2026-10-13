/**
[env]
namespace = true
database = true

[test]
issue = 476
reason = """
Brute-force KNN (`<|k, DIST|>`) with a non-literal query vector, identical
across the legacy and streaming executors (this file deliberately runs under
every planner strategy as a differential proof). Right-hand sides the legacy
tree computes at plan time — bind parameters, function calls (including
closure arguments), arrays with computed elements — now evaluate once at
stream open in the streaming KnnTopK operator, with the same
`array<number>` coercion (and error messages) as the legacy planner.
Right-hand sides the legacy tree treats as unsupported (subqueries, idioms)
register no executor entry and evaluate to false per row, so such queries
return no rows instead of erroring; the same per-row false applies to KNN in
projection position, bare expressions, and an Approximate form without an
HNSW index. Dimension mismatches skip records (no error). Operands of a
KNN evaluated as a predicate are deliberately not computed — the legacy
path hands operate::knn the whole expression without computing either
side, so an erroring operand yields false rather than an error. A
multi-source FROM restores the stripped conjunct so rows from sources
without a KNN entry are filtered out, as legacy does per table. Before
#476 the streaming planner rejected every non-literal vector with
PlannerUnimplemented and the physical fallback evaluated KNN to true.
"""

[[test.results]]
value = "[{ id: pts:1, point: [1f, 2f, 3f, 4f] }]"

[[test.results]]
value = "[{ id: pts:2, point: [4f, 5f, 6f, 7f] }]"

[[test.results]]
value = "[{ id: pts:3, point: [8f, 9f, 10f, 11f] }]"

[[test.results]]
value = "[{ id: q:1, vec: [2f, 3f, 4f, 5f] }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Expected `array<number>` but found `NONE`"

[[test.results]]
error = "Expected `number` but found `'a'` when coercing element at index 0 of `array<number>`"

[[test.results]]
error = "Expected `array<number>` but found `42`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: pts:1, near: false }, { id: pts:2, near: false }, { id: pts:3, near: false }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: res:1, out: [{ id: pts:1 }, { id: pts:2 }] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }]"

[[test.results]]
value = "false"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: pts:1, near: false }, { id: pts:2, near: false }, { id: pts:3, near: false }]"

[[test.results]]
value = "[{ id: pts:1, near: false }, { id: pts:2, near: false }, { id: pts:3, near: false }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: other:1, point: [1f, 2f, 3f, 4f] }]"

[[test.results]]
value = "[{ id: other:2, point: [2f, 3f, 4f, 5f] }]"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }]"
*/

CREATE pts:1 SET point = [1f,2f,3f,4f];
CREATE pts:2 SET point = [4f,5f,6f,7f];
CREATE pts:3 SET point = [8f,9f,10f,11f];
CREATE q:1 SET vec = [2f,3f,4f,5f];
-- 1. subquery RHS: unsupported by the legacy tree -> false per row
SELECT id FROM pts WHERE point <|2,EUCLIDEAN|> (SELECT VALUE vec FROM ONLY q:1);
-- 2. function-call RHS with a closure argument: computed once, works
SELECT id FROM pts WHERE point <|2,EUCLIDEAN|> array::map([2,3,4,5], |$x| <float> $x);
-- 3. per-row field idiom RHS: unsupported -> false per row
SELECT id FROM pts WHERE point <|1,EUCLIDEAN|> point;
-- 4. undefined param RHS: coercion error
SELECT id FROM pts WHERE point <|2,EUCLIDEAN|> $undefined;
-- 5. RHS with a non-numeric element: coercion error
SELECT id FROM pts WHERE point <|2,EUCLIDEAN|> ['a','b','c','d'];
-- 6. scalar RHS: coercion error
SELECT id FROM pts WHERE point <|2,EUCLIDEAN|> 42;
-- 7. residual cond alongside an unsupported RHS: still false per row
SELECT id FROM pts WHERE point <|3,EUCLIDEAN|> (SELECT VALUE vec FROM ONLY q:1) AND id != pts:2;
-- 8. projection-position KNN: no executor entry -> false
SELECT id, point <|2,EUCLIDEAN|> [2f,3f,4f,5f] AS near FROM pts;
-- 9. dimension mismatch: records skipped, no error
SELECT id FROM pts WHERE point <|2,EUCLIDEAN|> [1f,2f];
-- 10. vector::distance::knn() with an unsupported RHS
SELECT id, vector::distance::knn() AS d FROM pts WHERE point <|2,EUCLIDEAN|> (SELECT VALUE vec FROM ONLY q:1);
-- 11. FOREACH-bound param vector
FOR $v IN [[2f,3f,4f,5f]] { CREATE res:1 SET out = (SELECT id FROM pts WHERE point <|2,EUCLIDEAN|> $v); };
SELECT * FROM res:1;
-- 12. user-defined function argument vector
DEFINE FUNCTION fn::knn($v: array<float>) { RETURN SELECT id FROM pts WHERE point <|2,EUCLIDEAN|> $v; };
RETURN fn::knn([2f,3f,4f,5f]);
-- 13. LET param holding a computed vector
LET $q2 = array::map([2,3,4,5], |$x| <float> $x);
SELECT id FROM pts WHERE point <|2,EUCLIDEAN|> $q2;
-- 14. bare expression KNN: no document -> false
RETURN [1f,2f] <|1,EUCLIDEAN|> [1f,2f];
-- 15. Approximate form without an HNSW index: false per row
SELECT id FROM pts WHERE point <|2,100|> [2f,3f,4f,5f];
-- 16. erroring right-hand operand, projection position: KNN predicates never
--     evaluate their operands (legacy hands operate::knn the expression
--     without computing either side), so no error surfaces
SELECT id, point <|2,EUCLIDEAN|> type::int('x') AS near FROM pts;
SELECT id, point <|2,EUCLIDEAN|> (1 / 0) AS near FROM pts;
-- 17. multi-source FROM, Approximate with an HNSW index on pts only:
--     pts rows come from the index top-k, other rows are filtered out
--     (no KNN entry for that table -> false), matching legacy per-table
--     executor behaviour
DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 4 DIST EUCLIDEAN TYPE F32 EFC 500 M 12;
CREATE other:1 SET point = [1f,2f,3f,4f];
CREATE other:2 SET point = [2f,3f,4f,5f];
SELECT id FROM pts, other WHERE point <|2,40|> [2f,3f,4f,5f];
