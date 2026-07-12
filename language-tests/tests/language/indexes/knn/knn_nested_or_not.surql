/**
[env]
namespace = true
database = true
planner-strategy = ["compute-only"]

[test]
issue = 476
reason = """
Legacy behaviour of brute-force KNN nested under OR / NOT, pinned for the
executor cutover. Under OR the legacy tree still registers the KNN checker,
so the top-K rows pass via the KNN side and further rows via the other
branch. Under NOT the tree does not descend, no checker is registered, the
KNN evaluates to false per row and the negation lets every row through.
The streaming planner instead rejects both shapes with a hard error (see
knn_nested_or_not_new_executor.surql); supporting them requires per-row
KNN-checker machinery and is tracked separately from #476.
"""

[[test.results]]
value = "[{ id: pts:1, point: [1f, 2f, 3f, 4f] }]"

[[test.results]]
value = "[{ id: pts:2, point: [4f, 5f, 6f, 7f] }]"

[[test.results]]
value = "[{ id: pts:3, point: [8f, 9f, 10f, 11f] }]"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }, { id: pts:3 }]"

[[test.results]]
value = "[{ id: pts:1 }, { id: pts:2 }, { id: pts:3 }]"
*/

CREATE pts:1 SET point = [1f,2f,3f,4f];
CREATE pts:2 SET point = [4f,5f,6f,7f];
CREATE pts:3 SET point = [8f,9f,10f,11f];
-- K-form nested under OR: top-2 via KNN, pts:3 via the other branch
SELECT id FROM pts WHERE point <|2,EUCLIDEAN|> [2f,3f,4f,5f] OR id = pts:3;
-- K-form under NOT: checker never registered, negation passes all rows
SELECT id FROM pts WHERE !(point <|2,EUCLIDEAN|> [2f,3f,4f,5f]);
