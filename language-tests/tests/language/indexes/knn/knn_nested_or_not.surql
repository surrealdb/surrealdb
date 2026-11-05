/**
[env]
namespace = true
database = true

[test]
issue = 476
reason = """
Brute-force KNN nested under OR / NOT is a deliberate hard error
(`Error::Query`), with no per-row fallback. Supporting these shapes requires
per-row KNN-checker machinery and is tracked separately from #476.
"""

[[test.results]]
value = "[{ id: pts:1, point: [1f, 2f, 3f, 4f] }]"

[[test.results]]
value = "[{ id: pts:2, point: [4f, 5f, 6f, 7f] }]"

[[test.results]]
value = "[{ id: pts:3, point: [8f, 9f, 10f, 11f] }]"

[[test.results]]
error = "Invalid query: KNN operators must appear at the top level of the WHERE clause (joined with AND); nesting `<|k, …|>` inside OR or NOT is not supported."

[[test.results]]
error = "Invalid query: KNN operators must appear at the top level of the WHERE clause (joined with AND); nesting `<|k, …|>` inside OR or NOT is not supported."
*/

CREATE pts:1 SET point = [1f,2f,3f,4f];
CREATE pts:2 SET point = [4f,5f,6f,7f];
CREATE pts:3 SET point = [8f,9f,10f,11f];
-- K-form nested under OR
SELECT id FROM pts WHERE point <|2,EUCLIDEAN|> [2f,3f,4f,5f] OR id = pts:3;
-- K-form under NOT
SELECT id FROM pts WHERE !(point <|2,EUCLIDEAN|> [2f,3f,4f,5f]);
