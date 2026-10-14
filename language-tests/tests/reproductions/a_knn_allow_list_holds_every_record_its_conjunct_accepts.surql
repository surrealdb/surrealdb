/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A KNN search narrowed to an index's allow-list returns the nearest records
	that satisfy the statement's conditions, as the brute-force search does.
	An index stores an array once per element and an empty one under a single
	key, so a range over a column that may hold an array does not list every
	record the range accepts: `[] > 'blue'` and `[1, 2] > 'blue'` hold, yet
	neither record is listed under a key above 'blue'. Such a conjunct is
	judged on the candidates rather than narrowing them. An equality to a
	value stored under its own key lists every record it accepts, arrays
	included, and narrows the search; a number the same as a float and as a
	decimal matches whatever its kind. A number that is not, such as `0.1`, is
	keyed by each kind's exact value yet equals the other kind's spelling, and
	a regex equals every string it matches, each stored under its own key, so
	both are judged on the candidates too. No record stores a regex, so a string
	equality never misses a record whose value equals the string as a regex.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[k:1, k:2, k:3]"

[[test.results]]
value = "[k:1, k:2, k:3]"

[[test.results]]
value = "[k:3]"

[[test.results]]
value = "[k:3]"

[[test.results]]
value = "[k:3]"

[[test.results]]
value = "[k:3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[k:6]"

[[test.results]]
value = "[k:6]"

[[test.results]]
value = "[k:7]"

[[test.results]]
value = "[k:7]"

[[test.results]]
value = "[k:1]"

[[test.results]]
value = "[k:1]"

[[test.results]]
value = "[k:3, k:8]"

[[test.results]]
value = "[k:3, k:8]"

[[test.results]]
value = "[k:3, k:8]"

[[test.results]]
value = "[k:3, k:8]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[k:3, k:8]"

[[test.results]]
value = "[k:3, k:8]"

[[test.results]]
value = "[k:9, k:10]"

[[test.results]]
value = "[k:9, k:10]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[k:9, k:10, k:11]"

[[test.results]]
value = "[k:9, k:10, k:11]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[kc:1, kc:2, kc:3]"

[[test.results]]
value = "[kc:1, kc:2, kc:3]"

[[test.results]]
value = "[kc:4]"

[[test.results]]
value = "[kc:4]"

[[test.results]]
match = '''string::contains(<string> $result, "BitmapIndexScan [ctx: Db] [index: idx_ce, access: ['zz']]") AND string::contains(<string> $result, "predicate: c = 'zz' AND extra = 1")'''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[k:12, k:13]"

[[test.results]]
value = "[k:12, k:13]"

[[test.results]]
value = "[k:12, k:13]"

[[test.results]]
value = "[k:12, k:13]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[k:14, k:15]"

[[test.results]]
value = "[k:14, k:15]"

[[test.results]]
value = "[k:14, k:15]"

[[test.results]]
value = "[k:14, k:15]"

[[test.results]]
error = "Tried to serialize a value which cannot be serialized."
*/

DEFINE INDEX idx_c ON k FIELDS c;
DEFINE INDEX idx_vec ON k FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN;
CREATE k:1 SET c = [], vec = [0.0, 0.0] RETURN NONE;
CREATE k:2 SET c = [1, 2], vec = [1.0, 1.0] RETURN NONE;
CREATE k:3 SET c = 'zz', vec = [2.0, 2.0] RETURN NONE;
CREATE k:4 SET c = 'aa', vec = [3.0, 3.0] RETURN NONE;
CREATE k:5 SET c = [1, 'red'], vec = [4.0, 4.0] RETURN NONE;

SELECT VALUE id FROM k WHERE c > 'blue' AND vec <|3,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c > 'blue' AND vec <|3,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WHERE c >= 'b' AND c < 'zzz' AND vec <|3,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c >= 'b' AND c < 'zzz' AND vec <|3,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WHERE c = 'zz' AND vec <|3,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c = 'zz' AND vec <|3,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
CREATE k:6 SET vec = [0.5, 0.5] RETURN NONE;
CREATE k:7 SET c = NULL, vec = [0.6, 0.6] RETURN NONE;
CREATE k:8 SET c = 'azz', vec = [0.7, 0.7] RETURN NONE;
SELECT VALUE id FROM k WHERE c = NONE AND vec <|2,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c = NONE AND vec <|2,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WHERE c = NULL AND vec <|2,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c = NULL AND vec <|2,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WHERE c = [] AND vec <|2,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c = [] AND vec <|2,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WHERE c CONTAINS 'z' AND vec <|2,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c CONTAINS 'z' AND vec <|2,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WHERE (c = 'zz' OR c = 'azz') AND vec <|2,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE (c = 'zz' OR c = 'azz') AND vec <|2,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
CREATE k:9 SET c = 3f, vec = [0.8, 0.8] RETURN NONE;
CREATE k:10 SET c = 3dec, vec = [0.9, 0.9] RETURN NONE;
SELECT VALUE id FROM k WHERE c = /zz/ AND vec <|2,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c = /zz/ AND vec <|2,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WHERE c = 3 AND vec <|2,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c = 3 AND vec <|2,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
-- `==` narrows the search as `=` does: every number of the sought value is
-- stored under one key.
CREATE k:11 SET c = 3, vec = [0.95, 0.95] RETURN NONE;
SELECT VALUE id FROM k WHERE c == 3 AND vec <|3,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c == 3 AND vec <|3,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
-- A compound index narrows the search on the prefix it pins, and a later
-- column — an array, an empty array, NULL — is judged on the candidates.
DEFINE INDEX idx_ce ON kc FIELDS c, extra;
DEFINE INDEX idx_kv ON kc FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN;
CREATE kc:1 SET c = 'zz', extra = [1, 2], vec = [0.1, 0.1] RETURN NONE;
CREATE kc:2 SET c = 'zz', extra = [], vec = [0.2, 0.2] RETURN NONE;
CREATE kc:3 SET c = 'zz', extra = NULL, vec = [0.3, 0.3] RETURN NONE;
CREATE kc:4 SET c = 'zz', extra = 1, vec = [0.4, 0.4] RETURN NONE;
CREATE kc:5 SET c = 'aa', extra = 1, vec = [0.0, 0.0] RETURN NONE;
SELECT VALUE id FROM kc WHERE c = 'zz' AND vec <|3,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM kc WITH NOINDEX WHERE c = 'zz' AND vec <|3,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM kc WHERE c = 'zz' AND extra = 1 AND vec <|2,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM kc WITH NOINDEX WHERE c = 'zz' AND extra = 1 AND vec <|2,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
EXPLAIN SELECT VALUE id FROM kc WHERE c = 'zz' AND extra = 1 AND vec <|2,40|> [0.0, 0.0];
-- A number whose float and decimal spellings are different numbers is keyed
-- by each kind's exact value, yet a float equals the decimal it prints as.
CREATE k:12 SET c = 0.1f, vec = [0.97, 0.97] RETURN NONE;
CREATE k:13 SET c = 0.1dec, vec = [0.98, 0.98] RETURN NONE;
SELECT VALUE id FROM k WHERE c = 0.1dec AND vec <|3,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c = 0.1dec AND vec <|3,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WHERE c = 0.1f AND vec <|3,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c = 0.1f AND vec <|3,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
-- A record id whose key holds such a number is judged on the candidates, not
-- by the allow-list, so it loses neither spelling.
CREATE k:14 SET c = foo:[0.1f], vec = [0.99, 0.99] RETURN NONE;
CREATE k:15 SET c = foo:[0.1dec], vec = [1.01, 1.01] RETURN NONE;
SELECT VALUE id FROM k WHERE c = foo:[0.1dec] AND vec <|3,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c = foo:[0.1dec] AND vec <|3,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WHERE c = foo:[0.1f] AND vec <|3,40|> [0.0, 0.0] ORDER BY id;
SELECT VALUE id FROM k WITH NOINDEX WHERE c = foo:[0.1f] AND vec <|3,EUCLIDEAN|> [0.0, 0.0] ORDER BY id;
-- A regex is no storable value, so a string key never stands for a record
-- holding a regex that matches it.
CREATE k:16 SET c = /zz/, vec = [1.02, 1.02] RETURN NONE;
