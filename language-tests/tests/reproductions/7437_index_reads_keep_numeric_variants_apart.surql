/**
[env]
# The legacy compute-only engine still returns one of the two records twice
# through a full-text index, and the ordinary runner replays this body under
# every strategy.
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
Two record ids that differ only in the numeric variant of a nested value are
two records, and every index read must return each of them as itself.

The runner compares numbers across variants, so a result naming `c:[1]` twice
matches one naming `c:[1]` and `c:[1dec]`. Each read here therefore reports the
kind of the number inside the id it returned, which tells the two apart.

Covered: a full-text index, a full-text match fused with a graph predicate
(whose reachable records are deduplicated before their doc-IDs are looked up),
and the distances a KNN scan hands to `vector::distance::knn()`.
"""
issue = 7437

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ int: false }, { int: true }]"

[[test.results]]
value = "[{ int: false }, { int: true }]"

[[test.results]]
value = "[{ d: 0f, int: true }, { d: 1.4142135623730951f, int: false }]"
*/

CREATE c:[1]    SET body = 'hello world', v = [1.0f, 0.0f] RETURN NONE;
CREATE c:[1dec] SET body = 'hello world', v = [0.0f, 1.0f] RETURN NONE;
RELATE c:[1]->wrote->doc:x RETURN NONE;
RELATE c:[1dec]->wrote->doc:x RETURN NONE;

DEFINE ANALYZER simple TOKENIZERS blank,class;
DEFINE INDEX c_ft ON c FIELDS body FULLTEXT ANALYZER simple BM25;

SELECT type::is_int(id[0]) AS int FROM c WHERE body @@ 'hello' ORDER BY int;
SELECT type::is_int(id[0]) AS int FROM c
	WHERE body @@ 'hello' AND ->wrote->doc CONTAINS doc:x ORDER BY int;
SELECT type::is_int(id[0]) AS int, vector::distance::knn() AS d FROM c
	WHERE v <|2,EUCLIDEAN|> [1f, 0f] ORDER BY d;
