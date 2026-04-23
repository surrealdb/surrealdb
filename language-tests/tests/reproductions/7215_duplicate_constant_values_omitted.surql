/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro"]

[test]
reason = "Duplicate constant values in SELECT projections must not be omitted"
issue = 7215

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: comment:1, isLiked: false, isLocked: false, isNew: false }]"

[[test.results]]
value = "{ id: comment:1, isLiked: false, isLocked: false, isNew: false }"
*/

DEFINE TABLE OVERWRITE comment SCHEMALESS TYPE NORMAL;

UPSERT comment:1 RETURN NONE;

SELECT
    *,
    false AS isLiked,
    false AS isLocked,
    false AS isNew
FROM comment;

-- ONLY variant (reported to work correctly)
SELECT
    *,
    false AS isLiked,
    false AS isLocked,
    false AS isNew
FROM ONLY comment:1;
