/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Nested CREATE subquery can read $parent for field values (correlated with outer row)"
issue = 7133

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: dm3_user:root, name: 'root' }]"

[[test.results]]
value = "[{ note: [{ id: dm3_note:n1, uref: dm3_user:root }] }]"
*/

DEFINE TABLE dm3_user SCHEMAFULL;
DEFINE FIELD name ON dm3_user TYPE string;
DEFINE TABLE dm3_note SCHEMAFULL;
DEFINE FIELD uref ON dm3_note TYPE record<dm3_user>;

CREATE dm3_user:root SET name = 'root';

SELECT (
    CREATE dm3_note:n1 SET uref = $parent.id RETURN AFTER
) AS note
FROM dm3_user:root;
