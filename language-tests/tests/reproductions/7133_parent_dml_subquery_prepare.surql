/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "$parent must resolve during iterator.prepare for nested UPDATE/DELETE (correlated DML subqueries)"
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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: dm_user:alice, name: 'alice' }]"

[[test.results]]
value = "[{ id: dm_item:x, label: 'x' }]"

[[test.results]]
value = "[{ id: dm_item:y, label: 'y' }]"

[[test.results]]
value = "[{ id: dm_link:e1, in: dm_user:alice, out: dm_item:x, touched: false }]"

[[test.results]]
value = "[{ id: dm_link:e2, in: dm_user:alice, out: dm_item:y, touched: false }]"

[[test.results]]
value = "[{ updated: [{ id: dm_link:e1, in: dm_user:alice, out: dm_item:x, touched: true }, { id: dm_link:e2, in: dm_user:alice, out: dm_item:y, touched: true }] }]"

[[test.results]]
value = "[{ removed: [{ id: dm_link:e2, in: dm_user:alice, out: dm_item:y, touched: true }] }]"

[[test.results]]
value = "[{ label: 'x' }]"
*/

DEFINE TABLE dm_user SCHEMAFULL;
DEFINE FIELD name ON dm_user TYPE string;

DEFINE TABLE dm_item SCHEMAFULL;
DEFINE FIELD label ON dm_item TYPE string;

DEFINE TABLE dm_link SCHEMAFULL TYPE RELATION IN dm_user OUT dm_item;
DEFINE FIELD touched ON dm_link TYPE option<bool> DEFAULT false;

CREATE dm_user:alice SET name = 'alice';
CREATE dm_item:x SET label = 'x';
CREATE dm_item:y SET label = 'y';
RELATE dm_user:alice->dm_link:e1->dm_item:x CONTENT {
    touched: false
};
RELATE dm_user:alice->dm_link:e2->dm_item:y CONTENT {
    touched: false
};

-- Nested UPDATE: $parent must resolve during iterator.prepare
SELECT (
    UPDATE $parent->dm_link SET touched = true RETURN AFTER
) AS updated
FROM dm_user:alice;

-- Nested DELETE: `$parent->dm_link` must resolve during prepare
SELECT (
    DELETE $parent->dm_link WHERE out = dm_item:y RETURN BEFORE
) AS removed
FROM dm_user:alice;

SELECT out.label AS label FROM dm_link WHERE in = dm_user:alice ORDER BY label;
