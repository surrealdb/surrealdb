/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "UPDATE and DELETE with IN on the leading field of a compound UNIQUE index must affect the rows SELECT returns"
issue = 7593

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ group: 'g1', id: item:a, position: 0 }]"

[[test.results]]
value = "[{ group: 'g1', id: item:b, position: 1 }]"

[[test.results]]
value = "[{ group: 'g2', id: item:c, position: 0 }]"

[[test.results]]
value = "[item:a, item:b, item:c]"

[[test.results]]
value = "[item:a, item:b, item:c]"

[[test.results]]
value = "[{ group: 'g2', id: item:c, position: 0, touched: true }]"

[[test.results]]
value = "[item:a, item:b]"

[[test.results]]
value = "[item:a, item:b]"
*/

DEFINE TABLE item SCHEMALESS;
DEFINE INDEX item_group_position ON item FIELDS group, position UNIQUE;
CREATE item:a SET group = 'g1', position = 0;
CREATE item:b SET group = 'g1', position = 1;
CREATE item:c SET group = 'g2', position = 0;
SELECT VALUE id FROM item WHERE group IN ['g1', 'g2'];
UPDATE item SET touched = true WHERE group IN ['g1', 'g2'] RETURN VALUE id;
DELETE item WHERE group IN ['g2'] RETURN BEFORE;
UPDATE item SET touched = true WHERE group INSIDE ['g1'] RETURN VALUE id;
SELECT VALUE id FROM item;
