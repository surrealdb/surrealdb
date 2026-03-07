/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Compound index with equality prefix should support ORDER BY on the next column with LIMIT pushdown"

[[test.results]]
value = "None"

[[test.results]]
value = "None"

[[test.results]]
value = "None"

[[test.results]]
value = "None"

[[test.results]]
value = "[{ id: item:1, modified: d'2025-01-01T00:00:00Z', visible: true }]"

[[test.results]]
value = "[{ id: item:3, modified: d'2025-01-03T00:00:00Z', visible: true }]"

[[test.results]]
value = "[{ id: item:4, modified: d'2025-01-04T00:00:00Z', visible: true }]"

[[test.results]]
value = "[{ id: item:5, modified: d'2025-01-05T00:00:00Z', visible: true }]"

[[test.results]]
value = "[{ id: item:2, modified: d'2025-01-02T00:00:00Z', visible: false }]"

[[test.results]]
value = "[{ id: item:6, modified: d'2025-01-06T00:00:00Z', visible: false }]"

[[test.results]]
value = "[{ id: item:5, modified: d'2025-01-05T00:00:00Z', visible: true }, { id: item:4, modified: d'2025-01-04T00:00:00Z', visible: true }, { id: item:3, modified: d'2025-01-03T00:00:00Z', visible: true }]"

[[test.results]]
value = "[{ id: item:1, modified: d'2025-01-01T00:00:00Z', visible: true }, { id: item:3, modified: d'2025-01-03T00:00:00Z', visible: true }, { id: item:4, modified: d'2025-01-04T00:00:00Z', visible: true }]"

[[test.results]]
value = "[{ id: item:5, modified: d'2025-01-05T00:00:00Z', visible: true }]"

[[test.results]]
value = "[{ id: item:1, modified: d'2025-01-01T00:00:00Z', visible: true }]"
*/

-- Define table and compound index
DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD visible ON item TYPE bool;
DEFINE FIELD modified ON item TYPE datetime;
DEFINE INDEX idx_compound ON item FIELDS visible, modified;

-- Insert test data (visible items)
CREATE item:1 SET visible = true, modified = d'2025-01-01T00:00:00Z';
CREATE item:3 SET visible = true, modified = d'2025-01-03T00:00:00Z';
CREATE item:4 SET visible = true, modified = d'2025-01-04T00:00:00Z';
CREATE item:5 SET visible = true, modified = d'2025-01-05T00:00:00Z';

-- Insert non-visible items that should be filtered
CREATE item:2 SET visible = false, modified = d'2025-01-02T00:00:00Z';
CREATE item:6 SET visible = false, modified = d'2025-01-06T00:00:00Z';

-- ORDER BY modified DESC LIMIT 3: should return items 5, 4, 3
SELECT * FROM item WHERE visible = true ORDER BY modified DESC LIMIT 3;

-- ORDER BY modified ASC LIMIT 3: should return items 1, 3, 4
SELECT * FROM item WHERE visible = true ORDER BY modified ASC LIMIT 3;

-- ORDER BY modified DESC LIMIT 1: should return item 5
SELECT * FROM item WHERE visible = true ORDER BY modified DESC LIMIT 1;

-- ORDER BY modified ASC LIMIT 1: should return item 1
SELECT * FROM item WHERE visible = true ORDER BY modified ASC LIMIT 1;
