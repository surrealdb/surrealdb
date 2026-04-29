/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Batch ceiling must correctly return LIMIT rows when a residual filter rejects some index entries"

[[test.results]]
value = "'OK'"

# DESC LIMIT 5 with priority > 3: items 19(10),18(9),17(8),16(7),15(6)
[[test.results]]
value = "[{ id: item:19, modified: d'2025-01-19T00:00:00Z', priority: 10 }, { id: item:18, modified: d'2025-01-18T00:00:00Z', priority: 9 }, { id: item:17, modified: d'2025-01-17T00:00:00Z', priority: 8 }, { id: item:16, modified: d'2025-01-16T00:00:00Z', priority: 7 }, { id: item:15, modified: d'2025-01-15T00:00:00Z', priority: 6 }]"

# ASC LIMIT 5 with priority > 3: items 3(4),4(5),5(6),6(7),7(8)
[[test.results]]
value = "[{ id: item:3, modified: d'2025-01-03T00:00:00Z', priority: 4 }, { id: item:4, modified: d'2025-01-04T00:00:00Z', priority: 5 }, { id: item:5, modified: d'2025-01-05T00:00:00Z', priority: 6 }, { id: item:6, modified: d'2025-01-06T00:00:00Z', priority: 7 }, { id: item:7, modified: d'2025-01-07T00:00:00Z', priority: 8 }]"

# DESC LIMIT 3 with priority=10: only 2 items (19,9) have priority=10
[[test.results]]
value = "[{ id: item:19, modified: d'2025-01-19T00:00:00Z', priority: 10 }, { id: item:9, modified: d'2025-01-09T00:00:00Z', priority: 10 }]"

# Pagination: DESC LIMIT 5 START 5
[[test.results]]
value = "[{ id: item:15, modified: d'2025-01-15T00:00:00Z', priority: 6 }, { id: item:14, modified: d'2025-01-14T00:00:00Z', priority: 5 }, { id: item:13, modified: d'2025-01-13T00:00:00Z', priority: 4 }, { id: item:12, modified: d'2025-01-12T00:00:00Z', priority: 3 }, { id: item:11, modified: d'2025-01-11T00:00:00Z', priority: 2 }]"
*/

-- Setup: compound index on (visible, modified).
-- priority is NOT in the index, creating a residual filter that the
-- batch ceiling mechanism must handle correctly.
{
    DEFINE TABLE item SCHEMAFULL;
    DEFINE FIELD visible ON item TYPE bool;
    DEFINE FIELD priority ON item TYPE int;
    DEFINE FIELD modified ON item TYPE datetime;
    DEFINE INDEX idx_vis_mod ON item FIELDS visible, modified;

    -- Create 20 visible items with varying priority
    FOR $i IN 1..=20 {
        CREATE type::record('item', $i) SET
            visible = true,
            priority = ($i % 10) + 1,
            modified = d'2025-01-01T00:00:00Z' + duration::from_days($i - 1);
    };
    -- Create 5 non-visible items (filtered by index prefix)
    FOR $i IN 21..=25 {
        CREATE type::record('item', $i) SET
            visible = false,
            priority = 10,
            modified = d'2025-01-15T00:00:00Z' + duration::from_days($i - 21);
    };
    RETURN "OK";
};

-- Test 1: residual filter (priority > 3) rejects some rows from index scan
-- Batch ceiling must fetch enough extra rows to compensate
SELECT id, priority, modified FROM item
    WHERE visible = true AND priority > 3
    ORDER BY modified DESC LIMIT 5;

-- Test 2: same filter, ASC order
SELECT id, priority, modified FROM item
    WHERE visible = true AND priority > 3
    ORDER BY modified ASC LIMIT 5;

-- Test 3: high selectivity filter — only priority=10 items pass
SELECT id, priority, modified FROM item
    WHERE visible = true AND priority = 10
    ORDER BY modified DESC LIMIT 3;

-- Test 4: pagination with START — batch ceiling must handle offset correctly
SELECT id, priority, modified FROM item
    WHERE visible = true
    ORDER BY modified DESC LIMIT 5 START 5;
