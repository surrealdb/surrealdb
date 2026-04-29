/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "WHERE field IN [...] + ORDER BY indexed_field + LIMIT must return correct results using full-range scan with batch ceiling, not IN expansion"

[[test.results]]
value = "'OK'"

# WHERE IN DESC LIMIT 5: latest 5 active/pending events
[[test.results]]
value = "[{ created_at: d'2025-01-10T00:00:00Z', id: event:10, status: 'active' }, { created_at: d'2025-01-08T00:00:00Z', id: event:8, status: 'pending' }, { created_at: d'2025-01-07T00:00:00Z', id: event:7, status: 'active' }, { created_at: d'2025-01-05T00:00:00Z', id: event:5, status: 'pending' }, { created_at: d'2025-01-04T00:00:00Z', id: event:4, status: 'active' }]"

# WHERE IN ASC LIMIT 5: earliest 5 active/pending events
[[test.results]]
value = "[{ created_at: d'2025-01-01T00:00:00Z', id: event:1, status: 'active' }, { created_at: d'2025-01-02T00:00:00Z', id: event:2, status: 'pending' }, { created_at: d'2025-01-04T00:00:00Z', id: event:4, status: 'active' }, { created_at: d'2025-01-05T00:00:00Z', id: event:5, status: 'pending' }, { created_at: d'2025-01-07T00:00:00Z', id: event:7, status: 'active' }]"

# WHERE IN DESC LIMIT 5 START 3: skip 3, get next 4
[[test.results]]
value = "[{ created_at: d'2025-01-05T00:00:00Z', id: event:5, status: 'pending' }, { created_at: d'2025-01-04T00:00:00Z', id: event:4, status: 'active' }, { created_at: d'2025-01-02T00:00:00Z', id: event:2, status: 'pending' }, { created_at: d'2025-01-01T00:00:00Z', id: event:1, status: 'active' }]"

# Single IN value: 3 active events DESC
[[test.results]]
value = "[{ created_at: d'2025-01-10T00:00:00Z', id: event:10, status: 'active' }, { created_at: d'2025-01-07T00:00:00Z', id: event:7, status: 'active' }, { created_at: d'2025-01-04T00:00:00Z', id: event:4, status: 'active' }]"

# IN with all statuses: top 3 events (no filter)
[[test.results]]
value = "[{ created_at: d'2025-01-10T00:00:00Z', id: event:10, status: 'active' }, { created_at: d'2025-01-09T00:00:00Z', id: event:9, status: 'archived' }, { created_at: d'2025-01-08T00:00:00Z', id: event:8, status: 'pending' }]"
*/

-- Setup: two separate indexes — one for WHERE, one for ORDER BY.
-- The planner should choose idx_created_at for ORDER BY coverage and
-- apply the WHERE IN as a residual filter with batch ceiling.
{
    DEFINE TABLE event SCHEMAFULL;
    DEFINE FIELD status ON event TYPE string;
    DEFINE FIELD created_at ON event TYPE datetime;
    DEFINE INDEX idx_status ON event FIELDS status;
    DEFINE INDEX idx_created_at ON event FIELDS created_at;

    CREATE event:1 SET status = "active", created_at = d'2025-01-01T00:00:00Z';
    CREATE event:2 SET status = "pending", created_at = d'2025-01-02T00:00:00Z';
    CREATE event:3 SET status = "archived", created_at = d'2025-01-03T00:00:00Z';
    CREATE event:4 SET status = "active", created_at = d'2025-01-04T00:00:00Z';
    CREATE event:5 SET status = "pending", created_at = d'2025-01-05T00:00:00Z';
    CREATE event:6 SET status = "archived", created_at = d'2025-01-06T00:00:00Z';
    CREATE event:7 SET status = "active", created_at = d'2025-01-07T00:00:00Z';
    CREATE event:8 SET status = "pending", created_at = d'2025-01-08T00:00:00Z';
    CREATE event:9 SET status = "archived", created_at = d'2025-01-09T00:00:00Z';
    CREATE event:10 SET status = "active", created_at = d'2025-01-10T00:00:00Z';
    RETURN "OK";
};

-- Test 1: WHERE IN two values + ORDER BY DESC LIMIT 5
-- 7 matching rows (active + pending), should return latest 5
SELECT id, status, created_at FROM event
    WHERE status IN ['active', 'pending']
    ORDER BY created_at DESC LIMIT 5;

-- Test 2: same but ASC
SELECT id, status, created_at FROM event
    WHERE status IN ['active', 'pending']
    ORDER BY created_at ASC LIMIT 5;

-- Test 3: pagination
SELECT id, status, created_at FROM event
    WHERE status IN ['active', 'pending']
    ORDER BY created_at DESC LIMIT 5 START 3;

-- Test 4: single IN value
SELECT id, status, created_at FROM event
    WHERE status IN ['active']
    ORDER BY created_at DESC LIMIT 3;

-- Test 5: IN with all statuses (no filtering)
SELECT id, status, created_at FROM event
    WHERE status IN ['active', 'pending', 'archived']
    ORDER BY created_at DESC LIMIT 3;
