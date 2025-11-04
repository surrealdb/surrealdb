/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Tests LET with SELECT count(), IF-ELSE conditional logic, UPSERT with +?= append operator, and RETURN statements with syntax variations"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ sport: ['basketball'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 1, id: person:test, sport: ['basketball'] }]"

[[test.results]]
value = "[{ sport: ['basketball', 'tennis'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 1, id: person:test, sport: ['basketball', 'tennis'] }]"

[[test.results]]
value = "[{ sport: ['basketball', 'tennis', 'badminton'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ id: person:test, sport: ['basketball', 'tennis', 'badminton'] }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ sport: ['basketball', 'tennis', 'badminton', 'volleyball'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ category: 'multi-sport' }]"

[[test.results]]
value = "[{ category: 'multi-sport', id: person:test, sport: ['basketball', 'tennis', 'badminton', 'volleyball'] }]"

*/

-- Check if the record exists
LET $record = (SELECT *, count() AS count FROM person:test);
-- Return the specified record
RETURN $record;
-- Update the record field if it exists
IF $record.count THEN
    (UPSERT person:test SET sport +?= 'football' RETURN sport)
ELSE
    (UPSERT person:test SET sport = ['basketball'] RETURN sport)
END;

-- Check if the record exists (syntax variation - no parentheses around SELECT)
LET $record = SELECT *, count() AS count FROM person:test;
-- Return the specified record
RETURN $record;
-- Update the record field if it exists (syntax variation - no parentheses around UPSERT)
IF $record.count THEN
    UPSERT person:test SET sport +?= 'tennis' RETURN sport
ELSE
    UPSERT person:test SET sport = ['basketball'] RETURN sport
END;

-- Check if the record exists (same as above for consistency)
LET $record = SELECT *, count() AS count FROM person:test;
-- Return the specified record
RETURN $record;
-- Update the record field if it exists (syntax variation - semicolon after UPSERT)
IF $record.count THEN
    UPSERT person:test SET sport +?= 'badminton' RETURN sport;
ELSE
    UPSERT person:test SET sport = ['basketball'] RETURN sport;
END;

-- Additional test patterns for completeness

-- Test with ONLY to get single record
LET $single_record = SELECT * FROM ONLY person:test;
RETURN $single_record;

-- Test conditional logic with field existence instead of count
LET $check_field = SELECT sport FROM person:test;
IF $check_field.sport THEN
    UPSERT person:test SET sport +?= 'volleyball' RETURN sport
ELSE
    UPSERT person:test SET sport = ['soccer'] RETURN sport
END;

-- Test with complex conditional expression
LET $sports_count = SELECT array::len(sport) AS sport_count FROM person:test;
IF $sports_count.sport_count > 2 THEN
    UPSERT person:test SET category = 'multi-sport' RETURN category
ELSE
    UPSERT person:test SET category = 'beginner' RETURN category
END;

-- Final state inspection
SELECT * FROM person:test;
