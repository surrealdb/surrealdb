/**
[test]
reason = "Subqueries with IF/ELSE conditionals and array append (+=) operations"

[[test.results]]
value = "NONE"

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
value = "[{ sport: ['basketball', 'football'] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 1, id: person:test, sport: ['basketball', 'football'] }]"

[[test.results]]
value = "[{ sport: ['basketball', 'football', 'football'] }]"
*/

DEFINE TABLE person;
-- Check if the record exists
LET $record = (SELECT *, count() AS count FROM person:test);
-- Return the specified record
RETURN $record;
-- Update the record field if it exists
IF $record.count THEN
	(UPSERT person:test SET sport += 'football' RETURN sport)
ELSE
	(UPSERT person:test SET sport = ['basketball'] RETURN sport)
END;
-- Check if the record exists
LET $record = SELECT *, count() AS count FROM person:test;
-- Return the specified record
RETURN $record;
-- Update the record field if it exists
IF $record.count THEN
	UPSERT person:test SET sport += 'football' RETURN sport
ELSE
	UPSERT person:test SET sport = ['basketball'] RETURN sport
END;
-- Check if the record exists
LET $record = SELECT *, count() AS count FROM person:test;
-- Return the specified record
RETURN $record;
-- Update the record field if it exists
IF $record.count THEN
	UPSERT person:test SET sport += 'football' RETURN sport;
ELSE
	UPSERT person:test SET sport = ['basketball'] RETURN sport;
END;
