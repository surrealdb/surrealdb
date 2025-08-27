/**
[test]
reason = "Test the record::is::edge function to check if records are edges"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
error = "Incorrect arguments for function record::is::edge(). Expected a record ID"

[[test.results]]
error = "Incorrect arguments for function record::is::edge(). Expected a record ID"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "[{ id: person:john, name: 'John' }]"

[[test.results]]
value = "false"

[[test.results]]
value = "[{ id: person:jane, name: 'Jane' }]"

[[test.results]]
value = "[{ id: knows:person, in: person:john, out: person:jane, since: '2023-01-01' }]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ id: person:jane, is_edge: false, name: 'Jane' }, { id: person:john, is_edge: false, name: 'John' }]"

[[test.results]]
value = "[{ id: knows:person, in: person:john, is_edge: true, out: person:jane, since: '2023-01-01' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: knows:person, in: person:john, out: person:jane, since: '2023-01-01' }]"
*/

-- Test 1: Non-existent record, using a record ID
record::is::edge(person:nonexistent);

-- Test 2: Non-existent record, using a string
record::is::edge('person:nonexistent');

-- Test 3: Invalid argument - table name, not a record ID (should error)
record::is::edge(table);

-- Test 4: Invalid argument - number, not a record ID (should error)
record::is::edge(64746);

-- Test 5: Non-existent record, using `type::thing` constructor
record::is::edge(type::thing('person', 'nonexistent'));

-- Test 6: Non-existent record (before creation)
record::is::edge(person:john);

-- Test 7: Create a regular record (node)
CREATE person:john SET name = 'John';

-- Test 8: Check if the regular record is an edge (should be false)
record::is::edge(person:john);

-- Test 9: Create another regular record
CREATE person:jane SET name = 'Jane';

-- Test 10: Create an edge record (relationship) with an explicit ID
RELATE person:john->knows:person->person:jane SET since = '2023-01-01';

-- Test 11: Check if edge record is an edge, using a record ID (should be true)
record::is::edge(knows:person);

-- Test 12: Check if edge record is an edge, using a string (should be true)
record::is::edge('knows:person');

-- Test 13: Check if edge record is an edge, using `type::thing` constructor (should be true)
record::is::edge(type::thing('knows', 'person'));

-- Test 14: JavaScript function call for a regular record (should be false)
function() { return surrealdb.functions.record.is.edge("person:john"); };

-- Test 15: JavaScript function call for an edge record (should be true)
function() { return surrealdb.functions.record.is.edge("knows:person"); };

-- Test 16: Check if the function works as expected inside a select predicate (should be false)
SELECT *, record::is::edge(id) as is_edge FROM person;

-- Test 17: Check if the function works as expected inside a select predicate (should be true)
SELECT *, record::is::edge(id) as is_edge FROM knows;

-- Test 18: Check if the function works as expected inside a select filter (should be empty)
SELECT * FROM person WHERE record::is::edge(id) = true;

-- Test 19: Check if the function works as expected inside a select filter (should not be empty)
SELECT * FROM knows WHERE record::is::edge(id) = true;