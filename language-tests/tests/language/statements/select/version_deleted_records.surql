/**
[env]
backend = ["surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "A VERSION read does not return a record which was deleted as of the version, whether the table is scanned forwards, backwards, in a LIMIT and START window, or only counted, and returns it again from the version it was created anew"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:2, name: 'Bob again' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: person:1, name: 'Alice' }, { id: person:2, name: 'Bob' }, { id: person:3, name: 'Carol' }]"

[[test.results]]
value = "[{ id: person:1, name: 'Alice' }, { id: person:3, name: 'Carol' }]"

[[test.results]]
value = "[{ id: person:3, name: 'Carol' }, { id: person:1, name: 'Alice' }]"

[[test.results]]
value = "[{ id: person:1, name: 'Alice' }]"

[[test.results]]
value = "[{ id: person:3, name: 'Carol' }]"

[[test.results]]
value = "[{ id: person:1, name: 'Alice' }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: person:1, name: 'Alice' }, { id: person:2, name: 'Bob again' }, { id: person:3, name: 'Carol' }]"

*/

-- Create three records
{
	CREATE person:1 SET name = 'Alice';
	CREATE person:2 SET name = 'Bob';
	CREATE person:3 SET name = 'Carol';
	RETURN "OK";
};

-- Capture a timestamp before the delete
SLEEP 50ms;
LET $ts_before = time::now();
SLEEP 50ms;

-- Delete the middle record
DELETE person:2;

-- Capture a timestamp after the delete
SLEEP 50ms;
LET $ts_after = time::now();
SLEEP 50ms;

-- Create the deleted record anew
CREATE person:2 SET name = 'Bob again';

-- Capture a timestamp after the record was created anew
SLEEP 50ms;
LET $ts_again = time::now();
SLEEP 50ms;

-- Before the delete: all three records
SELECT * FROM person VERSION $ts_before;

-- After the delete: the deleted record is not returned
SELECT * FROM person VERSION $ts_after;

-- After the delete, backwards
SELECT * FROM person ORDER BY id DESC VERSION $ts_after;

-- After the delete, the deleted record takes up no place of a LIMIT or START
SELECT * FROM person LIMIT 1 VERSION $ts_after;
SELECT * FROM person LIMIT 1 START 1 VERSION $ts_after;
SELECT * FROM person ORDER BY id DESC LIMIT 1 START 1 VERSION $ts_after;

-- After the delete, the deleted record is not counted
SELECT count() FROM person GROUP ALL VERSION $ts_after;

-- After the delete, a read of the record itself agrees
SELECT * FROM person:2 VERSION $ts_after;

-- After the record was created anew: all three records, and the new one is the record
SELECT * FROM person VERSION $ts_again;
