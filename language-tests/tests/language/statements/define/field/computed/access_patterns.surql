/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Validates computed fields are evaluated across all record access patterns"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ age: 30, can_drive: true, friend: person:jaime, id: person:tobie, label: 'Tobie (30)', name: 'Tobie' }]"

[[test.results]]
value = "[{ age: 16, can_drive: false, id: person:jaime, label: 'Jaime (16)', name: 'Jaime' }, { age: 30, can_drive: true, friend: person:jaime, id: person:tobie, label: 'Tobie (30)', name: 'Tobie' }]"

[[test.results]]
value = "{ age: 30, can_drive: true, friend: person:jaime, id: person:tobie, label: 'Tobie (30)', name: 'Tobie' }"

[[test.results]]
value = "[{ age: 30, can_drive: true, friend: { age: 16, can_drive: false, id: person:jaime, label: 'Jaime (16)', name: 'Jaime' }, id: person:tobie, label: 'Tobie (30)', name: 'Tobie' }]"

[[test.results]]
value = '[{ "->knows": [knows:l6ovdnryn4ydl00yez5f] }]'
skip-record-id-key = true

[[test.results]]
value = '[{ "->knows": { "->person": [person:jaime] } }]'

[[test.results]]
value = """[{ "->knows": { "->person": [{ age: 16, can_drive: false, id: person:jaime, label: 'Jaime (16)', name: 'Jaime' }] } }]"""

[[test.results]]
value = "[{ age: 30, can_drive: true, friend: person:jaime, id: person:tobie, label: 'Tobie (30)', name: 'Tobie' }, { age: 16, can_drive: false, id: person:jaime, label: 'Jaime (16)', name: 'Jaime' }]"

[[test.results]]
value = "[{ age: 30, can_drive: true, friend: person:jaime, id: person:tobie, label: 'Tobie (30)', name: 'Tobie' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 30, can_drive: true, friend: person:jaime, id: person:tobie, label: 'Tobie (30)', name: 'Tobie' }]"

[[test.results]]
value = "[{ friend: { age: 16, can_drive: false, id: person:jaime, label: 'Jaime (16)', name: 'Jaime' } }]"

[[test.results]]
value = '''[{ "->?": { "->?": [{ age: 16, can_drive: false, id: person:jaime, label: 'Jaime (16)', name: 'Jaime' }] } }]'''

*/

{
	DEFINE TABLE person SCHEMALESS;
	DEFINE FIELD can_drive ON person TYPE bool COMPUTED age >= 18;
	DEFINE FIELD label ON person TYPE string COMPUTED string::concat(name, ' (', <string>age, ')');

	CREATE person:tobie SET name = 'Tobie', age = 30, friend = person:jaime;
	CREATE person:jaime SET name = 'Jaime', age = 16;

	RELATE person:tobie->knows->person:jaime;

	RETURN 'OK';
};

-- 1: Point lookup by record ID
SELECT * FROM person:tobie;

-- 2: Full table scan
SELECT * FROM person ORDER BY id;

-- 3: Idiom dereference via RETURN
RETURN person:tobie.*;

-- 4: FETCH on a record link field (fetched record should also have computed fields)
SELECT * FROM person:tobie FETCH friend;

-- 5: Single-hop graph edge
SELECT ->knows FROM person:tobie;

-- 6: Graph traversal to target table
SELECT ->knows->person FROM person:tobie;

-- 7: Graph traversal with wildcard dereference
SELECT ->knows->person.* FROM person:tobie;

-- 8: Array source
SELECT * FROM [person:tobie, person:jaime];

-- 9: Subquery source (VALUE returns record IDs, outer SELECT re-fetches them)
SELECT * FROM (SELECT VALUE id FROM person WHERE name = 'Tobie');

-- 10: Param-bound record ID
LET $x = person:tobie;
SELECT * FROM $x;

-- 11: Record link field dereference
SELECT friend.* FROM person:tobie;

-- 12: Double dereference with wildcard
SELECT ->?->?.* FROM person:tobie;
