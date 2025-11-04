/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Tests UPSERT, RELATE with complex array record IDs, and SELECT with field access plus graph traversal"

[[test.results]]
value = "[{ brother: person:tobie, id: person:jaime, name: 'Jaime' }]"

[[test.results]]
value = "[{ brother: person:jaime, id: person:tobie, name: 'Tobie' }]"

[[test.results]]
value = "[{ id: knows:[person:tobie, person:jaime], in: person:tobie, out: person:jaime, when: d'2025-10-03T10:28:16.113963Z' }]"
skip-datetime = true

[[test.results]]
value = "[{ brother: { name: 'Jaime' }, friends: ['Jaime'], id: person:tobie, name: 'Tobie' }]"

[[test.results]]
value = "[{ brother: { name: 'Tobie' }, id: person:jaime, known_by: ['Tobie'], name: 'Jaime' }]"

[[test.results]]
value = '''[{ "->knows": { when: [d'2025-10-03T10:28:16.113963Z'] }, brother: person:jaime, id: person:tobie, name: 'Tobie', specific_friends: ['Jaime'] }]'''
skip-datetime = true

[[test.results]]
value = "[{ id: knows:[person:tobie, person:jaime], in: person:tobie, out: person:jaime, when: d'2025-10-03T10:28:16.113963Z' }]"
skip-datetime = true

[[test.results]]
value = "[{ id: knows:[person:tobie, person:jaime], in: person:tobie, out: person:jaime, when: d'2025-10-03T10:28:16.113963Z' }]"
skip-datetime = true

[[test.results]]
value = "[{ id: knows:[person:tobie, person:jaime], in: person:tobie, out: person:jaime, strength: 'very strong', updated: d'2025-10-03T10:28:16.117085Z', when: d'2025-10-03T10:28:16.113963Z' }]"
skip-datetime = true

[[test.results]]
value = "[{ id: knows:[person:tobie, person:jaime], in: person:tobie, out: person:jaime, strength: 'very strong', updated: d'2025-10-03T10:28:16.117085Z', when: d'2025-10-03T10:28:16.113963Z' }]"
skip-datetime = true

*/

-- Create bidirectional brother relationships using UPSERT
UPSERT person:jaime SET brother = person:tobie, name = 'Jaime';
UPSERT person:tobie SET brother = person:jaime, name = 'Tobie';

-- Create a relation with complex array-based record ID containing both person records
RELATE person:tobie->knows:[person:tobie, person:jaime]->person:jaime SET when = time::now();

-- Test complex SELECT combining:
-- 1. All fields (*)
-- 2. Field access on related record (brother.name)
-- 3. Graph traversal with wildcard (->knows->?.name AS friends)
SELECT *, brother.name, ->knows->?.name AS friends FROM person:tobie;

-- Additional tests for completeness

-- Test the reverse relationship
SELECT *, brother.name, <-knows<-?.name AS known_by FROM person:jaime;

-- Test without the wildcard to see the difference
SELECT *, ->knows.when, ->knows->person.name AS specific_friends FROM person:tobie;

-- Test querying the relation record itself
SELECT * FROM knows:[person:tobie, person:jaime];

-- Test range query on the complex relation IDs
SELECT * FROM knows:[person:tobie, NONE]..;

-- Test UPSERT on the relation record
UPSERT knows:[person:tobie, person:jaime] SET strength = 'very strong', updated = time::now();

-- Verify the updated relation
SELECT * FROM knows:[person:tobie, person:jaime];
