/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Tests UPSERT, RELATE with complex array record IDs, and SELECT with field access plus graph traversal"

# 0
[[test.results]]
value = "[{ brother: person:tobie, id: person:jaime, name: 'Jaime' }]"

# 1
[[test.results]]
value = "[{ brother: person:jaime, id: person:tobie, name: 'Tobie' }]"

# 2
[[test.results]]
value = "[{ id: knows:[person:tobie, person:jaime], in: person:tobie, out: person:jaime, when: d'2025-10-03T10:28:16.113963Z' }]"
skip-datetime = true

# 3
[[test.results]]
value = "[{ brother: { name: 'Jaime' }, friends: ['Jaime'], id: person:tobie, name: 'Tobie' }]"

# 4
[[test.results]]
value = "[{ brother: { name: 'Tobie' }, id: person:jaime, known_by: ['Tobie'], name: 'Jaime' }]"

# 5
[[test.results]]
value = '''[{ "->knows": { when: [d'2025-10-03T10:28:16.113963Z'] }, brother: person:jaime, id: person:tobie, name: 'Tobie', specific_friends: ['Jaime'] }]'''
skip-datetime = true

# 6
[[test.results]]
value = "[{ id: knows:[person:tobie, person:jaime], in: person:tobie, out: person:jaime, when: d'2025-10-03T10:28:16.113963Z' }]"
skip-datetime = true

# 7
[[test.results]]
value = "[{ id: knows:[person:tobie, person:jaime], in: person:tobie, out: person:jaime, when: d'2025-10-03T10:28:16.113963Z' }]"
skip-datetime = true

# 8
[[test.results]]
value = "[{ id: knows:[person:tobie, person:jaime], in: person:tobie, out: person:jaime, strength: 'very strong', updated: d'2025-10-03T10:28:16.117085Z', when: d'2025-10-03T10:28:16.113963Z' }]"
skip-datetime = true

# 9
[[test.results]]
value = "[{ id: knows:[person:tobie, person:jaime], in: person:tobie, out: person:jaime, strength: 'very strong', updated: d'2025-10-03T10:28:16.117085Z', when: d'2025-10-03T10:28:16.113963Z' }]"
skip-datetime = true

*/

-- 0: Create bidirectional brother relationships using UPSERT
UPSERT person:jaime SET brother = person:tobie, name = 'Jaime';

-- 1: Create bidirectional brother relationships using UPSERT
UPSERT person:tobie SET brother = person:jaime, name = 'Tobie';

-- 2: Create a relation with complex array-based record ID containing both person records
RELATE person:tobie->knows:[person:tobie, person:jaime]->person:jaime SET when = time::now();

-- 3: Test complex SELECT combining fields, field access, and graph traversal
SELECT *, brother.name, ->knows->?.name AS friends FROM person:tobie;

-- 4: Test the reverse relationship
SELECT *, brother.name, <-knows<-?.name AS known_by FROM person:jaime;

-- 5: Test without the wildcard to see the difference
SELECT *, ->knows.when, ->knows->person.name AS specific_friends FROM person:tobie;

-- 6: Test querying the relation record itself
SELECT * FROM knows:[person:tobie, person:jaime];

-- 7: Test range query on the complex relation IDs
SELECT * FROM knows:[person:tobie, NONE]..;

-- 8: Test UPSERT on the relation record
UPSERT knows:[person:tobie, person:jaime] SET strength = 'very strong', updated = time::now();

-- 9: Verify the updated relation
SELECT * FROM knows:[person:tobie, person:jaime];
