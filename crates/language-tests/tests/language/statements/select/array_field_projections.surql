/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Tests UPSERT with array fields, field destructuring, FETCH patterns, and $parent subqueries"

[[test.results]]
value = "[{ id: tag:rs, name: 'Rust' }]"

[[test.results]]
value = "[{ id: tag:js, name: 'JavaScript' }]"

[[test.results]]
value = "[{ id: tag:php, name: 'PHP' }]"

[[test.results]]
value = "[{ id: tag:go, name: 'Golang' }]"

[[test.results]]
value = "[{ id: person:tobie, tags: [tag:rs, tag:js, tag:php, tag:go] }]"

[[test.results]]
value = "[{ id: person:jaime, tags: [tag:js] }]"

[[test.results]]
value = "[{ id: person:other, tags: [tag:js, tag:rs] }]"

#[[test.results]]
#value = "[{ id: person:jaime, tags: [tag:js] }, { id: person:other, tags: [tag:js, tag:rs] }, { id: person:tobie, tags: [tag:rs, tag:js, tag:php, tag:go] }]"
#
#[[test.results]]
#value = "[{ tags: [{ id: tag:js, name: 'JavaScript' }] }, { tags: [{ id: tag:js, name: 'JavaScript' }, { id: tag:rs, name: 'Rust' }] }, { tags: [{ id: tag:rs, name: 'Rust' }, { id: tag:js, name: 'JavaScript' }, { id: tag:php, name: 'PHP' }, { id: tag:go, name: 'Golang' }] }]"
#
#[[test.results]]
#value = "[{ tags: { id: [tag:js], name: ['JavaScript'] } }, { tags: { id: [tag:js, tag:rs], name: ['JavaScript', 'Rust'] } }, { tags: { id: [tag:rs, tag:js, tag:php, tag:go], name: ['Rust', 'JavaScript', 'PHP', 'Golang'] } }]"
#
#[[test.results]]
#value = "[{ id: person:jaime, tags: [{ id: tag:js, name: 'JavaScript' }] }, { id: person:other, tags: [{ id: tag:js, name: 'JavaScript' }, { id: tag:rs, name: 'Rust' }] }, { id: person:tobie, tags: [{ id: tag:rs, name: 'Rust' }, { id: tag:js, name: 'JavaScript' }, { id: tag:php, name: 'PHP' }, { id: tag:go, name: 'Golang' }] }]"
#
#[[test.results]]
#value = "[{ id: person:jaime, tags: [{ id: tag:js, name: 'JavaScript' }] }, { id: person:other, tags: [{ id: tag:js, name: 'JavaScript' }, { id: tag:rs, name: 'Rust' }] }, { id: person:tobie, tags: [{ id: tag:rs, name: 'Rust' }, { id: tag:js, name: 'JavaScript' }, { id: tag:php, name: 'PHP' }, { id: tag:go, name: 'Golang' }] }]"
#
#[[test.results]]
#value = "[{ tags: [{ id: tag:js, name: 'JavaScript' }, { id: tag:rs, name: 'Rust' }] }, { tags: [{ id: tag:rs, name: 'Rust' }, { id: tag:js, name: 'JavaScript' }, { id: tag:php, name: 'PHP' }, { id: tag:go, name: 'Golang' }] }]"
#
#[[test.results]]
#value = "[{ id: person:other, tags: [{ id: tag:js, name: 'JavaScript' }, { id: tag:rs, name: 'Rust' }] }, { id: person:tobie, tags: [{ id: tag:rs, name: 'Rust' }, { id: tag:js, name: 'JavaScript' }, { id: tag:php, name: 'PHP' }, { id: tag:go, name: 'Golang' }] }]"
#
#[[test.results]]
#value = "[{ id: person:jaime, tags: { id: [tag:js] } }, { id: person:other, tags: { id: [tag:js, tag:rs] } }, { id: person:tobie, tags: { id: [tag:rs, tag:js, tag:php, tag:go] } }]"
#
[[test.results]]
value = "[{ id: person:jaime, js_tags: [{ id: tag:js, name: 'JavaScript' }], tags: [tag:js] }, { id: person:other, js_tags: [{ id: tag:js, name: 'JavaScript' }], tags: [tag:js, tag:rs] }, { id: person:tobie, js_tags: [{ id: tag:js, name: 'JavaScript' }], tags: [tag:rs, tag:js, tag:php, tag:go] }]"

#[[test.results]]
#value = "[{ tag_count: 4, tags: [{ id: tag:rs, name: 'Rust' }, { id: tag:js, name: 'JavaScript' }, { id: tag:php, name: 'PHP' }, { id: tag:go, name: 'Golang' }] }, { tag_count: 2, tags: [{ id: tag:js, name: 'JavaScript' }, { id: tag:rs, name: 'Rust' }] }, { tag_count: 1, tags: [{ id: tag:js, name: 'JavaScript' }] }]"

*/

-- Create tag records using UPSERT
UPSERT tag:rs SET name = 'Rust';
UPSERT tag:js SET name = 'JavaScript';
UPSERT tag:php SET name = 'PHP';
UPSERT tag:go SET name = 'Golang';

-- Create person records with arrays of tag references
UPSERT person:tobie SET tags = [tag:rs, tag:js, tag:php, tag:go];
UPSERT person:jaime SET tags = [tag:js];
UPSERT person:other SET tags = [tag:js, tag:rs];

-- Basic SELECT from person table
#SELECT * FROM person;

-- Field destructuring with curly braces - project id and name from each tag
#SELECT tags.{id,name} FROM person;

-- Individual field projection from array elements
#SELECT tags.id, tags.name FROM person;

-- FETCH with nested field access - fetch name field from all tags
#SELECT * FROM person FETCH tags.*.name;

-- Subquery using $parent to access array elements
#SELECT *, (SELECT id, name FROM $parent.tags) AS tags FROM person;

-- Additional projection patterns for completeness

-- Test with WHERE clause on projected fields
#SELECT tags.{id,name} FROM person WHERE array::len(tags) > 1;

-- Test destructuring with nested selection
#SELECT id, tags.{id, name} FROM person WHERE tags.*.name CONTAINS 'Rust';

-- Test FETCH combined with projection
#SELECT id, tags.id FROM person FETCH tags.name;

-- Test $parent with filtering
SELECT *, (SELECT id, name FROM $this.tags WHERE name CONTAINS 'Script') AS js_tags FROM person;

-- Test complex projection with ordering
# SELECT tags.{id,name}, array::len(tags) AS tag_count FROM person ORDER BY tag_count DESC;
