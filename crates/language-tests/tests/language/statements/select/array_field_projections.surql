/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Tests UPSERT with array fields, field destructuring, FETCH patterns, and $parent subqueries"

# 0
[[test.results]]
value = "[{ id: tag:rs, name: 'Rust' }]"

# 1
[[test.results]]
value = "[{ id: tag:js, name: 'JavaScript' }]"

# 2
[[test.results]]
value = "[{ id: tag:php, name: 'PHP' }]"

# 3
[[test.results]]
value = "[{ id: tag:go, name: 'Golang' }]"

# 4
[[test.results]]
value = "[{ id: person:tobie, tags: [tag:rs, tag:js, tag:php, tag:go] }]"

# 5
[[test.results]]
value = "[{ id: person:jaime, tags: [tag:js] }]"

# 6
[[test.results]]
value = "[{ id: person:other, tags: [tag:js, tag:rs] }]"

# 7
[[test.results]]
value = "[{ id: person:jaime, tags: [tag:js] }, { id: person:other, tags: [tag:js, tag:rs] }, { id: person:tobie, tags: [tag:rs, tag:js, tag:php, tag:go] }]"

# 8
[[test.results]]
value = "[{ tags: [{ id: tag:js, name: 'JavaScript' }] }, { tags: [{ id: tag:js, name: 'JavaScript' }, { id: tag:rs, name: 'Rust' }] }, { tags: [{ id: tag:rs, name: 'Rust' }, { id: tag:js, name: 'JavaScript' }, { id: tag:php, name: 'PHP' }, { id: tag:go, name: 'Golang' }] }]"

# 9
[[test.results]]
value = "[{ tags: { id: [tag:js], name: ['JavaScript'] } }, { tags: { id: [tag:js, tag:rs], name: ['JavaScript', 'Rust'] } }, { tags: { id: [tag:rs, tag:js, tag:php, tag:go], name: ['Rust', 'JavaScript', 'PHP', 'Golang'] } }]"

# 10
[[test.results]]
value = "[{ id: person:jaime, tags: [{ id: tag:js, name: 'JavaScript' }] }, { id: person:other, tags: [{ id: tag:js, name: 'JavaScript' }, { id: tag:rs, name: 'Rust' }] }, { id: person:tobie, tags: [{ id: tag:rs, name: 'Rust' }, { id: tag:js, name: 'JavaScript' }, { id: tag:php, name: 'PHP' }, { id: tag:go, name: 'Golang' }] }]"

# 11
[[test.results]]
value = "[{ id: person:jaime, tags: [{ id: tag:js, name: 'JavaScript' }] }, { id: person:other, tags: [{ id: tag:js, name: 'JavaScript' }, { id: tag:rs, name: 'Rust' }] }, { id: person:tobie, tags: [{ id: tag:rs, name: 'Rust' }, { id: tag:js, name: 'JavaScript' }, { id: tag:php, name: 'PHP' }, { id: tag:go, name: 'Golang' }] }]"

# 12
[[test.results]]
value = "[{ tags: [{ id: tag:js, name: 'JavaScript' }, { id: tag:rs, name: 'Rust' }] }, { tags: [{ id: tag:rs, name: 'Rust' }, { id: tag:js, name: 'JavaScript' }, { id: tag:php, name: 'PHP' }, { id: tag:go, name: 'Golang' }] }]"

# 13
[[test.results]]
value = "[{ id: person:other, tags: [{ id: tag:js, name: 'JavaScript' }, { id: tag:rs, name: 'Rust' }] }, { id: person:tobie, tags: [{ id: tag:rs, name: 'Rust' }, { id: tag:js, name: 'JavaScript' }, { id: tag:php, name: 'PHP' }, { id: tag:go, name: 'Golang' }] }]"

# 14
[[test.results]]
value = "[{ id: person:jaime, tags: { id: [tag:js] } }, { id: person:other, tags: { id: [tag:js, tag:rs] } }, { id: person:tobie, tags: { id: [tag:rs, tag:js, tag:php, tag:go] } }]"

# 15
[[test.results]]
value = "[{ id: person:jaime, js_tags: [{ id: tag:js, name: 'JavaScript' }], tags: [tag:js] }, { id: person:other, js_tags: [{ id: tag:js, name: 'JavaScript' }], tags: [tag:js, tag:rs] }, { id: person:tobie, js_tags: [{ id: tag:js, name: 'JavaScript' }], tags: [tag:rs, tag:js, tag:php, tag:go] }]"

# 16
[[test.results]]
value = "[{ tag_count: 4, tags: [{ id: tag:rs, name: 'Rust' }, { id: tag:js, name: 'JavaScript' }, { id: tag:php, name: 'PHP' }, { id: tag:go, name: 'Golang' }] }, { tag_count: 2, tags: [{ id: tag:js, name: 'JavaScript' }, { id: tag:rs, name: 'Rust' }] }, { tag_count: 1, tags: [{ id: tag:js, name: 'JavaScript' }] }]"

*/

-- Create tag records using UPSERT
-- 0, 1, 2, 3
UPSERT tag:rs SET name = 'Rust';
UPSERT tag:js SET name = 'JavaScript';
UPSERT tag:php SET name = 'PHP';
UPSERT tag:go SET name = 'Golang';

-- Create person records with arrays of tag references
-- 4, 5, 6
UPSERT person:tobie SET tags = [tag:rs, tag:js, tag:php, tag:go];
UPSERT person:jaime SET tags = [tag:js];
UPSERT person:other SET tags = [tag:js, tag:rs];

-- Basic SELECT from person table
-- 7
SELECT * FROM person;

-- Field destructuring with curly braces - project id and name from each tag
-- 8
SELECT tags.{id,name} FROM person;

-- Individual field projection from array elements
-- 9
SELECT tags.id, tags.name FROM person;

-- FETCH with nested field access - fetch name field from all tags
-- 10
SELECT * FROM person FETCH tags.*.name;

-- Subquery using $parent to access array elements
-- 11
SELECT *, (SELECT id, name FROM $this.tags) AS tags FROM person;

-- Additional projection patterns for completeness

-- Test with WHERE clause on projected fields
-- 12
SELECT tags.{id,name} FROM person WHERE array::len(tags) > 1;

-- Test destructuring with nested selection
-- 13
SELECT id, tags.{id, name} FROM person WHERE tags.*.name CONTAINS 'Rust';

-- Test FETCH combined with projection
-- 14
SELECT id, tags.id FROM person FETCH tags.name;

-- Test $parent with filtering
-- 15
SELECT *, (SELECT id, name FROM $this.tags WHERE name CONTAINS 'Script') AS js_tags FROM person;

-- Test complex projection with ordering
-- 16
SELECT tags.{id,name}, array::len(tags) AS tag_count FROM person ORDER BY tag_count DESC;
