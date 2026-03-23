/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro"]

[test]
reason = "WHERE field CONTAINS scalar and scalar INSIDE field should use array indexes with Part::All"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: organization:a, mission_topic_tags: ['climate', 'environment'], name: 'Climate Org' }]"

[[test.results]]
value = "[{ id: organization:b, mission_topic_tags: ['education', 'youth'], name: 'Education Org' }]"

[[test.results]]
value = "[{ id: organization:c, mission_topic_tags: ['climate', 'education'], name: 'Green Edu' }]"

[[test.results]]
value = "[{ id: organization:d, mission_topic_tags: ['health'], name: 'Health Org' }]"

[[test.results]]
value = "[{ id: organization:e, mission_topic_tags: [], name: 'Empty Org' }]"

[[test.results]]
value = "[{ name: 'Climate Org' }, { name: 'Green Edu' }]"

[[test.results]]
value = "[{ name: 'Education Org' }, { name: 'Green Edu' }]"

[[test.results]]
value = """{ attributes: { projections: 'name' }, children: [{ attributes: { predicate: "mission_topic_tags CONTAINS 'climate'" }, children: [{ attributes: { access: "= 'climate'", direction: 'Forward', index: 'org_topics' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', expressions: [{ role: 'predicate', sql: "mission_topic_tags CONTAINS 'climate'" }], operator: 'Filter' }], context: 'Db', operator: 'SelectProject' }"""
*/

-- Define index on array field elements
DEFINE INDEX org_topics ON organization FIELDS mission_topic_tags[*];

-- Insert test data
CREATE organization:a SET name = "Climate Org", mission_topic_tags = ["climate", "environment"];
CREATE organization:b SET name = "Education Org", mission_topic_tags = ["education", "youth"];
CREATE organization:c SET name = "Green Edu", mission_topic_tags = ["climate", "education"];
CREATE organization:d SET name = "Health Org", mission_topic_tags = ["health"];
CREATE organization:e SET name = "Empty Org", mission_topic_tags = [];

-- Test: CONTAINS should use index
SELECT name FROM organization WHERE mission_topic_tags CONTAINS "climate";

-- Test: INSIDE (reverse) should also use index
SELECT name FROM organization WHERE "education" INSIDE mission_topic_tags;

-- Verify index usage via EXPLAIN
SELECT name FROM organization WHERE mission_topic_tags CONTAINS "climate" EXPLAIN;
