/**
[env]
namespace = true
database = true
auth = { level = "owner" }


[test]
reason = "An array-element index (`FIELDS tags[*]`) answers `tags[*] CONTAINS v` and `v INSIDE tags[*]` with an index seek. `tags CONTAINS v` returns the same rows here but is answered by a scan, because a stored scalar could satisfy it without holding `v` as an element."

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
value = '''
"SelectProject [ctx: Db] [projections: name]
    TableScan [ctx: Db] [table: organization, direction: Forward, predicate: mission_topic_tags CONTAINS 'climate', pre_decode_filter: yes]
"'''

[[test.results]]
value = "[{ name: 'Climate Org' }, { name: 'Green Edu' }]"

[[test.results]]
value = "[{ name: 'Education Org' }, { name: 'Green Edu' }]"

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: name]
    IndexScan [ctx: Db] [table: organization, index: org_topics, access: = 'climate', direction: Forward]
"'''

[[test.results]]
value = '''
"SelectProject [ctx: Db] [projections: name]
    IndexScan [ctx: Db] [table: organization, index: org_topics, access: = 'education', direction: Forward]
"'''
*/

-- Define index on array field elements
DEFINE INDEX org_topics ON organization FIELDS mission_topic_tags[*];

-- Insert test data
CREATE organization:a SET name = "Climate Org", mission_topic_tags = ["climate", "environment"];
CREATE organization:b SET name = "Education Org", mission_topic_tags = ["education", "youth"];
CREATE organization:c SET name = "Green Edu", mission_topic_tags = ["climate", "education"];
CREATE organization:d SET name = "Health Org", mission_topic_tags = ["health"];
CREATE organization:e SET name = "Empty Org", mission_topic_tags = [];

-- CONTAINS and INSIDE over the field itself
SELECT name FROM organization WHERE mission_topic_tags CONTAINS "climate";
SELECT name FROM organization WHERE "education" INSIDE mission_topic_tags;
EXPLAIN SELECT name FROM organization WHERE mission_topic_tags CONTAINS "climate";

-- The flattened spelling seeks the index, in both directions
SELECT name FROM organization WHERE mission_topic_tags[*] CONTAINS "climate";
SELECT name FROM organization WHERE "education" INSIDE mission_topic_tags[*];
EXPLAIN SELECT name FROM organization WHERE mission_topic_tags[*] CONTAINS "climate";
EXPLAIN SELECT name FROM organization WHERE "education" INSIDE mission_topic_tags[*];
