/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: [ ], fields: [ ], indexes: [ { cols: [ ], index: 'COUNT', name: 'count_idx', what: 'test' } ], lives: [ ], tables: [ ] }"

[[test.results]]
value = '''[{ detail: { plan: { index: 'count_idx', operator: 'Count' }, table: 'test' }, operation: 'Iterate Index Count' }, { detail: { "Aggregate expressions": {  }, Aggregations: { _a0: 'Count' }, "Group expressions": {  }, "Select expression": { count: '_a0' }, type: 'Group' }, operation: 'Collector' }]'''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: [ ], fields: [ ], indexes: [ { cols: [ ], decommissioned: true, index: 'COUNT', name: 'count_idx', what: 'test' } ], lives: [ ], tables: [ ] }"

[[test.results]]
value = '''[{ detail: { direction: 'forward', table: 'test' }, operation: 'Iterate Table Count' }, { detail: { "Aggregate expressions": {  }, Aggregations: { _a0: 'Count' }, "Group expressions": {  }, "Select expression": { count: '_a0' }, type: 'Group' }, operation: 'Collector' }]'''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "NONE"

*/

-- Define the table
DEFINE TABLE test;

-- Create records
{
    CREATE test:1 SET value = 'a';
    CREATE test:2 SET value = 'b';
    CREATE test:3 SET value = 'c';
    RETURN 'OK';
};

-- Create an INDEX COUNT
DEFINE INDEX count_idx ON test COUNT;

-- Check INFO FOR TABLE STRUCTURE shows decommission: false
INFO FOR TABLE test STRUCTURE;

-- Check that SELECT EXPLAIN uses the index
SELECT count() FROM test GROUP ALL EXPLAIN;
SELECT count() FROM test GROUP ALL;

-- Decommission the index with ALTER INDEX
ALTER INDEX count_idx ON test DECOMMISSION;

-- Check INFO FOR TABLE STRUCTURE shows decommission: true
INFO FOR TABLE test STRUCTURE;

-- Check that SELECT EXPLAIN does not use the index anymore
SELECT count() FROM test GROUP ALL EXPLAIN;
SELECT count() FROM test GROUP ALL;

-- We can safely remove the index
REMOVE INDEX count_idx ON test;