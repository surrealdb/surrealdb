/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: [], fields: [], indexes: [{ cols: [], index: 'COUNT', name: 'count_idx', table: 'test' }], lives: [], tables: [] }"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
└────> Scan [ctx: Db] [source: test]
"'''

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: [], fields: [], indexes: [{ cols: [], comment: 'may rebuild instead', index: 'COUNT', name: 'count_idx', prepare_remove: true, table: 'test' }], lives: [], tables: [] }"

[[test.results]]
value = '''
"Aggregate [ctx: Db] [mode: GROUP ALL]
└────> Scan [ctx: Db] [source: test]
"'''

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

-- Check INFO FOR TABLE STRUCTURE does show prepare_remove
INFO FOR TABLE test STRUCTURE;

-- Check that EXPLAIN SELECT uses the index
EXPLAIN SELECT count() FROM test GROUP ALL;
SELECT count() FROM test GROUP ALL;

-- Decommission the index with ALTER INDEX
ALTER INDEX count_idx ON test PREPARE REMOVE COMMENT 'may rebuild instead';

-- Check INFO FOR TABLE STRUCTURE shows prepare_remove: true
INFO FOR TABLE test STRUCTURE;

-- Check that EXPLAIN SELECT does not use the index anymore
EXPLAIN SELECT count() FROM test GROUP ALL;
SELECT count() FROM test GROUP ALL;

-- We can safely remove the index
REMOVE INDEX count_idx ON test;
