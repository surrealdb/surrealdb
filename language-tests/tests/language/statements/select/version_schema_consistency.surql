/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "VERSION clause provides consistent schema and data snapshots at the same point in time"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t1:1, name: 'Alice' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t1:2, name: 'Bob' }]"

[[test.results]]
value = "[{ id: t1:1, name: 'Alice' }, { id: t1:2, name: 'Bob' }]"

[[test.results]]
value = "[{ id: t1:1, name: 'Alice' }]"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "{ events: {  }, fields: { name: 'DEFINE FIELD name ON t1 TYPE string PERMISSIONS FULL' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "[{ id: t1:1, name: 'Alice' }, { id: t1:2, name: 'Bob' }]"

*/

-- Create table and initial data
DEFINE TABLE t1;
CREATE t1:1 SET name = 'Alice';

-- Capture timestamp before schema change
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;

-- Add a field definition and more data after the timestamp
DEFINE FIELD name ON TABLE t1 TYPE string;
CREATE t1:2 SET name = 'Bob';

-- Current state: both records
SELECT * FROM t1;

-- VERSION $ts: only the first record (before second CREATE)
SELECT * FROM t1 VERSION $ts;

-- INFO FOR TABLE at VERSION $ts: should have NO field definitions
INFO FOR TABLE t1 VERSION $ts;

-- INFO FOR TABLE current: should have the field definition
INFO FOR TABLE t1;

-- Current state: all records
SELECT * FROM t1;
