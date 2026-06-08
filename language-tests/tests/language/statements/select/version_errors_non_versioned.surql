/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = false
timeout = 5000

[test]
reason = "VERSION clause on a non-versioned backend does not return current data"

[[test.results]]
value = "[{ id: test:1, val: 1 }]"

[[test.results]]
value = "[{ id: test:1, val: 1 }]"

[[test.results]]
error = "There was a problem with the key-value store: The underlying datastore does not support versioned queries"
*/

-- Create some data
CREATE test:1 SET val = 1;

-- Without VERSION: returns data normally
SELECT * FROM test;

-- With VERSION: same error across all backends
SELECT * FROM test VERSION time::now();