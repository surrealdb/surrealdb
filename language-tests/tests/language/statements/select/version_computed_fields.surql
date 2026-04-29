/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "VERSION clause should use computed field definitions from the versioned schema"

[[test.results]]
value = "[{ id: item:1, price: 100, tax: 10 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:1, price: 100, tax: 10, total: 110 }]"

[[test.results]]
value = "[{ id: item:1, price: 100, tax: 10, total: 110 }]"

[[test.results]]
value = "[{ id: item:1, price: 100, tax: 10 }]"

*/

-- Computed field definitions should be resolved at the versioned
-- timestamp, not HEAD. If a computed field is added after the
-- timestamp, VERSION queries should not evaluate it.

CREATE item:1 SET price = 100, tax = 10;

SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;

-- Add a computed field after $t1
DEFINE FIELD total ON item VALUE $this.price + $this.tax;

-- Update the record to trigger the computed field
UPDATE item:1 SET price = 100;

-- HEAD: should include the computed total field
SELECT * FROM item:1;

-- VERSION $t1: computed field didn't exist, should not have total
SELECT * FROM item:1 VERSION $t1;
