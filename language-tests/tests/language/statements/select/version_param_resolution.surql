/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "VERSION should resolve DEFINE PARAM values from the versioned schema"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:1, price: 100 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ total: 1000 }]"

[[test.results]]
value = "[{ total: 200 }]"

*/

-- DEFINE PARAM values should be resolved at the versioned timestamp
-- when referenced in a VERSION query's expressions.

DEFINE PARAM OVERWRITE $multiplier VALUE 2;
CREATE item:1 SET price = 100;

SLEEP 50ms;
LET $t1 = time::now();
SLEEP 50ms;

-- Change the param
DEFINE PARAM OVERWRITE $multiplier VALUE 10;

-- HEAD: $multiplier is 10
SELECT price * $multiplier AS total FROM item:1;

-- VERSION $t1: $multiplier should be 2 (the value at that time)
SELECT price * $multiplier AS total FROM item:1 VERSION $t1;
