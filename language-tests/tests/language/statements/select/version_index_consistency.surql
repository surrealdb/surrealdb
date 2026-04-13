/**
[env]
backend = ["mem", "rocksdb", "surrealkv"]
versioned = true
timeout = 5000

[test]
reason = "VERSION clause shows historical schema state for indexes defined after the version timestamp"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: product:1, name: 'Widget', price: 10 }]"

[[test.results]]
value = "[{ id: product:2, name: 'Gadget', price: 20 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: { idx_price: 'DEFINE INDEX idx_price ON product FIELDS price' }, lives: {  }, tables: {  } }"

*/

-- Create table and data
DEFINE TABLE product;
CREATE product:1 SET name = 'Widget', price = 10;
CREATE product:2 SET name = 'Gadget', price = 20;

-- Capture timestamp before index creation
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;

-- Define an index after the timestamp
DEFINE INDEX idx_price ON TABLE product FIELDS price;

-- INFO FOR TABLE at VERSION $ts: no indexes
INFO FOR TABLE product VERSION $ts;

-- INFO FOR TABLE current: shows the index
INFO FOR TABLE product;
