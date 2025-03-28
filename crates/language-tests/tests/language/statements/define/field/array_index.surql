/**
[test]

[[test.results]]
value = "{ events: {  }, fields: { items: 'DEFINE FIELD items[*] ON test TYPE number VALUE $value * 2', items: 'DEFINE FIELD items[0] ON test TYPE number VALUE $value * 3', scores: 'DEFINE FIELD scores[*] ON test TYPE number ASSERT $value > 0', scores: 'DEFINE FIELD scores[0] ON test TYPE number ASSERT $value > 10', prices: 'DEFINE FIELD prices[*] ON test TYPE number VALUE $value * 1.1 ASSERT $value > 0', prices: 'DEFINE FIELD prices[0] ON test TYPE number VALUE $value * 1.2 ASSERT $value > 10' }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
error = "Invalid array element type for field 'nested[*]': array"

[[test.results]]
error = "Invalid array element type for field 'nested[0]': array"

[[test.results]]
value = "[{ id: test:test, items: [3, 4, 6], scores: [15, 15, 25], prices: [12, 22, 33] }]"

[[test.results]]
error = "Field value check failed for field 'scores[*]' on record 'test:test'"

[[test.results]]
error = "Field value check failed for field 'prices[*]' on record 'test:test'"

*/
DEFINE TABLE test SCHEMAFULL;

DEFINE FIELD items[*] ON test TYPE number VALUE $value * 2;
DEFINE FIELD items[0] ON test TYPE number VALUE $value * 3;

DEFINE FIELD scores[*] ON test TYPE number ASSERT $value > 0;
DEFINE FIELD scores[0] ON test TYPE number ASSERT $value > 10;

DEFINE FIELD prices[*] ON test TYPE number VALUE $value * 1.1 ASSERT $value > 0;
DEFINE FIELD prices[0] ON test TYPE number VALUE $value * 1.2 ASSERT $value > 10;

DEFINE FIELD nested[*] ON test TYPE array;
DEFINE FIELD nested[0] ON test TYPE array;

CREATE test SET items = [1, 2, 3], scores = [15, 15, 25], prices = [10, 20, 30];

SELECT * FROM test WHERE id = test:test;

CREATE test SET items = [1, 2, 3], scores = [-1, 15, 25], prices = [10, 20, 30];
CREATE test SET items = [1, 2, 3], scores = [5, 15, 25], prices = [0, 20, 30];