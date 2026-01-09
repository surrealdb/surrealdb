/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:int, number: 0 }]"

[[test.results]]
error = "Database index `index` already contains 0f, with record `test:int`"

[[test.results]]
error = "Database index `index` already contains 0dec, with record `test:int`"

[[test.results]]
error = "Database index `index` already contains 0dec, with record `test:int`"

[[test.results]]
value = "[{ id: test:int, number: 0 }]"

[[test.results]]
value = "[{ id: test:int, number: 0 }]"

[[test.results]]
value = "[{ id: test:int, number: 0 }]"

[[test.results]]
value = "[{ id: test:int, number: 0 }]"

[[test.results]]
value = "[{ id: test:int, number: 0 }]"

*/

DEFINE INDEX index ON TABLE test COLUMNS number UNIQUE;
CREATE test:int SET number = 0;
CREATE test:float SET number = 0.0;
CREATE test:dec_int SET number = 0dec;
CREATE test:dec_dec SET number = 0.0dec;
SELECT * FROM test WITH NOINDEX WHERE number = 0 ORDER BY id;
SELECT * FROM test WHERE number = 0 ORDER BY id;
SELECT * FROM test WHERE number = 0.0 ORDER BY id;
SELECT * FROM test WHERE number = 0dec ORDER BY id;
SELECT * FROM test WHERE number = 0.0dec ORDER BY id;
