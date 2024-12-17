/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:int, number: 0 }]"

[[test.results]]
value = "[{ id: test:float, number: 0f }]"

[[test.results]]
value = "[{ id: test:float, number: 0f }, { id: test:int, number: 0 }]"

[[test.results]]
value = "[{ id: test:float, number: 0f }, { id: test:int, number: 0 }]"

[[test.results]]
value = "[{ id: test:float, number: 0f }, { id: test:int, number: 0 }]"

*/

DEFINE INDEX index ON TABLE test COLUMNS number;
CREATE test:int SET number = 0;
CREATE test:float SET number = 0.0;
-- TODO: CREATE test:dec_int SET number = 0dec;
-- TODO: CREATE test:dec_dec SET number = 0.0dec;
SELECT * FROM test WITH NOINDEX WHERE number = 0 ORDER BY id;
SELECT * FROM test WHERE number = 0 ORDER BY id;
SELECT * FROM test WHERE number = 0.0 ORDER BY id;
-- TODO: SELECT * FROM test WHERE number = 0dec ORDER BY id;
-- TODO: SELECT * FROM test WHERE number = 0.0dec ORDER BY id;
