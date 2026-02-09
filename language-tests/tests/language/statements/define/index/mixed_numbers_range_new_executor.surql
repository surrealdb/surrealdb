/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: test:float0, number: 0.5f }, { id: test:float1, number: 1.5f }, { id: test:int1, number: 1 }]"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> SortByKey [ctx: Db] [sort_keys: id ASC]\n       └────> Scan [ctx: Db] [source: test, predicate: number > 0 AND number < 2]\n''''

[[test.results]]
value = "[{ id: test:float0, number: 0.5f }, { id: test:float1, number: 1.5f }, { id: test:int1, number: 1 }]"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> SortByKey [ctx: Db] [sort_keys: id ASC]\n       └────> Scan [ctx: Db] [source: test, predicate: number > 0.1f AND number < 2]\n''''

[[test.results]]
value = "[{ id: test:float0, number: 0.5f }, { id: test:int1, number: 1 }]"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> SortByKey [ctx: Db] [sort_keys: id ASC]\n       └────> Scan [ctx: Db] [source: test, predicate: number > 0.1f AND number < 1.5f]\n''''

[[test.results]]
value = "[{ id: test:float0, number: 0.5f }, { id: test:int1, number: 1 }]"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> SortByKey [ctx: Db] [sort_keys: id ASC]\n       └────> Scan [ctx: Db] [source: test, predicate: number > 0 AND number < 1.5f]\n''''

[[test.results]]
value = "[{ id: test:float0, number: 0.5f }, { id: test:float1, number: 1.5f }, { id: test:int1, number: 1 }, { id: test:int2, number: 2 }]"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> SortByKey [ctx: Db] [sort_keys: id ASC]\n       └────> Scan [ctx: Db] [source: test, predicate: number > 0.1f]\n''''

[[test.results]]
value = "[{ id: test:float0, number: 0.5f }, { id: test:float1, number: 1.5f }, { id: test:int1, number: 1 }, { id: test:int2, number: 2 }]"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> SortByKey [ctx: Db] [sort_keys: id ASC]\n       └────> Scan [ctx: Db] [source: test, predicate: number > 0]\n''''

[[test.results]]
value = "[{ id: test:float0, number: 0.5f }, { id: test:float1, number: 1.5f }, { id: test:int0, number: 0 }, { id: test:int1, number: 1 }]"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> SortByKey [ctx: Db] [sort_keys: id ASC]\n       └────> Scan [ctx: Db] [source: test, predicate: number < 2]\n''''

[[test.results]]
value = "[{ id: test:float0, number: 0.5f }, { id: test:float1, number: 1.5f }, { id: test:int0, number: 0 }, { id: test:int1, number: 1 }]"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> SortByKey [ctx: Db] [sort_keys: id ASC]\n       └────> Scan [ctx: Db] [source: test, predicate: number < 1.9f]\n''''

*/
{
    DEFINE INDEX index ON TABLE test COLUMNS number;
    CREATE test:int0 SET number = 0;
    CREATE test:float0 SET number = 0.5;
    CREATE test:int1 SET number = 1;
    CREATE test:float1 SET number = 1.5;
    CREATE test:int2 SET number = 2;
    RETURN "OK";
};

SELECT * FROM test WHERE number > 0 AND number < 2 ORDER BY id;
EXPLAIN SELECT * FROM test WHERE number > 0 AND number < 2 ORDER BY id;

SELECT * FROM test WHERE number > 0.1 AND number < 2 ORDER BY id;
EXPLAIN SELECT * FROM test WHERE number > 0.1 AND number < 2 ORDER BY id;

SELECT * FROM test WHERE number > 0.1 AND number < 1.5 ORDER BY id;
EXPLAIN SELECT * FROM test WHERE number > 0.1 AND number < 1.5 ORDER BY id;

SELECT * FROM test WHERE number > 0 AND number < 1.5 ORDER BY id;
EXPLAIN SELECT * FROM test WHERE number > 0 AND number < 1.5 ORDER BY id;

SELECT * FROM test WHERE number > 0.1 ORDER BY id;
EXPLAIN SELECT * FROM test WHERE number > 0.1 ORDER BY id;

SELECT * FROM test WHERE number > 0 ORDER BY id;
EXPLAIN SELECT * FROM test WHERE number > 0 ORDER BY id;

SELECT * FROM test WHERE number < 2 ORDER BY id;
EXPLAIN SELECT * FROM test WHERE number < 2 ORDER BY id;

SELECT * FROM test WHERE number < 1.9 ORDER BY id;
EXPLAIN SELECT * FROM test WHERE number < 1.9 ORDER BY id;
