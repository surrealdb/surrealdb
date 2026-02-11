/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''
"Project [ctx: Db]
└────> SortByKey [ctx: Db] [sort_keys: i DESC]
       └────> Scan [ctx: Db] [source: test, predicate: k1 = 'key1' AND k2 = 'key2']
"'''

[[test.results]]
value = "[5, 4, 3, 2, 1]"

[[test.results]]
value = '''
"Project [ctx: Db]
└────> SortByKey [ctx: Db] [sort_keys: i ASC]
       └────> Scan [ctx: Db] [source: test, predicate: k1 = 'key1' AND k2 = 'key2']
"'''

[[test.results]]
value = "[1, 2, 3, 4, 5]"

[[test.results]]
value = '''
"Project [ctx: Db]
└────> Limit [ctx: Db] [limit: 1]
       └────> SortByKey [ctx: Db] [sort_keys: i DESC]
              └────> Scan [ctx: Db] [source: test, predicate: k1 = 'key1' AND k2 = 'key2']
"'''

[[test.results]]
value = "[5]"

[[test.results]]
value = '''
"Project [ctx: Db]
└────> Limit [ctx: Db] [limit: 1]
       └────> SortByKey [ctx: Db] [sort_keys: i ASC]
              └────> Scan [ctx: Db] [source: test, predicate: k1 = 'key1' AND k2 = 'key2']
"'''

[[test.results]]
value = "[1]"

[[test.results]]
value = '''
"Project [ctx: Db]
└────> Limit [ctx: Db] [limit: 1, offset: 1]
       └────> SortByKey [ctx: Db] [sort_keys: i DESC]
              └────> Scan [ctx: Db] [source: test, predicate: k1 = 'key1' AND k2 = 'key2']
"'''

[[test.results]]
value = "[4]"

[[test.results]]
value = '''
"Project [ctx: Db]
└────> Limit [ctx: Db] [limit: 1, offset: 1]
       └────> SortByKey [ctx: Db] [sort_keys: i ASC]
              └────> Scan [ctx: Db] [source: test, predicate: k1 = 'key1' AND k2 = 'key2']
"'''

[[test.results]]
value = "[2]"

*/

{
    DEFINE INDEX idx_k1_k2 ON TABLE test COLUMNS k1,k2;
    FOR $i in 1..6 {
    CREATE test CONTENT {
            i: $i,
            k1: 'key1',
            k2: 'key2'
        };
    };
    RETURN "OK";
};

EXPLAIN SELECT * FROM test WHERE k1 = 'key1' AND k2="key2" ORDER BY i DESC;
(SELECT * FROM test WHERE k1 = 'key1' AND k2="key2" ORDER BY i DESC).i;

EXPLAIN SELECT * FROM test WHERE k1 = 'key1' AND k2="key2" ORDER BY i ASC;
(SELECT * FROM test WHERE k1 = 'key1' AND k2="key2" ORDER BY i ASC).i;

EXPLAIN SELECT * FROM test WHERE k1 = 'key1' and k2="key2" ORDER BY i DESC LIMIT 1;
(SELECT * FROM test WHERE k1 = 'key1' and k2="key2" ORDER BY i DESC LIMIT 1).i;

EXPLAIN SELECT * FROM test WHERE k1 = 'key1' and k2="key2" ORDER BY i ASC LIMIT 1;
(SELECT * FROM test WHERE k1 = 'key1' and k2="key2" ORDER BY i ASC LIMIT 1).i;

EXPLAIN SELECT * FROM test WHERE k1 = 'key1' and k2="key2" ORDER BY i DESC START 1 LIMIT 1;
(SELECT * FROM test WHERE k1 = 'key1' and k2="key2" ORDER BY i DESC START 1 LIMIT 1).i;

EXPLAIN SELECT * FROM test WHERE k1 = 'key1' and k2="key2" ORDER BY i ASC START 1 LIMIT 1;
(SELECT * FROM test WHERE k1 = 'key1' and k2="key2" ORDER BY i ASC START 1 LIMIT 1).i;
