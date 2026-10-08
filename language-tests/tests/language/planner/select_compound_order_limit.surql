/**
[env]

[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[5, 4, 3, 2, 1]"

[[test.results]]
value = "[1, 2, 3, 4, 5]"

[[test.results]]
value = "[5]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[4]"

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
(SELECT * FROM test WHERE k1 = 'key1' AND k2="key2" ORDER BY i DESC).i;
(SELECT * FROM test WHERE k1 = 'key1' AND k2="key2" ORDER BY i ASC).i;
(SELECT * FROM test WHERE k1 = 'key1' and k2="key2" ORDER BY i DESC LIMIT 1).i;
(SELECT * FROM test WHERE k1 = 'key1' and k2="key2" ORDER BY i ASC LIMIT 1).i;
(SELECT * FROM test WHERE k1 = 'key1' and k2="key2" ORDER BY i DESC START 1 LIMIT 1).i;
(SELECT * FROM test WHERE k1 = 'key1' and k2="key2" ORDER BY i ASC START 1 LIMIT 1).i;
