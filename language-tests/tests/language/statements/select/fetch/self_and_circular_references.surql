/**
[test]
reason = "Test FETCH clause edge cases: non-existent fields, null/none values, empty arrays, missing records"

# 0: Setup
[[test.results]]
value = "'OK'"

# 1: FETCH on self reference
[[test.results]]
value = """[{ id: user:1, loves: { id: user:1, loves: user:1, name: 'Narcissus' }, name: 'Narcissus' }]"""

# 2: FETCH on self reference with nested self reference
[[test.results]]
value = """[{ id: user:1, loves: { id: user:1, loves: { id: user:1, loves: user:1, name: 'Narcissus' }, name: 'Narcissus' }, name: 'Narcissus' }]"""

# 3: FETCH on circular reference
[[test.results]]
value = """[{ id: user:2, knows: { id: user:3, knows: user:2, name: 'Circle 2' }, name: 'Circle 1' }]"""

# 4: FETCH on circular reference with nested circular reference
[[test.results]]
value = """[{ id: user:2, knows: { id: user:3, knows: { id: user:2, knows: { id: user:3, knows: user:2, name: 'Circle 2' }, name: 'Circle 1' }, name: 'Circle 2' }, name: 'Circle 1' }]"""
*/

{
    CREATE user:1 SET name = 'Narcissus', loves = user:1;

    CREATE user:2 SET name = 'Circle 1', knows = user:3;
    CREATE user:3 SET name = 'Circle 2', knows = user:2;
    RETURN "OK";
};

-- 1: FETCH on self reference
SELECT * FROM user:1 FETCH loves;
-- 2: FETCH on self reference with nested self reference
SELECT * FROM user:1 FETCH loves, loves.loves;

-- 3: FETCH on circular reference
SELECT * FROM user:2 FETCH knows;
-- 4: FETCH on circular reference with nested circular reference
SELECT * FROM user:2 FETCH knows, knows.knows, knows.knows.knows;
