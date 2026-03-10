/**
[test]
reason = "Test FETCH clause with objects containing record IDs and nested object structures"

# 0: Setup
[[test.results]]
value = "'OK'"

# 1: Basic FETCH on object field
[[test.results]]
value = """[{ id: user:1, metadata: { age: 25, city: city:new_york }, name: 'Bob' }]"""

# 2: Nested FETCH on object field
[[test.results]]
value = """[{ id: user:1, metadata: { age: 25, city: { id: city:new_york, name: 'New York', country: 'USA' } }, name: 'Bob' }]"""
*/

-- 0: Setup
{
    CREATE user:1 SET name = 'Bob', metadata = { age: 25, city: city:new_york }
    CREATE city:new_york SET name = 'New York', country = 'USA';
    RETURN "OK";
};

-- 1: Basic FETCH on object field
SELECT * FROM user:1;

-- 2: Nested FETCH on object field
SELECT * FROM user:1 FETCH metadata.city;
