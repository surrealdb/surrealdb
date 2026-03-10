/**
[test]
reason = "Test FETCH clause combined with other SELECT clauses like WHERE, ORDER BY, LIMIT, START, OMIT, ONLY"

# 0: Setup
[[test.results]]
value = "'OK'"

# 1: ORDER BY ASC
[[test.results]]
value = """[{ age: 30, city: { country: 'USA', id: city:1, name: 'San Francisco' }, id: user:2, name: 'Alice' }, { age: 25, city: { country: 'USA', id: city:2, name: 'New York' }, id: user:1, name: 'Bob' }]"""

# 2: ORDER BY DESC
[[test.results]]
value = """[{ age: 25, city: { country: 'USA', id: city:2, name: 'New York' }, id: user:1, name: 'Bob' }, { age: 30, city: { country: 'USA', id: city:1, name: 'San Francisco' }, id: user:2, name: 'Alice' }]"""

# 3: ORDER BY city.name ASC
[[test.results]]
value = """[{ age: 25, city: { country: 'USA', id: city:2, name: 'New York' }, id: user:1, name: 'Bob' }, { age: 30, city: { country: 'USA', id: city:1, name: 'San Francisco' }, id: user:2, name: 'Alice' }]"""

# 4: ORDER BY city.name DESC
[[test.results]]
value = """[{ age: 25, city: { country: 'USA', id: city:2, name: 'New York' }, id: user:1, name: 'Bob' }, { age: 30, city: { country: 'USA', id: city:1, name: 'San Francisco' }, id: user:2, name: 'Alice' }]"""
*/

-- 0: Setup
{
    CREATE user:1 SET name = 'Bob', age = 25, city = city:2;
    CREATE user:2 SET name = 'Alice', age = 30, city = city:1;
    CREATE city:1 SET name = 'San Francisco', country = 'USA';
    CREATE city:2 SET name = 'New York', country = 'USA';
    RETURN "OK";
};

-- 1: ORDER BY ASC -- this orders by the city record ID, not the name because FETCH occurs after the ORDER BY.
SELECT * FROM user ORDER BY city ASC FETCH city;

-- 2: ORDER BY DESC -- this orders by the city record ID, not the name because FETCH occurs after the ORDER BY.
SELECT * FROM user ORDER BY city DESC FETCH city;

-- 3, 4: ORDER BY city.name ASC/DESC -- this should try to order by the city.name field, but since it's only in the FETCH clause, it should be sorting
-- on the value NONE because FETCH occurs after the ORDER BY.
SELECT * FROM user ORDER BY city.name ASC FETCH city;
SELECT * FROM user ORDER BY city.name DESC FETCH city;
