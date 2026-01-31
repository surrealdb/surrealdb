/**
[test]
reason = "Test FETCH with GROUP BY clause"

# 0: Setup
[[test.results]]
value = "'OK'"

# 1, 2: GROUP ALL
[[test.results]]
value = """[{ age: [25, 30], city: [city:2, city:1], name: ['Bob', 'Alice'] }]"""
[[test.results]]
value = """[{ age: [25, 30], city: [{ country: 'USA', id: city:2, name: 'New York' }, { country: 'USA', id: city:1, name: 'San Francisco' }], name: ['Bob', 'Alice'] }]]"""

# 3, 4: GROUP BY
[[test.results]]
value = """[{ avg_age: 30f, city: city:1, name: ['Alice'] }, { avg_age: 25f, city: city:2, name: ['Bob'] }]"""
[[test.results]]
value = """[{ avg_age: 30f, city: { country: 'USA', id: city:1, name: 'San Francisco' }, name: ['Alice'] }, { avg_age: 25f, city: { country: 'USA', id: city:2, name: 'New York' }, name: ['Bob'] }]"""

*/


-- 0: Setup
{
    CREATE user:1 SET name = 'Bob', age = 25, city = city:2;
    CREATE user:2 SET name = 'Alice', age = 30, city = city:1;
    CREATE city:1 SET name = 'San Francisco', country = 'USA';
    CREATE city:2 SET name = 'New York', country = 'USA';
    RETURN "OK";
};

-- 1, 2: GROUP ALL
SELECT name, age, city FROM user GROUP ALL;
SELECT name, age, city FROM user GROUP ALL FETCH city;

-- 3, 4: GROUP BY
SELECT name, math::mean(age) as avg_age, city FROM user GROUP BY city;
SELECT name, math::mean(age) as avg_age, city FROM user GROUP BY city FETCH city;
