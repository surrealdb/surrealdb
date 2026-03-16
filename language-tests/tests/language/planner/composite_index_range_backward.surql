/**
[test]
reason = "Test composite index with equality prefix and range condition in backward (DESC) direction"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 70, city: 'london', id: record:3 }, { age: 55, city: 'london', id: record:2 }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: *] {rows: 2}
    IndexScan [ctx: Db] [index: idx_city_age, access: ['london'] MoreThan 50, direction: Backward] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ age: 70, city: 'london', id: record:3 }, { age: 55, city: 'london', id: record:2 }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: *] {rows: 2}
    IndexScan [ctx: Db] [index: idx_city_age, access: ['london'] MoreThanEqual 55, direction: Backward] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ age: 40, city: 'paris', id: record:5 }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: *] {rows: 1}
    IndexScan [ctx: Db] [index: idx_city_age, access: ['paris'] LessThan 50, direction: Backward] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ age: 60, city: 'paris', id: record:4 }, { age: 40, city: 'paris', id: record:5 }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: *] {rows: 2}
    IndexScan [ctx: Db] [index: idx_city_age, access: ['paris'] LessThanEqual 60, direction: Backward] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ age: 70, city: 'london', id: record:3 }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: *] {rows: 1}
    IndexScan [ctx: Db] [index: idx_city_age, access: ['london'] MoreThan 50, direction: Backward, limit: 1] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ age: 60, city: 'paris', id: record:4 }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: *] {rows: 1}
    IndexScan [ctx: Db] [index: idx_city_age, access: ['paris'] LessThanEqual 60, direction: Backward, limit: 1] {rows: 1}

Total rows: 1"'''
*/

-- Composite index on (city, age)
{
    DEFINE INDEX idx_city_age ON TABLE record COLUMNS city, age;
    CREATE record:1 SET city = 'london', age = 30;
    CREATE record:2 SET city = 'london', age = 55;
    CREATE record:3 SET city = 'london', age = 70;
    CREATE record:4 SET city = 'paris', age = 60;
    CREATE record:5 SET city = 'paris', age = 40;
    RETURN NONE;
};

-- Equality + range (>) ORDER BY DESC
SELECT * FROM record WHERE city = 'london' AND age > 50 ORDER BY age DESC;
EXPLAIN ANALYZE SELECT * FROM record WHERE city = 'london' AND age > 50 ORDER BY age DESC;

-- Equality + range (>=) ORDER BY DESC
SELECT * FROM record WHERE city = 'london' AND age >= 55 ORDER BY age DESC;
EXPLAIN ANALYZE SELECT * FROM record WHERE city = 'london' AND age >= 55 ORDER BY age DESC;

-- Equality + range (<) ORDER BY DESC
SELECT * FROM record WHERE city = 'paris' AND age < 50 ORDER BY age DESC;
EXPLAIN ANALYZE SELECT * FROM record WHERE city = 'paris' AND age < 50 ORDER BY age DESC;

-- Equality + range (<=) ORDER BY DESC
SELECT * FROM record WHERE city = 'paris' AND age <= 60 ORDER BY age DESC;
EXPLAIN ANALYZE SELECT * FROM record WHERE city = 'paris' AND age <= 60 ORDER BY age DESC;

-- Equality + range (>) ORDER BY DESC LIMIT 1
SELECT * FROM record WHERE city = 'london' AND age > 50 ORDER BY age DESC LIMIT 1;
EXPLAIN ANALYZE SELECT * FROM record WHERE city = 'london' AND age > 50 ORDER BY age DESC LIMIT 1;

-- Equality + range (<=) ORDER BY DESC LIMIT 1
SELECT * FROM record WHERE city = 'paris' AND age <= 60 ORDER BY age DESC LIMIT 1;
EXPLAIN ANALYZE SELECT * FROM record WHERE city = 'paris' AND age <= 60 ORDER BY age DESC LIMIT 1;
