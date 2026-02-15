/**
[test]
reason = "Test composite index with equality prefix and range on next column"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ age: 30, city: 'london', id: record:1 }]"

[[test.results]]
value = "[{ age: 55, city: 'london', id: record:2 }]"

[[test.results]]
value = "[{ age: 70, city: 'london', id: record:3 }]"

[[test.results]]
value = "[{ age: 60, city: 'paris', id: record:4 }]"

[[test.results]]
value = "[{ age: 40, city: 'paris', id: record:5 }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2}
    IndexScan [ctx: Db] [index: idx_city_age, access: ['london'] MoreThan 50, direction: Forward] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ age: 55, city: 'london', id: record:2 }, { age: 70, city: 'london', id: record:3 }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2}
    IndexScan [ctx: Db] [index: idx_city_age, access: ['london'] MoreThanEqual 55, direction: Forward] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ age: 55, city: 'london', id: record:2 }, { age: 70, city: 'london', id: record:3 }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1}
    IndexScan [ctx: Db] [index: idx_city_age, access: ['paris'] LessThan 50, direction: Forward] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ age: 40, city: 'paris', id: record:5 }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 2}
    IndexScan [ctx: Db] [index: idx_city_age, access: ['paris'] LessThanEqual 60, direction: Forward] {rows: 2}

Total rows: 2"'''

[[test.results]]
value = "[{ age: 40, city: 'paris', id: record:5 }, { age: 60, city: 'paris', id: record:4 }]"

*/

-- Composite index on (city, age)
DEFINE INDEX idx_city_age ON TABLE record COLUMNS city, age;
CREATE record:1 SET city = 'london', age = 30;
CREATE record:2 SET city = 'london', age = 55;
CREATE record:3 SET city = 'london', age = 70;
CREATE record:4 SET city = 'paris', age = 60;
CREATE record:5 SET city = 'paris', age = 40;

-- Equality on first column + range (>) on second column
-- Should use compound index with range narrowing, not just prefix scan
EXPLAIN ANALYZE SELECT * FROM record WHERE city = 'london' AND age > 50;
SELECT * FROM record WHERE city = 'london' AND age > 50;

-- Equality on first column + range (>=) on second column
EXPLAIN ANALYZE SELECT * FROM record WHERE city = 'london' AND age >= 55;
SELECT * FROM record WHERE city = 'london' AND age >= 55;

-- Equality on first column + range (<) on second column
EXPLAIN ANALYZE SELECT * FROM record WHERE city = 'paris' AND age < 50;
SELECT * FROM record WHERE city = 'paris' AND age < 50;

-- Equality on first column + range (<=) on second column
EXPLAIN ANALYZE SELECT * FROM record WHERE city = 'paris' AND age <= 60;
SELECT * FROM record WHERE city = 'paris' AND age <= 60;
