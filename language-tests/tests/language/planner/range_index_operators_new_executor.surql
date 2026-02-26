/**
[test]
reason = "Test all range operator combinations (inclusive/exclusive) with standard and unique indexes (new executor)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:0, year: 2000 }]"

[[test.results]]
value = "[{ id: test:10, year: 2010 }]"

[[test.results]]
value = "[{ id: test:15, year: 2015 }]"

[[test.results]]
value = "[{ id: test:16, year: 2015 }]"

[[test.results]]
value = "[{ id: test:20, year: 2020 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id] {rows: 3}
    IndexScan [ctx: Db] [index: year, access: >2000 <2020, direction: Forward] {rows: 3}

Total rows: 3''''

[[test.results]]
value = "[{ id: test:10 }, { id: test:15 }, { id: test:16 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id] {rows: 4}
    IndexScan [ctx: Db] [index: year, access: >=2000 <2020, direction: Forward] {rows: 4}

Total rows: 4''''

[[test.results]]
value = "[{ id: test:0 }, { id: test:10 }, { id: test:15 }, { id: test:16 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id] {rows: 4}
    IndexScan [ctx: Db] [index: year, access: >2000 <=2020, direction: Forward] {rows: 4}

Total rows: 4''''

[[test.results]]
value = "[{ id: test:10 }, { id: test:15 }, { id: test:16 }, { id: test:20 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id] {rows: 5}
    IndexScan [ctx: Db] [index: year, access: >=2000 <=2020, direction: Forward] {rows: 5}

Total rows: 5''''

[[test.results]]
value = "[{ id: test:0 }, { id: test:10 }, { id: test:15 }, { id: test:16 }, { id: test:20 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id] {rows: 2}
    IndexScan [ctx: Db] [index: year, access: <2015, direction: Forward] {rows: 2}

Total rows: 2''''

[[test.results]]
value = "[{ id: test:0 }, { id: test:10 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id] {rows: 4}
    IndexScan [ctx: Db] [index: year, access: <=2015, direction: Forward] {rows: 4}

Total rows: 4''''

[[test.results]]
value = "[{ id: test:0 }, { id: test:10 }, { id: test:15 }, { id: test:16 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id] {rows: 1}
    IndexScan [ctx: Db] [index: year, access: >2015, direction: Forward] {rows: 1}

Total rows: 1''''

[[test.results]]
value = "[{ id: test:20 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id] {rows: 3}
    IndexScan [ctx: Db] [index: year, access: >=2015, direction: Forward] {rows: 3}

Total rows: 3''''

[[test.results]]
value = "[{ id: test:15 }, { id: test:16 }, { id: test:20 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:0, year: 2000 }]"

[[test.results]]
value = "[{ id: test:10, year: 2010 }]"

[[test.results]]
value = "[{ id: test:15, year: 2015 }]"

[[test.results]]
value = "[{ id: test:16, year: 2016 }]"

[[test.results]]
value = "[{ id: test:20, year: 2020 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id] {rows: 3}
    IndexScan [ctx: Db] [index: year, access: >2000 <2020, direction: Forward] {rows: 3}

Total rows: 3''''

[[test.results]]
value = "[{ id: test:10 }, { id: test:15 }, { id: test:16 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id] {rows: 4}
    IndexScan [ctx: Db] [index: year, access: >=2000 <2020, direction: Forward] {rows: 4}

Total rows: 4''''

[[test.results]]
value = "[{ id: test:0 }, { id: test:10 }, { id: test:15 }, { id: test:16 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id] {rows: 4}
    IndexScan [ctx: Db] [index: year, access: >2000 <=2020, direction: Forward] {rows: 4}

Total rows: 4''''

[[test.results]]
value = "[{ id: test:10 }, { id: test:15 }, { id: test:16 }, { id: test:20 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id] {rows: 5}
    IndexScan [ctx: Db] [index: year, access: >=2000 <=2020, direction: Forward] {rows: 5}

Total rows: 5''''

[[test.results]]
value = "[{ id: test:0 }, { id: test:10 }, { id: test:15 }, { id: test:16 }, { id: test:20 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id] {rows: 2}
    IndexScan [ctx: Db] [index: year, access: <2015, direction: Forward] {rows: 2}

Total rows: 2''''

[[test.results]]
value = "[{ id: test:0 }, { id: test:10 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id] {rows: 3}
    IndexScan [ctx: Db] [index: year, access: <=2015, direction: Forward] {rows: 3}

Total rows: 3''''

[[test.results]]
value = "[{ id: test:0 }, { id: test:10 }, { id: test:15 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id] {rows: 2}
    IndexScan [ctx: Db] [index: year, access: >2015, direction: Forward] {rows: 2}

Total rows: 2''''

[[test.results]]
value = "[{ id: test:16 }, { id: test:20 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: id] {rows: 3}
    IndexScan [ctx: Db] [index: year, access: >=2015, direction: Forward] {rows: 3}

Total rows: 3''''

[[test.results]]
value = "[{ id: test:15 }, { id: test:16 }, { id: test:20 }]"
*/

-- Standard index range tests
DEFINE INDEX year ON TABLE test COLUMNS year;
CREATE test:0 SET year = 2000;
CREATE test:10 SET year = 2010;
CREATE test:15 SET year = 2015;
CREATE test:16 SET year = 2015;
CREATE test:20 SET year = 2020;

-- Exclusive from, exclusive to
EXPLAIN ANALYZE SELECT id FROM test WHERE year > 2000 AND year < 2020;
SELECT id FROM test WHERE year > 2000 AND year < 2020;
-- Inclusive from, exclusive to
EXPLAIN ANALYZE SELECT id FROM test WHERE year >= 2000 AND year < 2020;
SELECT id FROM test WHERE year >= 2000 AND year < 2020;
-- Exclusive from, inclusive to
EXPLAIN ANALYZE SELECT id FROM test WHERE year > 2000 AND year <= 2020;
SELECT id FROM test WHERE year > 2000 AND year <= 2020;
-- Inclusive from, inclusive to
EXPLAIN ANALYZE SELECT id FROM test WHERE year >= 2000 AND year <= 2020;
SELECT id FROM test WHERE year >= 2000 AND year <= 2020;

-- Single operators
EXPLAIN ANALYZE SELECT id FROM test WHERE year < 2015;
SELECT id FROM test WHERE year < 2015;

EXPLAIN ANALYZE SELECT id FROM test WHERE year <= 2015;
SELECT id FROM test WHERE year <= 2015;

EXPLAIN ANALYZE SELECT id FROM test WHERE year > 2015;
SELECT id FROM test WHERE year > 2015;

EXPLAIN ANALYZE SELECT id FROM test WHERE year >= 2015;
SELECT id FROM test WHERE year >= 2015;

-- Unique index range tests
REMOVE TABLE test;
DEFINE INDEX year ON TABLE test COLUMNS year UNIQUE;
CREATE test:0 SET year = 2000;
CREATE test:10 SET year = 2010;
CREATE test:15 SET year = 2015;

CREATE test:16 SET year = 2016;
CREATE test:20 SET year = 2020;

EXPLAIN ANALYZE SELECT id FROM test WHERE year > 2000 AND year < 2020;
SELECT id FROM test WHERE year > 2000 AND year < 2020;
EXPLAIN ANALYZE SELECT id FROM test WHERE year >= 2000 AND year < 2020;

SELECT id FROM test WHERE year >= 2000 AND year < 2020;
EXPLAIN ANALYZE SELECT id FROM test WHERE year > 2000 AND year <= 2020;
SELECT id FROM test WHERE year > 2000 AND year <= 2020;
EXPLAIN ANALYZE SELECT id FROM test WHERE year >= 2000 AND year <= 2020;
SELECT id FROM test WHERE year >= 2000 AND year <= 2020;

EXPLAIN ANALYZE SELECT id FROM test WHERE year < 2015;
SELECT id FROM test WHERE year < 2015;
EXPLAIN ANALYZE SELECT id FROM test WHERE year <= 2015;
SELECT id FROM test WHERE year <= 2015;
EXPLAIN ANALYZE SELECT id FROM test WHERE year > 2015;
SELECT id FROM test WHERE year > 2015;

EXPLAIN ANALYZE SELECT id FROM test WHERE year >= 2015;
SELECT id FROM test WHERE year >= 2015;
