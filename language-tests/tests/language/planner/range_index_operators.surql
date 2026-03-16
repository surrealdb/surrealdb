/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test all range operator combinations (inclusive/exclusive) with standard and unique indexes"

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
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: false, value: 2000 }, index: 'year', to: { inclusive: false, value: 2020 } }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: test:10 }, { id: test:15 }, { id: test:16 }]"

[[test.results]]
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: true, value: 2000 }, index: 'year', to: { inclusive: false, value: 2020 } }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: test:0 }, { id: test:10 }, { id: test:15 }, { id: test:16 }]"

[[test.results]]
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: false, value: 2000 }, index: 'year', to: { inclusive: true, value: 2020 } }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: test:10 }, { id: test:15 }, { id: test:16 }, { id: test:20 }]"

[[test.results]]
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: true, value: 2000 }, index: 'year', to: { inclusive: true, value: 2020 } }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: test:0 }, { id: test:10 }, { id: test:15 }, { id: test:16 }, { id: test:20 }]"

[[test.results]]
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: false, value: NONE }, index: 'year', to: { inclusive: false, value: 2015 } }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: test:0 }, { id: test:10 }]"

[[test.results]]
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: false, value: NONE }, index: 'year', to: { inclusive: true, value: 2015 } }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: test:0 }, { id: test:10 }, { id: test:15 }, { id: test:16 }]"

[[test.results]]
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: false, value: 2015 }, index: 'year', to: { inclusive: false, value: NONE } }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: test:20 }]"

[[test.results]]
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: true, value: 2015 }, index: 'year', to: { inclusive: false, value: NONE } }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

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
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: false, value: 2000 }, index: 'year', to: { inclusive: false, value: 2020 } }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: test:10 }, { id: test:15 }, { id: test:16 }]"

[[test.results]]
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: true, value: 2000 }, index: 'year', to: { inclusive: false, value: 2020 } }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: test:0 }, { id: test:10 }, { id: test:15 }, { id: test:16 }]"

[[test.results]]
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: false, value: 2000 }, index: 'year', to: { inclusive: true, value: 2020 } }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: test:10 }, { id: test:15 }, { id: test:16 }, { id: test:20 }]"

[[test.results]]
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: true, value: 2000 }, index: 'year', to: { inclusive: true, value: 2020 } }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: test:0 }, { id: test:10 }, { id: test:15 }, { id: test:16 }, { id: test:20 }]"

[[test.results]]
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: false, value: NONE }, index: 'year', to: { inclusive: false, value: 2015 } }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: test:0 }, { id: test:10 }]"

[[test.results]]
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: false, value: NONE }, index: 'year', to: { inclusive: true, value: 2015 } }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: test:0 }, { id: test:10 }, { id: test:15 }]"

[[test.results]]
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: false, value: 2015 }, index: 'year', to: { inclusive: false, value: NONE } }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: test:16 }, { id: test:20 }]"

[[test.results]]
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: true, value: 2015 }, index: 'year', to: { inclusive: false, value: NONE } }, table: 'test' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

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
SELECT id FROM test WHERE year > 2000 AND year < 2020 EXPLAIN;
SELECT id FROM test WHERE year > 2000 AND year < 2020;
-- Inclusive from, exclusive to
SELECT id FROM test WHERE year >= 2000 AND year < 2020 EXPLAIN;
SELECT id FROM test WHERE year >= 2000 AND year < 2020;
-- Exclusive from, inclusive to
SELECT id FROM test WHERE year > 2000 AND year <= 2020 EXPLAIN;
SELECT id FROM test WHERE year > 2000 AND year <= 2020;
-- Inclusive from, inclusive to
SELECT id FROM test WHERE year >= 2000 AND year <= 2020 EXPLAIN;
SELECT id FROM test WHERE year >= 2000 AND year <= 2020;

-- Single operators
SELECT id FROM test WHERE year < 2015 EXPLAIN;
SELECT id FROM test WHERE year < 2015;
SELECT id FROM test WHERE year <= 2015 EXPLAIN;
SELECT id FROM test WHERE year <= 2015;
SELECT id FROM test WHERE year > 2015 EXPLAIN;
SELECT id FROM test WHERE year > 2015;
SELECT id FROM test WHERE year >= 2015 EXPLAIN;
SELECT id FROM test WHERE year >= 2015;

-- Unique index range tests
REMOVE TABLE test;
DEFINE INDEX year ON TABLE test COLUMNS year UNIQUE;
CREATE test:0 SET year = 2000;
CREATE test:10 SET year = 2010;
CREATE test:15 SET year = 2015;
CREATE test:16 SET year = 2016;
CREATE test:20 SET year = 2020;

SELECT id FROM test WHERE year > 2000 AND year < 2020 EXPLAIN;
SELECT id FROM test WHERE year > 2000 AND year < 2020;
SELECT id FROM test WHERE year >= 2000 AND year < 2020 EXPLAIN;
SELECT id FROM test WHERE year >= 2000 AND year < 2020;
SELECT id FROM test WHERE year > 2000 AND year <= 2020 EXPLAIN;
SELECT id FROM test WHERE year > 2000 AND year <= 2020;
SELECT id FROM test WHERE year >= 2000 AND year <= 2020 EXPLAIN;
SELECT id FROM test WHERE year >= 2000 AND year <= 2020;

SELECT id FROM test WHERE year < 2015 EXPLAIN;
SELECT id FROM test WHERE year < 2015;
SELECT id FROM test WHERE year <= 2015 EXPLAIN;
SELECT id FROM test WHERE year <= 2015;
SELECT id FROM test WHERE year > 2015 EXPLAIN;
SELECT id FROM test WHERE year > 2015;
SELECT id FROM test WHERE year >= 2015 EXPLAIN;
SELECT id FROM test WHERE year >= 2015;
