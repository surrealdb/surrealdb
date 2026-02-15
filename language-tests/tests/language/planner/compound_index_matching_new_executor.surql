/**
[test]
reason = "Test compound index selection with partial and full key matching (new executor)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ event: 'dummy', id: event:0, name: 'dummy', path: 'D/U/M' }]"

[[test.results]]
value = "[{ event: 'event', id: event:1, name: 'someName', path: 'A/B/C' }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1}
    Filter [ctx: Db] [predicate: name = 'someName' AND event = 'event' AND path = 'A/B/C'] {rows: 1}
        IndexScan [ctx: Db] [index: idxNameEvent, access: ['someName', 'event'], direction: Forward] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1}
    Filter [ctx: Db] [predicate: name = 'someName' AND event = 'event' AND path = 'A/B/C'] {rows: 1}
        IndexScan [ctx: Db] [index: idxNameEventPath, access: ['someName', 'event', 'A/B/C'], direction: Forward] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = '''"Project [ctx: Db] {rows: 1}
    Filter [ctx: Db] [predicate: name = 'someName' AND event = 'event' AND path = 'A/B/C'] {rows: 1}
        IndexScan [ctx: Db] [index: idxNameEventPath, access: ['someName', 'event', 'A/B/C'], direction: Forward] {rows: 1}

Total rows: 1"'''

[[test.results]]
value = "[{ event: 'event', id: event:1, name: 'someName', path: 'A/B/C' }]"

[[test.results]]
value = "[{ event: 'event', id: event:1, name: 'someName', path: 'A/B/C' }]"

[[test.results]]
value = "[{ event: 'event', id: event:1, name: 'someName', path: 'A/B/C' }]"
*/

DEFINE INDEX idxNameEvent ON event FIELDS name, event;
DEFINE INDEX idxNameEventPath ON event FIELDS name, event, path;
CREATE event:0 SET name = 'dummy', event = 'dummy', path = 'D/U/M';
CREATE event:1 SET name = 'someName', event = 'event', path = 'A/B/C';

-- Force two-field compound index
EXPLAIN ANALYZE SELECT * FROM event WITH INDEX idxNameEvent WHERE name = 'someName' AND event = 'event' AND path = 'A/B/C';
-- Force three-field compound index
EXPLAIN ANALYZE SELECT * FROM event WITH INDEX idxNameEventPath WHERE name = 'someName' AND event = 'event' AND path = 'A/B/C';
-- Auto-select best compound index
EXPLAIN ANALYZE SELECT * FROM event WHERE name = 'someName' AND event = 'event' AND path = 'A/B/C';

-- Actual data results
SELECT * FROM event WITH INDEX idxNameEvent WHERE name = 'someName' AND event = 'event' AND path = 'A/B/C';
SELECT * FROM event WITH INDEX idxNameEventPath WHERE name = 'someName' AND event = 'event' AND path = 'A/B/C';
SELECT * FROM event WHERE name = 'someName' AND event = 'event' AND path = 'A/B/C';
