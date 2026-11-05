/**
[env]

[test]
reason = "Test compound index selection with partial and full key matching"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ event: 'dummy', id: event:0, name: 'dummy', path: 'D/U/M' }]"

[[test.results]]
value = "[{ event: 'event', id: event:1, name: 'someName', path: 'A/B/C' }]"

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

-- Actual data results
SELECT * FROM event WITH INDEX idxNameEvent WHERE name = 'someName' AND event = 'event' AND path = 'A/B/C';
SELECT * FROM event WITH INDEX idxNameEventPath WHERE name = 'someName' AND event = 'event' AND path = 'A/B/C';
SELECT * FROM event WHERE name = 'someName' AND event = 'event' AND path = 'A/B/C';
