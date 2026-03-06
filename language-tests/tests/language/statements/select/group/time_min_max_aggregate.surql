/**
[test]
reason = "Test time::min and time::max as aggregates with GROUP BY"

[[test.results]]
value = "[{ grp: 'a', id: event:1, ts: d'2024-01-01T00:00:00Z' }]"

[[test.results]]
value = "[{ grp: 'a', id: event:2, ts: d'2024-06-15T12:00:00Z' }]"

[[test.results]]
value = "[{ grp: 'a', id: event:3, ts: d'2024-12-31T23:59:59Z' }]"

[[test.results]]
value = "[{ grp: 'b', id: event:4, ts: d'2023-03-01T00:00:00Z' }]"

[[test.results]]
value = "[{ grp: 'b', id: event:5, ts: d'2023-09-01T00:00:00Z' }]"

[[test.results]]
value = "[{ earliest: d'2024-01-01T00:00:00Z', grp: 'a', latest: d'2024-12-31T23:59:59Z' }, { earliest: d'2023-03-01T00:00:00Z', grp: 'b', latest: d'2023-09-01T00:00:00Z' }]"

*/

CREATE event:1 SET grp = 'a', ts = d"2024-01-01T00:00:00Z";
CREATE event:2 SET grp = 'a', ts = d"2024-06-15T12:00:00Z";
CREATE event:3 SET grp = 'a', ts = d"2024-12-31T23:59:59Z";
CREATE event:4 SET grp = 'b', ts = d"2023-03-01T00:00:00Z";
CREATE event:5 SET grp = 'b', ts = d"2023-09-01T00:00:00Z";

SELECT grp, time::min(ts) AS earliest, time::max(ts) AS latest FROM event GROUP BY grp ORDER BY grp;
