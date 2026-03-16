/**
[test]
reason = "Subqueries aliased to a dotted path should merge into existing nested objects"
issue = 7038

[[test.results]]
value = "[{ id: devices:1, name: 'test-device', status: { phase: 'Running' } }]"

[[test.results]]
value = "[{ id: events:1, kind: 'Warning', message: 'High CPU' }]"

[[test.results]]
value = "[{ id: devices:1, name: 'test-device', status: { events: [{ kind: 'Warning', message: 'High CPU' }], phase: 'Running' } }]"
*/

CREATE devices:1 SET name = 'test-device', status = { phase: 'Running' };
CREATE events:1 SET kind = 'Warning', message = 'High CPU';
-- Subquery aliased to a dotted path should merge into existing nested object
SELECT
	*,
	(SELECT kind, message FROM events LIMIT 5) AS status.events
FROM devices;
