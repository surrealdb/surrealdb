/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: tgt:1, num: 3, time: d'2012-01-01T08:00:03Z' }]"

[[test.results]]
value = "[{ id: tgt:2, num: 2, time: d'2012-01-01T08:00:02Z' }]"

[[test.results]]
value = "[{ id: tgt:3, num: 1, time: d'2012-01-01T08:00:01Z' }]"

[[test.results]]
value = '''[{ id: min:[], "math::min": 1, "time::min": d'2012-01-01T08:00:01Z' }]'''

[[test.results]]
value = "[]"

[[test.results]]
value = '''[{ id: min:[], "math::min": 2, "time::min": d'2012-01-01T08:00:02Z' }]'''

*/

DEFINE TABLE tgt;
DEFINE FIELD num ON tgt TYPE number;
DEFINE FIELD time ON tgt TYPE datetime;
DEFINE TABLE min AS SELECT math::min(num), time::min(time) FROM tgt GROUP ALL;

CREATE tgt:1 SET num = 3, time = d"2012-01-01T08:00:03Z";
CREATE tgt:2 SET num = 2, time = d"2012-01-01T08:00:02Z";
CREATE tgt:3 SET num = 1, time = d"2012-01-01T08:00:01Z";

SELECT * FROM min;

# Deletes the minum value, so forces a recalculation
DELETE tgt:3;

SELECT * FROM min;
