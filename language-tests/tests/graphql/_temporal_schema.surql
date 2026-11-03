/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
run = false
reason = "A table with date, time, timestamp and interval fields exposed through GraphQL, imported by the temporal GraphQL tests"
*/

DEFINE CONFIG GRAPHQL TABLES AUTO FUNCTIONS AUTO;

DEFINE TABLE shift SCHEMAFULL;
DEFINE FIELD label ON shift TYPE string;
DEFINE FIELD day ON shift TYPE date;
DEFINE FIELD starts ON shift TYPE time;
DEFINE FIELD at ON shift TYPE timestamp;
DEFINE FIELD length ON shift TYPE interval;

CREATE shift:early SET label = 'early', day = pd'2026-10-26', starts = pt'06:30:00', at = ts'2026-10-26T06:30:00', length = ivl'8 hours';
CREATE shift:late SET label = 'late', day = pd'2026-10-28', starts = pt'14:00:00.5', at = ts'2026-10-28T14:00:00.5', length = ivl'1 day 2 hours';
