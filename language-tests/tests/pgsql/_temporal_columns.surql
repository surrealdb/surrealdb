/**
[test]
run = false
*/

DEFINE TABLE temporal_catalog SCHEMAFULL;
DEFINE FIELD d ON temporal_catalog TYPE date;
DEFINE FIELD t ON temporal_catalog TYPE time;
DEFINE FIELD ts ON temporal_catalog TYPE timestamp;
DEFINE FIELD tstz ON temporal_catalog TYPE datetime;
DEFINE FIELD iv ON temporal_catalog TYPE interval;
DEFINE FIELD maybe ON temporal_catalog TYPE option<date>;
DEFINE FIELD days ON temporal_catalog TYPE array<date>;

DEFINE TABLE temporal_trip SCHEMAFULL;
DEFINE FIELD d ON temporal_trip TYPE date;
DEFINE FIELD t ON temporal_trip TYPE time;
DEFINE FIELD ts ON temporal_trip TYPE timestamp;
DEFINE FIELD iv ON temporal_trip TYPE interval;

CREATE temporal_trip:1 SET d = date'2026-10-27', t = time'12:30:00.5', ts = timestamp'2026-10-27T12:30:00.5', iv = interval'1 year 2 mons 3 days 04:05:06.5' RETURN NONE;
CREATE temporal_trip:2 SET d = date'1999-12-31', t = time'24:00:00', ts = timestamp'1999-12-31T23:59:59', iv = interval'-1 days -02:00:00' RETURN NONE;

DEFINE TABLE temporal_compat SCHEMAFULL;
DEFINE FIELD at ON temporal_compat TYPE datetime;
DEFINE FIELD wall ON temporal_compat TYPE timestamp;
DEFINE FIELD day ON temporal_compat TYPE date;
