/**
[test]
run = false
*/

DEFINE TABLE inf_event SCHEMAFULL;
DEFINE FIELD d ON inf_event TYPE date;
DEFINE FIELD ts ON inf_event TYPE timestamp;
DEFINE FIELD tz ON inf_event TYPE datetime;
