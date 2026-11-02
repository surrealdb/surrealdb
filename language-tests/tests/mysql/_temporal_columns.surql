/**
[test]
run = false
*/

DEFINE TABLE temporal_my SCHEMAFULL;
DEFINE FIELD d ON temporal_my TYPE date;
DEFINE FIELD t ON temporal_my TYPE interval;
DEFINE FIELD dt ON temporal_my TYPE timestamp;
DEFINE FIELD ts ON temporal_my TYPE datetime;
DEFINE FIELD y ON temporal_my TYPE u16;
