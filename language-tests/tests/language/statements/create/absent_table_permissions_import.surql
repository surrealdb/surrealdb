/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:1 }]"

*/
DEFINE ACCESS user ON DATABASE TYPE RECORD;
DEFINE TABLE user PERMISSIONS FULL;
CREATE user:1;
