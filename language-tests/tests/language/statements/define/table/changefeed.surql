/**
[test]
reason = "Test DEFINE TABLE with CHANGEFEED option"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: {  }, indexes: {  }, lives: {  }, tables: {  } }"

*/

DEFINE TABLE person CHANGEFEED 1h;
INFO FOR TABLE person;
DEFINE TABLE events CHANGEFEED 30m INCLUDE ORIGINAL;
INFO FOR TABLE events;
