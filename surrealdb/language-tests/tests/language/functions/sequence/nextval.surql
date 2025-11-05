/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "10"

[[test.results]]
value = "11"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The sequence 'sq' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed because it exceeded the timeout"

*/
DEFINE SEQUENCE sq START 10;
RETURN sequence::nextval('sq');
RETURN sequence::nextval('sq');
REMOVE SEQUENCE sq;
RETURN sequence::nextval('sq');

DEFINE SEQUENCE sq BATCH 10 TIMEOUT 0s;
CREATE |test:20| CONTENT { sq: sequence::nextval('sq') } RETURN NONE;