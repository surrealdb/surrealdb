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
value = "NONE"

*/
DEFINE SEQUENCE sq START 10;
RETURN sequence::nextval('sq');
RETURN sequence::nextval('sq');
REMOVE SEQUENCE sq;
RETURN sequence::nextval('sq');

DEFINE SEQUENCE sq TIMEOUT 0s;
RETURN sequence::nextval('sq');
