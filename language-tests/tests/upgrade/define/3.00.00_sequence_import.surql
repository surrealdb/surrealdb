/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "0"

[[test.results]]
value = "1"

[[test.results]]
value = "2"

*/
DEFINE SEQUENCE order_id BATCH 50 START 0;
sequence::nextval('order_id');
sequence::nextval('order_id');
sequence::nextval('order_id');
