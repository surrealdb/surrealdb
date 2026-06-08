/**
[env]
imports = ["upgrade/define/sequence_import.surql"]

[test]
upgrade = true
reason = "DEFINE SEQUENCE state survives upgrade — definition round-trips and post-upgrade allocation continues from where pre-upgrade left off (does not reset to START)"

[[test.results]]
match = '''$result.sequences.order_id == "DEFINE SEQUENCE order_id BATCH 50 START 0"'''

[[test.results]]
match = '''type::is_int($result) && $result >= 3'''

[[test.results]]
match = '''type::is_int($result) && $result >= 4'''

*/
INFO FOR DB;
-- The pre-upgrade datastore consumed values 0, 1, 2. After upgrade the
-- sequence must continue from at least 3 (the batch may have skipped ahead
-- if the from-version pre-reserved a chunk on disk).
sequence::nextval('order_id');
sequence::nextval('order_id');
