/**
[env]
namespace = true
database = true

[test]
reason = """
A sequence issues values up to 9223372036854775806 and then fails every draw
with an exhausted error, rather than overflowing: whatever its batch, when it
was defined in the same transaction, and when it starts past its last value.
Redefining an exhausted sequence revives it from its new `START`.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "9223372036854775805"

[[test.results]]
value = "9223372036854775806"

[[test.results]]
error = "The sequence has no values left to issue: its values cannot exceed 9223372036854775806"

[[test.results]]
error = "The sequence has no values left to issue: its values cannot exceed 9223372036854775806"

[[test.results]]
value = "NONE"

[[test.results]]
value = "9223372036854775805"

[[test.results]]
value = "9223372036854775806"

[[test.results]]
error = "The sequence has no values left to issue: its values cannot exceed 9223372036854775806"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The query was not executed due to a failed transaction"

[[test.results]]
error = "The sequence has no values left to issue: its values cannot exceed 9223372036854775806"

[[test.results]]
error = "Cannot COMMIT: the transaction was aborted due to a prior error"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The sequence has no values left to issue: its values cannot exceed 9223372036854775806"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "2"

*/

DEFINE SEQUENCE one BATCH 1 START 9223372036854775805;
RETURN sequence::nextval('one');
RETURN sequence::nextval('one');
RETURN sequence::nextval('one');
RETURN sequence::nextval('one');

DEFINE SEQUENCE many START 9223372036854775805;
RETURN sequence::nextval('many');
RETURN sequence::nextval('many');
RETURN sequence::nextval('many');

BEGIN;
DEFINE SEQUENCE here START 9223372036854775806;
sequence::nextval('here');
sequence::nextval('here');
COMMIT;

DEFINE SEQUENCE past START 9223372036854775807;
RETURN sequence::nextval('past');

DEFINE SEQUENCE OVERWRITE one START 1;
RETURN sequence::nextval('one');
RETURN sequence::nextval('one');
