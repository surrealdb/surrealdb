/**
# `records-10000` with `bucket` written into the records as an ordinary stored
# field, holding exactly the value `records-10000-computed` derives for it at
# read time (`math::floor(score / 10)`).
#
# The table carries no field definitions at all, so a read of `bucket` needs no
# field state: the value comes straight out of the record, and the scan-side
# fast paths that read a sort key or a predicate operand from revision-encoded
# bytes stay eligible. Paired with `records-10000-computed`, where the same
# field name resolves to a read-time derivation instead, which disqualifies
# those paths — the pair isolates that difference with the returned values held
# equal. The backfill runs during import and is never measured. `run = false`.

[env]
imports = ["./records-10000.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	UPDATE record SET bucket = math::floor(score / 10) RETURN NONE;

	RETURN 'OK';
}
