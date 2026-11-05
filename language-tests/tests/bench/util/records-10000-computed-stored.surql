/**
# `records-10000` with the three fields of `records-10000-computed` written into
# the records as ordinary stored values, holding exactly what that dataset
# derives at read time.
#
# The stored twin of the whole computed set, so a `SELECT *` returns the same
# keys with the same values on both arms and the delta is the materialisation
# machinery rather than a wider result object. `records-10000-bucket-stored` is
# the narrower sibling: it backfills `bucket` alone, which is what a bench
# projecting only that field needs, since storing the other two would make the
# stored arm decode records the computed arm never wrote.
#
# The table carries no field definitions at all, so a read needs no field state.
# The backfill runs during import and is never measured. `run = false`.

[env]
imports = ["./records-10000.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	UPDATE record SET
		label = string::concat(name, ' / ', city),
		bucket = math::floor(score / 10),
		word_count = array::len(string::split(words, ' '))
		RETURN NONE;

	RETURN 'OK';
}
