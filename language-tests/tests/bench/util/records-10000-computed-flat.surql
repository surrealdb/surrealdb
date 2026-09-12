/**
# `records-10000` plus a single `COMPUTED` field holding the whole body that
# `records-10000-computed-chain` spreads over three dependent fields.
#
# `rank3` evaluates to the same value per row as the chained arm's `rank3`, so
# the pair isolates the cost of the chain itself. `run = false`.

[env]
imports = ["./records-10000.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE FIELD rank3 ON record TYPE int COMPUTED ((age * 2) + number) * 3;

	RETURN 'OK';
}
