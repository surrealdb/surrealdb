/**
# `records-10000` plus three read-time `COMPUTED` fields.
#
# A `COMPUTED` field is never stored: it is derived from the record on every
# read that needs it, so these three bodies run per row per query. They are
# deliberately of different shapes and costs — a string concatenation, a scalar
# arithmetic fold, and a string split whose result is measured — so a
# materialisation change shows up as more than one number. Each reads only
# stored fields, so none of them depends on another; the dependency-ordered case
# is `records-10000-computed-chain`.
#
# Paired with the plain `records-10000` base as the baseline arm: the plain arm
# carries no field definitions at all, so a read of it takes the pipeline's
# fast path (no field state, batch predicate evaluation), while this arm pays
# per-row materialisation. Read as owner — the permissioned variant is
# `records-10000-computed-perms`. `run = false`.

[env]
imports = ["./records-10000.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE FIELD label ON record TYPE string COMPUTED string::concat(name, ' / ', city);
	DEFINE FIELD bucket ON record TYPE number COMPUTED math::floor(score / 10);
	DEFINE FIELD word_count ON record TYPE int COMPUTED array::len(string::split(words, ' '));

	RETURN 'OK';
}
