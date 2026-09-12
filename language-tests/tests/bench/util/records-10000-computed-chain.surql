/**
# `records-10000` plus a three-deep chain of `COMPUTED` fields: `rank3` reads
# `rank2`, which reads `rank1`, which reads the stored `age`.
#
# Selecting `rank3` therefore forces all three bodies to be derived, in
# dependency order, as three separate field materialisations. Paired with
# `records-10000-computed-flat`, which defines `rank3` alone with the whole
# chain inlined into one body: both arms return the same `rank3` for every row,
# so the delta is the per-field cost of a dependency chain — resolving the
# order, and writing and re-reading each intermediate through the document —
# rather than the arithmetic, which is identical. `run = false`.

[env]
imports = ["./records-10000.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE FIELD rank1 ON record TYPE int COMPUTED age * 2;
	DEFINE FIELD rank2 ON record TYPE int COMPUTED rank1 + number;
	DEFINE FIELD rank3 ON record TYPE int COMPUTED rank2 * 3;

	RETURN 'OK';
}
