/**
# `records-10000-computed-perms` plus a field-level SELECT permission on
# `status` that withholds it for roughly half the rows (`age < 50`, and `age`
# is uniform over 1..100).
#
# Withholding a stored field is what forces the computed fields to be derived a
# second time, against the reduced record, so that `status_tag` — whose body
# reads `status` — yields what the caller is allowed to see rather than the
# value the record computed to. The second derivation re-runs *every* computed
# field on the table, not only the one that reads the withheld field, so the
# three fields that read nothing restricted are recomputed to the same values
# they already held. Paired with `records-10000-computed-guarded-open` (same
# guard, nothing withheld, no re-derivation) and with
# `records-10000-computed-perms` (no guard at all).
#
# Row count is unchanged — a field permission withholds a field, never a row —
# so all three arms return 10k rows. The `status` field and the `status_tag`
# value do differ here for the rows the guard withholds, which is why the paired
# bench projects the three computed fields that read nothing restricted rather
# than the whole record: the guard is enforced whatever the projection names, so
# the arms can be held to identical output without giving up what they measure.
# `run = false`.

[env]
imports = ["./records-10000-computed-perms.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

{
	DEFINE FIELD status ON record PERMISSIONS FOR select WHERE age < 50;

	RETURN 'OK';
}
