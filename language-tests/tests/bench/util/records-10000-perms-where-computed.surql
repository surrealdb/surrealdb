/**
# `records-10000-perms-where` (row-level table permission, record user) with the
# three `COMPUTED` fields of `records-10000-computed` on the same table; the
# shared `records-10000` base is imported once.
#
# A conditional table permission has to be judged per record, so a COUNT over
# this table cannot be served from the key range and falls back to a scan. This
# arm adds computed fields on top of that fallback, so the count also pays
# whatever materialisation the scan performs for rows it only needs to count.
# Paired with `records-10000-perms-full` (key-range count) and
# `records-10000-perms-where` (fallback scan, no computed fields).
# `run = false`.

[env]
imports = ["./records-10000-perms-where.surql", "./records-10000-computed.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

RETURN 'OK';
