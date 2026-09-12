/**
# `records-10000-indexed` read as a record user with table permissions FULL.
#
# Imports both the indexed 10k-row dataset and the `perms-full` scaffolding
# (RECORD access method, `user:bench`, table permissions FULL); the shared
# `records-10000` base is imported once. This is the baseline arm for the
# indexed benches whose fast paths are withheld when a field carries a
# restricted SELECT permission — pair it with
# `records-10000-indexed-fieldperms-one`. `run = false`.

[env]
imports = ["./records-10000-indexed.surql", "./records-10000-perms-full.surql"]

[test]
run = false

[[test.results]]
value = "'OK'"
*/

RETURN 'OK';
