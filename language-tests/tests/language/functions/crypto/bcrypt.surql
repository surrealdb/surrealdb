/**
[test]
reason = "bcrypt hash + compare round-trip. Hashing is non-deterministic so we don't compare the hash itself — only that a fresh hash verifies against the original password and rejects a wrong one."

[env]
timeout = 10000

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/

crypto::bcrypt::compare(crypto::bcrypt::generate('p4ssw0rd'), 'p4ssw0rd');
crypto::bcrypt::compare(crypto::bcrypt::generate('p4ssw0rd'), 'wrong');
