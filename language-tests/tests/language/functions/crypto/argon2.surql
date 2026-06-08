/**
[test]
reason = "argon2 hash + compare round-trip. Hashing is non-deterministic so we don't compare the hash itself — only that a fresh hash verifies against the original password and rejects a wrong one."

[env]
timeout = 10000

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/

crypto::argon2::compare(crypto::argon2::generate('p4ssw0rd'), 'p4ssw0rd');
crypto::argon2::compare(crypto::argon2::generate('p4ssw0rd'), 'wrong');
