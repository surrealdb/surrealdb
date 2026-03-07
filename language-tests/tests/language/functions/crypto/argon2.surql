/**
[env]
timeout = 30000
context-timeout = 15000

[test]
reason = "Test crypto::argon2::generate() and crypto::argon2::compare()"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/
LET $hash = crypto::argon2::generate("password123");
crypto::argon2::compare($hash, "password123");
crypto::argon2::compare($hash, "wrong_password");
