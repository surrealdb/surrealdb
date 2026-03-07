/**
[env]
timeout = 120000
context-timeout = 60000

[test]
reason = "Test crypto::scrypt::generate() and crypto::scrypt::compare()"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/
LET $hash = crypto::scrypt::generate("password123");
crypto::scrypt::compare($hash, "password123");
crypto::scrypt::compare($hash, "wrong_password");
