/**
[env]
timeout = 30000
context-timeout = 15000

[test]
reason = "Test crypto::pbkdf2::generate() and crypto::pbkdf2::compare()"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/
LET $hash = crypto::pbkdf2::generate("password123");
crypto::pbkdf2::compare($hash, "password123");
crypto::pbkdf2::compare($hash, "wrong_password");
