/**
[test]
reason = "scrypt hash generation and comparison. Generation is salted and non-deterministic, so fresh hashes are only checked structurally and via a compare round-trip; deterministic checks use fixed PHC-format hashes with low (in-bounds) cost parameters. Compare returns false (not an error) for wrong passwords, malformed hashes, wrong-algorithm hashes, and hashes whose cost parameters exceed the allowed bound."

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[env]
timeout = 15000
*/

crypto::scrypt::compare(crypto::scrypt::generate('p4ssw0rd'), 'p4ssw0rd');
crypto::scrypt::compare(crypto::scrypt::generate('p4ssw0rd'), 'wrong');
string::starts_with(crypto::scrypt::generate('p4ssw0rd'), '$scrypt$ln=17,r=8,p=1$');
crypto::scrypt::compare('$scrypt$ln=10,r=8,p=1$dGVzdHNhbHR0ZXN0c2FsdA$cL9/VaxqCCkB4xq7mB2I/UgkABJNAFH0Ie4vUFF4JGM', 'p4ssw0rd');
crypto::scrypt::compare('$scrypt$ln=10,r=8,p=1$dGVzdHNhbHR0ZXN0c2FsdA$cL9/VaxqCCkB4xq7mB2I/UgkABJNAFH0Ie4vUFF4JGM', 'wrong');
crypto::scrypt::compare('$scrypt$ln=10,r=8,p=1$dGVzdHNhbHR0ZXN0c2FsdA$WEgsoWCk6oFUC5EUybZQXM/9KnQsZ4yUjOFWoTLQrCU', '');
crypto::scrypt::compare('$scrypt$ln=10,r=8,p=1$dGVzdHNhbHR0ZXN0c2FsdA$PsDiJEqXBKM73UiNA1uFRcQqsx6o7SM8zRwdeOIeg8E', 'héllo🦀');
crypto::scrypt::compare('$scrypt$ln=10,r=32,p=1$dGVzdHNhbHR0ZXN0c2FsdA$KQZWS7ek/vUPhniOUfn54CHA/2ev2pBJ/wCriXVIngM', 'p4ssw0rd');
crypto::scrypt::compare('not-a-valid-hash', 'p4ssw0rd');
crypto::scrypt::compare('$pbkdf2-sha256$i=1000,l=32$dGVzdHNhbHR0ZXN0c2FsdA$kwG6pNcUOHJqLTbs8vtMZdbWcZWBNYDn8iifzAX2/bk', 'p4ssw0rd');
