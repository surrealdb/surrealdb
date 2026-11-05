/**
[test]
reason = "pbkdf2 hash generation and comparison. Generation is salted and non-deterministic, so fresh hashes are only checked structurally and via a compare round-trip; deterministic checks use fixed PHC-format hashes with low (in-bounds) round counts. Compare returns false (not an error) for wrong passwords, malformed hashes, wrong-algorithm hashes, and hashes whose cost parameters exceed the allowed bound."

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

crypto::pbkdf2::compare(crypto::pbkdf2::generate('p4ssw0rd'), 'p4ssw0rd');
crypto::pbkdf2::compare(crypto::pbkdf2::generate('p4ssw0rd'), 'wrong');
string::starts_with(crypto::pbkdf2::generate('p4ssw0rd'), '$pbkdf2-sha256$i=600000,l=32$');
crypto::pbkdf2::compare('$pbkdf2-sha256$i=1000,l=32$dGVzdHNhbHR0ZXN0c2FsdA$kwG6pNcUOHJqLTbs8vtMZdbWcZWBNYDn8iifzAX2/bk', 'p4ssw0rd');
crypto::pbkdf2::compare('$pbkdf2-sha256$i=1000,l=32$dGVzdHNhbHR0ZXN0c2FsdA$kwG6pNcUOHJqLTbs8vtMZdbWcZWBNYDn8iifzAX2/bk', 'wrong');
crypto::pbkdf2::compare('$pbkdf2-sha256$i=1000,l=32$dGVzdHNhbHR0ZXN0c2FsdA$vGuJxI/35HzZYn6pMQBF7/Qgdub033WEv0KtvvxAtfM', '');
crypto::pbkdf2::compare('$pbkdf2-sha256$i=1000,l=32$dGVzdHNhbHR0ZXN0c2FsdA$1DWq7Yf0zOdWnzWdX+vQJaNGc87ZTj5fAeDc/TELtT8', 'héllo🦀');
crypto::pbkdf2::compare('$pbkdf2-sha256$i=2400001,l=32$dGVzdHNhbHR0ZXN0c2FsdA$Sg2K4/RX6/8w/+tbIhvgMtBqCF3zBI/3K46OL0GFK/4', 'p4ssw0rd');
crypto::pbkdf2::compare('not-a-valid-hash', 'p4ssw0rd');
crypto::pbkdf2::compare('$scrypt$ln=10,r=8,p=1$dGVzdHNhbHR0ZXN0c2FsdA$cL9/VaxqCCkB4xq7mB2I/UgkABJNAFH0Ie4vUFF4JGM', 'p4ssw0rd');
