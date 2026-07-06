/**
[env]
clean = true

[test]
reason = "DEFINE USER derives and stores SCRAM-SHA-256 verifier material alongside the Argon2 hash (PASSWORD derives it, PASSHASH-only users have none, PASSSCRAM imports one). The verifier is a password-equivalent secret, so it is REDACTED in INFO metadata responses; the exact derivation is pinned by the iam::scram known-answer test, and export (not INFO) round-trips the real value."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
match = '''
string::contains($result.users.pw_user, "PASSHASH '$argon2id$")
AND string::contains($result.users.pw_user, "PASSSCRAM '[REDACTED]'")
AND !string::contains($result.users.pw_user, "SCRAM-SHA-256")
AND !string::contains($result.users.hash_user, "PASSSCRAM")
AND string::contains($result.users.scram_user, "PASSSCRAM '[REDACTED]'")
AND !string::contains($result.users.scram_user, "SCRAM-SHA-256")
'''

[[test.results]]
match = '''
$result.users[WHERE name = "pw_user"][0].scram = "[REDACTED]"
AND $result.users[WHERE name = "scram_user"][0].scram = "[REDACTED]"
AND $result.users[WHERE name = "hash_user"][0].scram = NONE
'''
*/

-- PASSWORD derives both an Argon2 hash and a SCRAM verifier.
DEFINE USER pw_user ON ROOT PASSWORD 'pencil' ROLES OWNER;
-- PASSHASH only: no plaintext, so no SCRAM verifier is stored.
DEFINE USER hash_user ON ROOT PASSHASH 'x' ROLES VIEWER;
-- PASSSCRAM imports a precomputed verifier (coexists with PASSHASH); this is how
-- export/import round-trips the SCRAM verifier without the plaintext.
DEFINE USER scram_user ON ROOT PASSHASH 'y' PASSSCRAM 'SCRAM-SHA-256$4096:MDEyMzQ1Njc4OWFiY2RlZg==$nQpbZ77WudtqufPwikHXGRt6g2QJ4zns8bZLw273DRM=:jn2amWP1q1h+jgjy0YTO14S6/F02SV7taipOeB7ef20=' ROLES VIEWER;
INFO FOR ROOT;
INFO FOR ROOT STRUCTURE;
