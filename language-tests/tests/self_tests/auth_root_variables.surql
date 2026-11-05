/**
[env]
auth = { level = "viewer" }
remote-skip = "asserts $token/$session, which over the API reflect a real signin (a JWT and a per-connection id); the embedded harness injects a connection-less session with neither"

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ ac: NONE, db: 'test', exp: NONE, id: NONE, ip: NONE, ns: 'test', or: NONE, rd: NONE, tk: NONE }"
*/

$auth;
$token;
$access;
$session;
