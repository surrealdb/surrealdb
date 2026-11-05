/**
[test]
reason = "LET inside a block rejects protected parameter names ($access, $auth, $token, $session) at plan time. The check is the one the top-level executor applies."

[env]

[[test.results]]
error = "'session' is a protected variable and cannot be set"

[[test.results]]
error = "'auth' is a protected variable and cannot be set"

*/

{
    LET $session = 1;
    $session;
};

{
    LET $auth = "spoof";
    $auth;
};
