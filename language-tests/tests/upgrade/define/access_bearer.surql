/**
[env]
imports = ["upgrade/define/access_bearer_import.surql"]

[test]
upgrade = true
reason = "BEARER access tied to a user survives upgrade — grant/token/session durations round-trip and the referenced user is still present"

[[test.results]]
match = '''
    $result.accesses.api_bearer == "DEFINE ACCESS api_bearer ON DATABASE TYPE BEARER FOR USER DURATION FOR GRANT 4w2d, FOR TOKEN 1h, FOR SESSION NONE"
    && string::contains($result.users.api_user, "DEFINE USER api_user ON DATABASE")
    && string::contains($result.users.api_user, "ROLES VIEWER")
'''

*/
INFO FOR DB;
