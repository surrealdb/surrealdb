/**
[env]
imports = ["upgrade/define/3.00.00_access_bearer_import.surql"]

[test]
upgrade = true
reason = "BEARER access tied to a user survives upgrade — grant/token/session durations round-trip and the referenced user is still present"

# How much of an access a release renders is that release's own business — the
# `WITH JWT` clause is absent from the definition text of the older binaries in
# the matrix — so this brackets the durations rather than pinning the line.
[[test.results]]
match = '''
    string::contains($result.accesses.api_bearer, "DEFINE ACCESS api_bearer ON DATABASE TYPE BEARER FOR USER")
    && string::contains($result.accesses.api_bearer, "DURATION FOR GRANT 4w2d, FOR TOKEN 1h, FOR SESSION NONE")
    && string::contains($result.users.api_user, "DEFINE USER api_user ON DATABASE")
    && string::contains($result.users.api_user, "ROLES VIEWER")
'''

*/
INFO FOR DB;
