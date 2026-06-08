/**
[env]
imports = ["upgrade/define/access_jwt_import.surql"]

[test]
upgrade = true
reason = "JWT access definition survives upgrade — algorithm, durations, issuer key, and comment all round-trip"

[[test.results]]
match = '''$result.accesses.api_jwt == "DEFINE ACCESS api_jwt ON DATABASE TYPE JWT ALGORITHM HS256 KEY '[REDACTED]' WITH ISSUER KEY '[REDACTED]' DURATION FOR TOKEN 1h, FOR SESSION 2h COMMENT 'jwt access for api'"'''

*/
INFO FOR DB;
