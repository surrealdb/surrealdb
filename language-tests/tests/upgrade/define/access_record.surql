/**
[env]
imports = ["upgrade/define/access_record_import.surql"]

[test]
upgrade = true
reason = "RECORD access with SIGNUP/SIGNIN closures and explicit durations survives upgrade — the closure expressions and durations round-trip via INFO FOR DB"

[[test.results]]
match = '''
    type::is_string($result.accesses.account)
    && string::contains($result.accesses.account, "DEFINE ACCESS account ON DATABASE TYPE RECORD")
    && string::contains($result.accesses.account, "SIGNUP")
    && string::contains($result.accesses.account, "SIGNIN")
    && string::contains($result.accesses.account, "DURATION FOR TOKEN 1h, FOR SESSION 1d")
'''

*/
INFO FOR DB;
