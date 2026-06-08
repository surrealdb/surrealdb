/**
[env]
imports = ["upgrade/define/changefeed_import.surql"]

[test]
upgrade = true
reason = "CHANGEFEED metadata survives upgrade AND the Cf key family (change-feed entries) written by the from-version is readable by the to-version via SHOW CHANGES"

[[test.results]]
match = '''string::contains($result.tables.cf_src, "CHANGEFEED 1h")'''

[[test.results]]
match = '''
    type::is_array($result)
    && count($result) >= 3
'''

*/
INFO FOR DB;
-- Returns one entry per mutation (create / update / delete) since versionstamp 0.
-- Versionstamps are non-deterministic, so only assert count >= 3.
SHOW CHANGES FOR TABLE cf_src SINCE 0 LIMIT 100;
