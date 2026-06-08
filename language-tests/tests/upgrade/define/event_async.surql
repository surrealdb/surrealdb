/**
[env]
imports = ["upgrade/define/event_async_import.surql"]

[test]
upgrade = true
reason = "Both catalog::EventKind variants (Sync and Async{retry, max_depth}) round-trip through upgrade — the ASYNC RETRY/MAXDEPTH clause is preserved verbatim and the sync event keeps its bare form"

[[test.results]]
match = '''
    string::contains($result.events.sync_evt, "DEFINE EVENT sync_evt ON log_src")
    && !string::contains($result.events.sync_evt, "ASYNC")
    && string::contains($result.events.async_evt, "DEFINE EVENT async_evt ON log_src ASYNC RETRY 2 MAXDEPTH 3")
'''

*/
INFO FOR TABLE log_src;
