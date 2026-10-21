/**
[env]
imports = ["upgrade/define/3.00.00_api_import.surql"]

[test]
upgrade = true
reason = "DEFINE API and DEFINE CONFIG API both round-trip through upgrade — middleware reference, permissions, and inline closure body all survive. An API stored before handlers had a security mode reads as the default (it renders with no SECURITY clause, so it is not silently an INVOKER API) and still runs."

[[test.results]]
match = '''
    type::is_string($result.apis["/ping"])
    && string::contains($result.apis["/ping"], "DEFINE API '/ping'")
    && string::contains($result.apis["/ping"], "FOR get")
    && string::contains($result.apis["/ping"], "body: 'pong'")
    && string::contains($result.apis["/ping"], "COMMENT 'health-check endpoint'")
    && !string::contains($result.apis["/ping"], "SECURITY")
    && type::is_string($result.configs.API)
    && string::contains($result.configs.API, "api::req::max_body('1mb')")
'''

[[test.results]]
match = '''
    $result.status = 200 && $result.body = "pong"
'''

*/
INFO FOR DB;
api::invoke("/ping");
