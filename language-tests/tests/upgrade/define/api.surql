/**
[env]
imports = ["upgrade/define/api_import.surql"]

[test]
upgrade = true
reason = "DEFINE API and DEFINE CONFIG API both round-trip through upgrade — middleware reference, permissions, and inline closure body all survive"

[[test.results]]
match = '''
    type::is_string($result.apis["/ping"])
    && string::contains($result.apis["/ping"], "DEFINE API '/ping'")
    && string::contains($result.apis["/ping"], "FOR get")
    && string::contains($result.apis["/ping"], "body: 'pong'")
    && string::contains($result.apis["/ping"], "COMMENT 'health-check endpoint'")
    && type::is_string($result.configs.API)
    && string::contains($result.configs.API, "api::req::max_body('1mb')")
'''

*/
INFO FOR DB;
