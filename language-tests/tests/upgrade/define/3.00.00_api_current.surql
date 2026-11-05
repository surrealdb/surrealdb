/**
[env]
imports = ["upgrade/define/3.00.00_api_import.surql"]

[test]
version = ">=3.4.0-a"
upgrade = true
reason = "An API stored before handlers had a security mode reads as the default (it renders with no SECURITY clause, so it is not silently an INVOKER API) and still runs alongside an api::req::max_body middleware."

[[test.results]]
match = '''
    type::is_string($result.apis["/ping"])
    && !string::contains($result.apis["/ping"], "SECURITY")
'''

[[test.results]]
match = '''
    $result.status = 200 && $result.body = "pong"
'''

*/
INFO FOR DB;
api::invoke("/ping");
