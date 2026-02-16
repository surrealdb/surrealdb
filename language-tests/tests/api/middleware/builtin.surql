/**
[test]
reason = "Test all built-in middleware functions: timeout, req::body, res::body, res::status, res::header, res::headers"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
skip-api-request-id = true
value = "{ body: { parsed: { name: 'test', value: 42 } }, headers: {}, status: 200 }"

# 2
[[test.results]]
value = "NONE"

# 3
[[test.results]]
match = "type::is_bytes($result.body) AND $result.headers['content-type'] == 'application/json' AND $result.status == 200"
error = false

# 4
[[test.results]]
value = "NONE"

# 5
[[test.results]]
skip-api-request-id = true
value = "{ body: { original: 200 }, headers: {}, status: 404 }"

# 6
[[test.results]]
value = "NONE"

# 7
[[test.results]]
skip-api-request-id = true
value = "{ body: { test: 'data' }, headers: { 'x-custom': 'value' }, status: 200 }"

# 8
[[test.results]]
value = "NONE"

# 9
[[test.results]]
skip-api-request-id = true
value = "{ body: { test: 'data' }, headers: { 'header1': 'value1', 'header2': 'value2' }, status: 200 }"

*/

# 0 - api::req::body middleware - JSON strategy
DEFINE API "/parse_json"
    FOR post
        MIDDLEWARE
            api::req::body("json")
        THEN {
            {
                status: 200,
                body: {
                    parsed: $request.body
                }
            };
        };

# 1
api::invoke("/parse_json", {
    method: "post",
    headers: {
        "content-type": "application/json"
    },
    body: <bytes>'{"name":"test","value":42}'
});

# 2 - api::res::body middleware - JSON strategy
DEFINE API "/serialize_json"
    FOR get
        MIDDLEWARE
            api::res::body("json")
        THEN {
            {
                status: 200,
                body: {
                    message: "Hello"
                }
            };
        };

# 3
api::invoke("/serialize_json");

# 4 - api::res::status middleware
DEFINE API "/override_status"
    FOR get
        MIDDLEWARE
            api::res::status(404)
        THEN {
            {
                status: 200,
                body: {
                    original: 200
                }
            };
        };

# 5
api::invoke("/override_status");

# 6 - api::res::header middleware
DEFINE API "/add_header"
    FOR get
        MIDDLEWARE
            api::res::header("x-custom", "value")
        THEN {
            {
                status: 200,
                body: {
                    test: "data"
                }
            };
        };

# 7
api::invoke("/add_header");

# 8 - api::res::headers middleware
DEFINE API "/add_headers"
    FOR get
        MIDDLEWARE
            api::res::headers({
                "header1": "value1",
                "header2": "value2"
            })
        THEN {
            {
                status: 200,
                body: {
                    test: "data"
                }
            };
        };

# 9
api::invoke("/add_headers");

