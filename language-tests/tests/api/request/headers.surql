/**
[test]
reason = "Test request header access via $request.headers"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
skip-api-request-id = true
value = "{ body: { 'user-agent': 'test-agent', 'x-custom': 'value' }, headers: {}, status: 200 }"

# 2
[[test.results]]
value = "NONE"

# 3
[[test.results]]
skip-api-request-id = true
value = "{ body: { 'content-type': 'application/json', 'authorization': 'Bearer token123' }, headers: {}, status: 200 }"

# 4
[[test.results]]
value = "NONE"

# 5
[[test.results]]
match = "$result.body.headers != NONE AND $result.status == 200"
error = false

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - Request headers access
DEFINE API "/headers"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    "user-agent": $request.headers["user-agent"],
                    "x-custom": $request.headers["x-custom"]
                }
            };
        };

# 1
api::invoke("/headers", {
    headers: {
        "user-agent": "test-agent",
        "x-custom": "value"
    }
});

# 2 - Multiple request headers
DEFINE API "/headers/multiple"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    "content-type": $request.headers["content-type"],
                    "authorization": $request.headers["authorization"]
                }
            };
        };

# 3
api::invoke("/headers/multiple", {
    headers: {
        "content-type": "application/json",
        "authorization": "Bearer token123"
    }
});

# 4 - Empty headers (default headers will be added by api::invoke)
DEFINE API "/headers/empty"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    headers: $request.headers
                }
            };
        };

# 5
api::invoke("/headers/empty", {
    headers: {}
});

