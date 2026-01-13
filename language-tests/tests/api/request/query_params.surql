/**
[test]
reason = "Test query parameter access via $request.query"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
value = "{ body: { query: { name: 'test', value: '123' } }, headers: {}, status: 200 }"

# 2
[[test.results]]
value = "NONE"

# 3
[[test.results]]
value = "{ body: { query: { page: '1', limit: '10' } }, headers: {}, status: 200 }"

# 4
[[test.results]]
value = "NONE"

# 5
[[test.results]]
value = "{ body: { query: {} }, headers: {}, status: 200 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - Query parameters in request
DEFINE API "/query"
    FOR get
        THEN {
            RETURN {
                status: 200,
                body: {
                    query: $request.query
                }
            };
        };

# 1
api::invoke("/query", {
    query: {
        name: "test",
        value: "123"
    }
});

# 2 - Multiple query parameters
DEFINE API "/query/multiple"
    FOR get
        THEN {
            RETURN {
                status: 200,
                body: {
                    query: $request.query
                }
            };
        };

# 3
api::invoke("/query/multiple", {
    query: {
        page: "1",
        limit: "10"
    }
});

# 4 - Empty query parameters
DEFINE API "/query/empty"
    FOR get
        THEN {
            RETURN {
                status: 200,
                body: {
                    query: $request.query
                }
            };
        };

# 5
api::invoke("/query/empty", {
    query: {}
});

