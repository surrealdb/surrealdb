/**
[test]
reason = "Test all HTTP methods (GET, POST, PUT, PATCH, DELETE, TRACE)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { method: 'get' }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { method: 'post' }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { method: 'put' }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { method: 'patch' }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { method: 'delete' }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { method: 'trace' }, headers: {}, status: 200 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

// GET method
DEFINE API "/method/get"
    FOR get
        THEN {
            RETURN {
                status: 200,
                body: {
                    method: string::lowercase($request.method)
                }
            };
        };

api::invoke("/method/get", {
    method: "get"
});

// POST method
DEFINE API "/method/post"
    FOR post
        THEN {
            RETURN {
                status: 200,
                body: {
                    method: string::lowercase($request.method)
                }
            };
        };

api::invoke("/method/post", {
    method: "post"
});

// PUT method
DEFINE API "/method/put"
    FOR put
        THEN {
            RETURN {
                status: 200,
                body: {
                    method: string::lowercase($request.method)
                }
            };
        };

api::invoke("/method/put", {
    method: "put"
});

// PATCH method
DEFINE API "/method/patch"
    FOR patch
        THEN {
            RETURN {
                status: 200,
                body: {
                    method: string::lowercase($request.method)
                }
            };
        };

api::invoke("/method/patch", {
    method: "patch"
});

// DELETE method
DEFINE API "/method/delete"
    FOR delete
        THEN {
            RETURN {
                status: 200,
                body: {
                    method: string::lowercase($request.method)
                }
            };
        };

api::invoke("/method/delete", {
    method: "delete"
});

// TRACE method
DEFINE API "/method/trace"
    FOR trace
        THEN {
            RETURN {
                status: 200,
                body: {
                    method: string::lowercase($request.method)
                }
            };
        };

api::invoke("/method/trace", {
    method: "trace"
});

