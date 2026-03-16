/**
[test]
reason = "Test all HTTP methods (GET, POST, PUT, PATCH, DELETE, TRACE)"

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: { method: 'get' }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: { method: 'post' }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: { method: 'put' }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: { method: 'patch' }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: { method: 'delete' }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: { method: 'trace' }, headers: {}, status: 200 }"

*/

// GET method
DEFINE API "/method/get"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    method: $request.method
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
            {
                status: 200,
                body: {
                    method: $request.method
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
            {
                status: 200,
                body: {
                    method: $request.method
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
            {
                status: 200,
                body: {
                    method: $request.method
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
            {
                status: 200,
                body: {
                    method: $request.method
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
            {
                status: 200,
                body: {
                    method: $request.method
                }
            };
        };

api::invoke("/method/trace", {
    method: "trace"
});

