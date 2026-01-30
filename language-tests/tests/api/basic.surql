/**
[test]
reason = "Test basic API definition, invocation, and response structure"

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: { message: 'Hello, World!' }, context: {  }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: { data: 'test' }, context: {  }, headers: { 'content-type': 'application/json' }, status: 201 }"

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: { default: 'status' }, context: {  }, headers: {}, status: 200 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

// Basic API definition with GET handler
DEFINE API "/hello"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    message: "Hello, World!"
                }
            };
        };

// Invoke without request object
api::invoke("/hello");

// Basic API with custom status and headers
DEFINE API "/create"
    FOR post
        THEN {
            {
                status: 201,
                headers: {
                    "content-type": "application/json"
                },
                body: {
                    data: "test"
                }
            };
        };

api::invoke("/create", {
    method: "post"
});

// Default status code (200) when not specified
DEFINE API "/default"
    FOR get
        THEN {
            {
                body: {
                    default: "status"
                }
            };
        };

api::invoke("/default");

