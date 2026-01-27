/**
[test]
reason = "Test that duplicate methods in API definitions are rejected"

[[test.results]]
error = "The api '/duplicate/simple' already has an action defined for the get method"

[[test.results]]
error = "The api '/duplicate/multiple' already has an action defined for the post method"

[[test.results]]
error = "The api '/duplicate/mixed' already has an action defined for the get method"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { handler: 'get' }, context: {  }, headers: {}, status: 200 }"

[[test.results]]
value = "{ body: { handler: 'post' }, context: {  }, headers: {}, status: 201 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

// Test 0: Duplicate single method
DEFINE API "/duplicate/simple"
    FOR get
        THEN {
            {
                status: 200,
                body: { handler: "first" }
            };
        }
    FOR get
        THEN {
            {
                status: 200,
                body: { handler: "second" }
            };
        };

// Test 1: Duplicate method in comma-separated list
DEFINE API "/duplicate/multiple"
    FOR post
        THEN {
            {
                status: 201,
                body: { handler: "first" }
            };
        }
    FOR post, put
        THEN {
            {
                status: 200,
                body: { handler: "second" }
            };
        };

// Test 2: Duplicate method across different action blocks
DEFINE API "/duplicate/mixed"
    FOR get, post
        THEN {
            {
                status: 200,
                body: { handler: "first" }
            };
        }
    FOR get, delete
        THEN {
            {
                status: 200,
                body: { handler: "second" }
            };
        };

// Test 3: Valid API with non-duplicate methods - should succeed
DEFINE API "/valid/unique"
    FOR get
        THEN {
            {
                status: 200,
                body: { handler: "get" }
            };
        }
    FOR post
        THEN {
            {
                status: 201,
                body: { handler: "post" }
            };
        };

// Test 4: Invoke the valid API's GET endpoint
api::invoke("/valid/unique", {
    method: "get"
});

// Test 5: Invoke the valid API's POST endpoint
api::invoke("/valid/unique", {
    method: "post"
});
