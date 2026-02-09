/**
[test]
reason = "Test dynamic path segments with :param syntax and type coercion"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
skip-api-request-id = true
value = "{ body: { id: '123' }, headers: {}, status: 200 }"

# 2
[[test.results]]
value = "NONE"

# 3
[[test.results]]
skip-api-request-id = true
value = "{ body: { id: '456', postId: 'abc' }, headers: {}, status: 200 }"

# 4
[[test.results]]
value = "NONE"

# 5
[[test.results]]
skip-api-request-id = true
value = "{ body: { id: 789 }, headers: {}, status: 200 }"

# 6
[[test.results]]
skip-api-request-id = true
value = "{ body: 'Not found', headers: {  }, status: 404 }"

# 7
[[test.results]]
skip-api-request-id = true
value = "{ body: 'Not found', headers: {  }, status: 404 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - Single dynamic segment
DEFINE API "/users/:id"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    id: $request.params.id
                }
            };
        };

# 1
api::invoke("/users/123");

# 2 - Multiple dynamic segments
DEFINE API "/users/:id/posts/:postId"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    id: $request.params.id,
                    postId: $request.params.postId
                }
            };
        };

# 3
api::invoke("/users/456/posts/abc");

# 4 - Typed dynamic segment
DEFINE API "/items/:id<number>"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    id: $request.params.id
                }
            };
        };

# 5
api::invoke("/items/789");

# 6 - Invalid type coercion (may still match but with wrong type)
api::invoke("/items/not_a_number");

# 7 - Missing segment - should return NONE
api::invoke("/users");

