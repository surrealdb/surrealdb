/**
[test]
reason = "Test API comment functionality"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: {}, headers: {}, status: 200 }"

[[test.results]]
value = '''{ "/commented": "DEFINE API '/commented' FOR any PERMISSIONS FULL THEN { RETURN { status: 200, body: {  } } } COMMENT 'This is a test API'" }'''

[env.capabilities]
allow-experimental = ["define_api"]

*/

// API with comment
DEFINE API "/commented"
    FOR any
        THEN {
            RETURN {
                status: 200,
                body: {}
            };
        }
    COMMENT "This is a test API";

api::invoke("/commented");

// Check comment in INFO output
(INFO FOR DB).apis;

