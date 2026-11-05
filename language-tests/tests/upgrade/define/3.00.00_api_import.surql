/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

*/
DEFINE CONFIG API
    MIDDLEWARE api::req::max_body("1mb")
    PERMISSIONS FULL;
DEFINE API "/ping"
    FOR get
        PERMISSIONS FULL
        THEN { RETURN { status: 200, body: 'pong' } }
    COMMENT 'health-check endpoint';
