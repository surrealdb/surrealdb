/**
[test]
reason = "Test timeout middleware and error handling"

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: 'The query was not executed because it exceeded the timeout: 1ns', headers: {  }, status: 500 }"

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: { completed: true }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
skip-api-request-id = true
value = "{ body: 'deadline has elapsed', headers: {  }, status: 500 }"

*/

// Timeout that will be exceeded
DEFINE API "/timeout/short"
    FOR get
        MIDDLEWARE
            api::timeout(1ns)
        THEN {
            CREATE something;
            {
                status: 200,
                body: {
                    completed: true
                }
            };
        };

api::invoke("/timeout/short");

// Timeout that should not be exceeded
DEFINE API "/timeout/long"
    FOR get
        MIDDLEWARE
            api::timeout(1s)
        THEN {
            {
                status: 200,
                body: {
                    completed: true
                }
            };
        };

api::invoke("/timeout/long");

// Timeout with different duration
DEFINE API "/timeout/medium"
    FOR get
        MIDDLEWARE
            api::timeout(10ms)
        THEN {
            SLEEP 100ms;
            {
                status: 200,
                body: {
                    completed: true
                }
            };
        };

api::invoke("/timeout/medium");

