/**
[test]
reason = "Test timeout middleware and error handling"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The query was not executed because it exceeded the timeout: 1ns"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { completed: true }, context: {  }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
error = "deadline has elapsed"

[env.capabilities]
allow-experimental = ["define_api"]

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

