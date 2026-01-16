/**
[test]
reason = "Test OVERWRITE and IF NOT EXISTS behavior"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { version: 'first' }, context: {  }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { version: 'second' }, context: {  }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { version: 'first' }, context: {  }, headers: {}, status: 200 }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ body: { version: 'second' }, context: {  }, headers: {}, status: 200 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

// Default behavior - first definition succeeds
DEFINE API "/overwrite_test"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    version: "first"
                }
            };
        };

api::invoke("/overwrite_test");

// Default behavior - second definition fails
DEFINE API "/overwrite_test"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    version: "second"
                }
            };
        };

// OVERWRITE - replaces existing definition
DEFINE API OVERWRITE "/overwrite_test"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    version: "second"
                }
            };
        };

api::invoke("/overwrite_test");

// IF NOT EXISTS - skips if exists
DEFINE API IF NOT EXISTS "/if_not_exists"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    version: "first"
                }
            };
        };

api::invoke("/if_not_exists");

// IF NOT EXISTS - second attempt should be skipped (no error)
DEFINE API IF NOT EXISTS "/if_not_exists"
    FOR get
        THEN {
            {
                status: 200,
                body: {
                    version: "second"
                }
            };
        };

// Should still return first version (IF NOT EXISTS skips second definition)
api::invoke("/if_not_exists");

