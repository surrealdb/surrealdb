/**
[test]
reason = "Test invalid path definition errors"

[[test.results]]
error = "The string could not be parsed into a path: Segment should start with /"

[[test.results]]
error = "The string could not be parsed into a path: Path cannot be empty"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[env.capabilities]
allow-experimental = ["define_api"]

*/

// Path not starting with /
DEFINE API "missing_slash"
    FOR get
        THEN {
            RETURN {
                status: 200,
                body: {}
            };
        };

// Empty path
DEFINE API ""
    FOR get
        THEN {
            RETURN {
                status: 200,
                body: {}
            };
        };

// Valid paths for comparison
DEFINE API "/valid"
    FOR get
        THEN {
            RETURN {
                status: 200,
                body: {}
            };
        };

DEFINE API "/also/valid"
    FOR get
        THEN {
            RETURN {
                status: 200,
                body: {}
            };
        };

