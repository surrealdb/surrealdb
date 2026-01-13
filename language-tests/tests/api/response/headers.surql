/**
[test]
reason = "Test response header manipulation"

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
value = "{ body: NULL, headers: { 'x-custom': 'value' }, status: 200 }"

# 2
[[test.results]]
value = "NONE"

# 3
[[test.results]]
value = "{ body: NULL, headers: { header1: 'value1', header2: 'value2' }, status: 200 }"

# 4
[[test.results]]
value = "NONE"

# 5
[[test.results]]
value = "{ body: NULL, headers: {}, status: 200 }"

# 6
[[test.results]]
value = "NONE"

# 7
[[test.results]]
value = "{ body: NULL, headers: { 'from-return': 'return' }, status: 200 }"

[env.capabilities]
allow-experimental = ["define_api"]

*/

# 0 - Single header via middleware
DEFINE API "/header/single"
    FOR get
        MIDDLEWARE
            api::res::header("x-custom", "value")
        THEN {
            RETURN {
                status: 200,
                body: null
            };
        };

# 1
api::invoke("/header/single");

# 2 - Multiple headers via middleware
DEFINE API "/header/multiple"
    FOR get
        MIDDLEWARE
            api::res::headers({
                "header1": "value1",
                "header2": "value2"
            })
        THEN {
            RETURN {
                status: 200,
                body: null
            };
        };

# 3
api::invoke("/header/multiple");

# 4 - Header removal (using api::res::headers with null value)
DEFINE API "/header/remove"
    FOR get
        MIDDLEWARE
            api::res::headers({
                "to-remove": NONE
            })
        THEN {
            RETURN {
                status: 200,
                headers: {
                    "to-remove": "should-be-removed"
                },
                body: null
            };
        };

# 5
api::invoke("/header/remove");

# 6 - Headers in RETURN object
DEFINE API "/header/return"
    FOR get
        THEN {
            RETURN {
                status: 200,
                headers: {
                    "from-return": "return"
                },
                body: null
            };
        };

# 7
api::invoke("/header/return");

